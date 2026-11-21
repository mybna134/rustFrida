use cooperative_lease::{ActivationData, ActivationView, Error, ExternalFailure, Gate, SuspendError};
use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

// 测试超时只判失败，不取消 callback，也不提供运行时超时返回值。
fn recv<T>(rx: &mpsc::Receiver<T>) -> T {
    rx.recv_timeout(Duration::from_secs(5))
        .expect("test watchdog")
}

fn wait_pending(gate: &Gate, n: usize) {
    let until = Instant::now() + Duration::from_secs(5);
    while gate.snapshot().pending != n {
        assert!(Instant::now() < until, "pending pin watchdog");
        thread::yield_now();
    }
}

fn data(n: u64) -> ActivationData {
    ActivationData {
        deadline_ms: Some(1_000 + n),
        checkpoint: Some(n),
    }
}

fn drained(g: &Gate) {
    let s = g.snapshot();
    assert!(!s.poisoned);
    assert_eq!(
        (s.active, s.suspended, s.pending, s.activations),
        (0, 0, 0, 0)
    );
    assert_eq!(s.owner, None);
}

// 仅模拟 C 切换的状态机，绝不是 QuickJS 保存帧、异常、GC 根安全性的证明。
#[derive(Default)]
struct FakeC(Mutex<CState>);
#[derive(Default)]
struct CState {
    current: Option<ActivationView>,
    saved: HashMap<u64, ActivationView>,
    events: Vec<(char, u64)>,
}
impl FakeC {
    fn enter(&self, v: ActivationView) {
        let mut c = self.0.lock().unwrap();
        assert!(c.current.is_none(), "two C executors");
        c.current = Some(v);
    }
    fn check(&self, v: ActivationView) {
        assert_eq!(self.0.lock().unwrap().current, Some(v));
    }
    fn leave(&self, v: ActivationView) {
        let mut c = self.0.lock().unwrap();
        assert_eq!(c.current.take(), Some(v));
    }
    fn suspend(&self, v: ActivationView) -> Result<(), SuspendError> {
        assert_eq!(thread::current().id(), v.thread);
        let mut c = self.0.lock().unwrap();
        assert_eq!(c.current.take(), Some(v));
        assert!(c.saved.insert(v.activation, v).is_none());
        c.events.push(('S', v.activation));
        Ok(())
    }
    fn resume(&self, v: ActivationView) -> Result<(), ()> {
        assert_eq!(thread::current().id(), v.thread);
        let mut c = self.0.lock().unwrap();
        assert!(c.current.is_none());
        assert_eq!(c.saved.remove(&v.activation), Some(v));
        c.current = Some(v);
        c.events.push(('R', v.activation));
        Ok(())
    }
}

#[test]
fn mutual_exclusion_and_same_thread_recursion() {
    let gate = Gate::new();
    let live = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let (done_tx, done_rx) = mpsc::channel();
    let mut threads = Vec::new();
    for _ in 0..6 {
        let (g, live, calls, done) = (gate.clone(), live.clone(), calls.clone(), done_tx.clone());
        threads.push(thread::spawn(move || {
            for _ in 0..80 {
                let mut outer = g.enter(data(1)).unwrap();
                outer
                    .execute(|v| {
                        assert_eq!(live.fetch_add(1, Ordering::SeqCst), 0);
                        let mut nested = g.enter(data(999)).unwrap();
                        assert_eq!(nested.current().unwrap().activation, v.activation);
                        assert_eq!(nested.current().unwrap().data, data(1));
                        assert_eq!(nested.current().unwrap().depth, 2);
                        nested
                            .execute(|_| {
                                thread::yield_now();
                                calls.fetch_add(1, Ordering::SeqCst);
                            })
                            .unwrap();
                        drop(nested);
                        assert_eq!(live.fetch_sub(1, Ordering::SeqCst), 1);
                    })
                    .unwrap();
            }
            done.send(()).unwrap();
        }));
    }
    for _ in 0..6 {
        recv(&done_rx);
    }
    for t in threads {
        t.join().unwrap();
    }
    assert_eq!(calls.load(Ordering::SeqCst), 480);
    drained(&gate);
}

#[test]
fn non_lifo_a_suspend_b_suspend_a_resume_b_resume() {
    let gate = Gate::new();
    let c = Arc::new(FakeC::default());
    let (a_s_tx, a_s_rx) = mpsc::channel();
    let (b_s_tx, b_s_rx) = mpsc::channel();
    let (a_r_tx, a_r_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let (ga, ca, da) = (gate.clone(), c.clone(), done_tx.clone());
    let a = thread::spawn(move || {
        let mut scope = ga.enter(data(11)).unwrap();
        let initial = scope.current().unwrap();
        ca.enter(initial);
        scope
            .with_external(
                |v| ca.suspend(v),
                || {
                    a_s_tx.send(initial.activation).unwrap();
                    recv(&b_s_rx);
                    assert_eq!(ga.snapshot().suspended, 2);
                    Ok(())
                },
                |v| ca.resume(v),
            )
            .unwrap();
        scope
            .execute(|v| {
                assert_eq!(v, initial);
                ca.check(v);
            })
            .unwrap();
        ca.leave(initial);
        drop(scope);
        assert_eq!(ga.snapshot().suspended, 1);
        a_r_tx.send(initial.activation).unwrap();
        da.send(()).unwrap();
    });
    let (gb, cb) = (gate.clone(), c.clone());
    let b = thread::spawn(move || {
        let a_id = recv(&a_s_rx);
        let mut scope = gb.enter(data(22)).unwrap();
        let initial = scope.current().unwrap();
        assert_ne!(initial.activation, a_id);
        cb.enter(initial);
        scope
            .with_external(
                |v| cb.suspend(v),
                || {
                    b_s_tx.send(()).unwrap();
                    assert_eq!(recv(&a_r_rx), a_id);
                    Ok(())
                },
                |v| cb.resume(v),
            )
            .unwrap();
        scope
            .execute(|v| {
                assert_eq!(v, initial);
                cb.check(v);
            })
            .unwrap();
        cb.leave(initial);
        drop(scope);
        done_tx.send(()).unwrap();
    });
    recv(&done_rx);
    recv(&done_rx);
    a.join().unwrap();
    b.join().unwrap();
    let events = c.0.lock().unwrap().events.clone();
    assert_eq!(
        events.iter().map(|e| e.0).collect::<Vec<_>>(),
        ['S', 'S', 'R', 'R']
    );
    assert_eq!(events[0].1, events[2].1);
    assert_eq!(events[1].1, events[3].1);
    drained(&gate);
}

#[test]
fn entire_reentrant_depth_and_same_thread_external_callback() {
    let g = Gate::new();
    let permit = g.register_callback().unwrap();
    let c = FakeC::default();
    let outer = g.enter(data(10)).unwrap();
    let middle = permit.enter(data(20)).unwrap();
    let mut inner = permit.enter(data(30)).unwrap();
    let original = inner.current().unwrap();
    assert_eq!(original.depth, 3);
    c.enter(original);
    inner
        .with_external(
            |v| {
                assert_eq!(g.snapshot().active, 3);
                assert_eq!(outer.current(), Err(Error::Transition));
                assert!(matches!(permit.enter(data(90)), Err(Error::Transition)));
                c.suspend(v)
            },
            || {
                let s = g.snapshot();
                assert_eq!((s.active, s.suspended, s.owner), (0, 3, None));
                assert_eq!(outer.current(), Err(Error::WrongActivation));
                let mut callback = permit.enter(data(99)).unwrap();
                let nested = callback.current().unwrap();
                assert_ne!(nested.activation, original.activation);
                assert_eq!(nested.data, data(99));
                c.enter(nested);
                // 同线程新 activation 也可以继续暂停并再次回入。
                callback
                    .with_external(
                        |v| c.suspend(v),
                        || {
                            let leaf = permit.enter(data(88)).unwrap();
                            assert_ne!(leaf.current().unwrap().activation, nested.activation);
                            assert_eq!(g.snapshot().suspended, 4);
                            drop(leaf);
                            Ok(())
                        },
                        |v| c.resume(v),
                    )
                    .unwrap();
                callback.execute(|v| c.check(v)).unwrap();
                c.leave(nested);
                drop(callback);
                Ok(())
            },
            |v| {
                assert_eq!(
                    g.snapshot().owner,
                    Some((original.thread, original.activation))
                );
                assert_eq!(outer.current(), Err(Error::Transition));
                assert!(matches!(permit.enter(data(90)), Err(Error::Transition)));
                c.resume(v)
            },
        )
        .unwrap();
    assert_eq!(inner.current().unwrap(), original);
    assert_eq!(outer.current().unwrap(), original);
    c.leave(original);
    drop(inner);
    assert_eq!(middle.current().unwrap().depth, 2);
    drop(middle);
    assert_eq!(outer.current().unwrap().depth, 1);
    drop(outer);
    drop(permit);
    drained(&g);
}

#[test]
fn closing_permit_and_suspended_generation_drain() {
    let g = Gate::new();
    let permit = g.register_callback().unwrap();
    let mut a = g.enter(data(1)).unwrap();
    let original = a.current().unwrap();
    a.with_external(
        |_| Ok(()),
        || {
            g.begin_close().unwrap();
            assert!(matches!(g.enter(data(2)), Err(Error::Closing)));
            assert!(matches!(g.register_callback(), Err(Error::Closing)));
            assert_eq!(g.try_destroy(), Err(Error::Busy));
            assert_eq!(g.open_next_generation(), Err(Error::Busy));
            thread::scope(|threads| {
                threads
                    .spawn(|| {
                        // 此 callback 在 Closing 之后才首次请求执行权。
                        let callback = permit.enter(data(3)).unwrap();
                        assert_eq!(callback.current().unwrap().generation, original.generation);
                        assert_eq!(g.snapshot().suspended, 1);
                        drop(callback);
                    })
                    .join()
                    .unwrap();
            });
            Ok(())
        },
        |v| {
            assert_eq!(v, original);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(a.current().unwrap(), original);
    drop(a);
    assert_eq!(g.snapshot().pending, 1);
    assert_eq!(g.try_destroy(), Err(Error::Busy));
    drop(permit);
    drained(&g);
    assert_eq!(g.try_destroy(), Ok(original.generation));
    assert_eq!(g.snapshot().generation, None);
    assert_eq!(g.open_next_generation(), Ok(original.generation + 1));
    let next = g.enter(data(4)).unwrap();
    assert_eq!(next.current().unwrap().generation, original.generation + 1);
    drop(next);
    drained(&g);
}

#[test]
fn admitted_waiter_pin_survives_closing_and_permit_drop() {
    let g = Gate::new();
    let a = g.enter(data(1)).unwrap();
    let permit = g.register_callback().unwrap();
    let (done_tx, done_rx) = mpsc::channel();
    let b = thread::spawn(move || {
        let b = permit.enter(data(2)).unwrap();
        drop(permit);
        assert_eq!(b.current().unwrap().generation, 1);
        drop(b);
        done_tx.send(()).unwrap();
    });
    wait_pending(&g, 2);
    g.begin_close().unwrap();
    assert_eq!(g.try_destroy(), Err(Error::Busy));
    assert!(matches!(g.enter(data(0)), Err(Error::Closing)));
    drop(a);
    recv(&done_rx);
    b.join().unwrap();
    drained(&g);
    assert_eq!(g.try_destroy(), Ok(1));
}

#[test]
fn top_level_admitted_before_close_is_not_cancelled() {
    let g = Gate::new();
    let a = g.enter(data(1)).unwrap();
    let b_gate = g.clone();
    let (done_tx, done_rx) = mpsc::channel();
    let b = thread::spawn(move || {
        let b = b_gate.enter(data(2)).unwrap();
        assert_eq!(b.current().unwrap().generation, 1);
        drop(b);
        done_tx.send(()).unwrap();
    });
    wait_pending(&g, 1);
    g.begin_close().unwrap();
    drop(a);
    recv(&done_rx);
    b.join().unwrap();
    drained(&g);
}

#[test]
fn suspend_failure_does_not_yield_or_call_external() {
    let g = Gate::new();
    let mut a = g.enter(data(1)).unwrap();
    let initial = a.current().unwrap();
    let r: Result<(), ExternalFailure<()>> = a.with_external(
        |_| Err(SuspendError::Unchanged),
        || panic!("must not call external"),
        |_| panic!("must not resume"),
    );
    let f = r.err().unwrap();
    assert_eq!(f.kind(), Error::SuspendFailed);
    assert_eq!(f.executed, None); // external 从未执行(SuspendError::Unchanged)
    assert_eq!(a.current().unwrap(), initial);
    assert_eq!((g.snapshot().active, g.snapshot().suspended), (1, 0));
    drop(a);
    drained(&g);
}

#[test]
fn external_error_and_unwind_resume_before_return() {
    for panic_external in [false, true] {
        let g = Gate::new();
        let c = FakeC::default();
        let mut a = g.enter(data(7)).unwrap();
        let initial = a.current().unwrap();
        c.enter(initial);
        let result: Result<(), ExternalFailure<()>> = a.with_external(
            |v| c.suspend(v),
            || {
                let callback = g.enter(data(8)).unwrap();
                assert_ne!(callback.current().unwrap().activation, initial.activation);
                if panic_external {
                    panic!("external Rust panic (not C unwind)");
                }
                Err(())
            },
            |v| c.resume(v),
        );
        assert_eq!(
            result,
            Err(ExternalFailure::after_external(
                if panic_external {
                    Error::ExecutionPanicked
                } else {
                    Error::ExternalFailed
                },
                Err(()),
            ))
        );
        a.execute(|v| {
            assert_eq!(v, initial);
            c.check(v);
        })
        .unwrap();
        c.leave(initial);
        drop(a);
        drained(&g);
    }
}

#[test]
fn ordinary_scope_drop_during_unwind_releases_owner() {
    let g = Gate::new();
    assert!(catch_unwind(AssertUnwindSafe(|| {
        let _a = g.enter(data(1)).unwrap();
        let _b = g.enter(data(2)).unwrap();
        panic!("ordinary Rust unwind");
    }))
    .is_err());
    drained(&g);
    let a = g.enter(data(3)).unwrap();
    drop(a);
    drained(&g);
}

#[test]
fn execution_panic_is_caught_and_lease_remains_valid() {
    let g = Gate::new();
    let mut a = g.enter(data(1)).unwrap();
    assert_eq!(a.execute(|_| panic!("test")), Err(Error::ExecutionPanicked));
    assert!(a.current().is_ok());
    drop(a);
    drained(&g);
}

#[test]
fn hook_failures_poison_without_unlocking_for_a_second_executor() {
    for case in 0..4 {
        let g = Gate::new();
        let mut a = g.enter(data(1)).unwrap();
        let (tx, rx) = mpsc::channel();
        let (start_tx, start_rx) = mpsc::channel();
        let bg = g.clone();
        let b = thread::spawn(move || {
            recv(&start_rx);
            let error = bg.enter(data(2)).err();
            tx.send(error).unwrap();
        });
        let r = a.with_external(
            |_| {
                if case < 2 {
                    start_tx.send(()).unwrap();
                    wait_pending(&g, 1);
                }
                if case == 0 {
                    panic!("suspend panic");
                }
                if case == 1 {
                    return Err(SuspendError::Uncertain);
                }
                Ok(())
            },
            || Ok(()),
            |_| {
                start_tx.send(()).unwrap();
                wait_pending(&g, 1);
                if case == 2 {
                    return Err(());
                }
                panic!("resume panic");
            },
        );
        // 嵌套/清理竞态下的失败可能是 Hook/Suspend/Resume 之一;external 语义已执行与否各 case不同,
        // 这里只断言错误类别透出且 gate 毒化。
        assert!(matches!(
            r.as_ref().map_err(ExternalFailure::kind),
            Err(Error::HookPanicked | Error::SuspendFailed | Error::ResumeFailed)
        ));
        assert_eq!(recv(&rx), Some(Error::Poisoned));
        b.join().unwrap();
        assert!(g.snapshot().poisoned);
        assert!(g.snapshot().owner.is_some());
        assert_eq!(a.current(), Err(Error::Poisoned));
        drop(a);
        assert!(g.snapshot().active + g.snapshot().suspended > 0);
        assert_eq!(g.try_destroy(), Err(Error::Poisoned));
        assert_eq!(g.open_next_generation(), Err(Error::Poisoned));
    }
}

#[test]
fn dropping_a_suspended_sibling_poison_instead_of_clearing_new_owner() {
    let g = Gate::new();
    let outer = g.enter(data(1)).unwrap();
    let mut inner = g.enter(data(2)).unwrap();
    let r = inner.with_external(
        |_| Ok(()),
        || {
            let callback = g.enter(data(3)).unwrap();
            let owner = g.snapshot().owner;
            drop(outer); // misuse: its original activation is still suspended
            assert!(g.snapshot().poisoned);
            assert_eq!(g.snapshot().owner, owner);
            drop(callback);
            Ok(())
        },
        |_| panic!("cannot safely resume poisoned C state"),
    );
    assert_eq!(r.err().map(|f| f.kind()), Some(Error::Poisoned));
    // resume 在 poisoned 状态被拒:external 已经执行过,事实必须随错误透出。
    assert!(r.err().unwrap().executed.is_some());
    drop(inner);
    assert_eq!(g.try_destroy(), Err(Error::Poisoned));
}

#[test]
fn leftover_callback_scope_cannot_self_deadlock_or_steal_lease() {
    let g = Gate::new();
    let mut a = g.enter(data(1)).unwrap();
    let mut leftover = None;
    let r = a.with_external(
        |_| Ok(()),
        || {
            leftover = Some(g.enter(data(2)).unwrap());
            Ok(())
        },
        |_| panic!("callback still running"),
    );
    let f = r.err().unwrap();
    assert_eq!(f.kind(), Error::WrongActivation);
    // 外部闭包留下同线程悬空 scope 被拒:external 本体(Ok(()))已经执行完。
    assert_eq!(f.executed, Some(Ok(())));
    assert!(g.snapshot().poisoned);
    drop(leftover);
    drop(a);
    assert_eq!(g.try_destroy(), Err(Error::Poisoned));
}

#[test]
fn deadline_checkpoint_are_activation_local_and_mutable() {
    let g = Gate::new();
    let mut a = g.enter(data(1)).unwrap();
    a.set_data(data(42)).unwrap();
    a.with_external(
        |v| {
            assert_eq!(v.data, data(42));
            Ok(())
        },
        || {
            let mut b = g.enter(data(2)).unwrap();
            b.set_data(data(77)).unwrap();
            b.with_external(
                |_| Ok(()),
                || {
                    let c = g.enter(data(3)).unwrap();
                    assert_eq!(c.current().unwrap().data, data(3));
                    Ok(())
                },
                |v| {
                    assert_eq!(v.data, data(77));
                    Ok(())
                },
            )
            .unwrap();
            assert_eq!(b.current().unwrap().data, data(77));
            Ok(())
        },
        |v| {
            assert_eq!(v.data, data(42));
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(a.current().unwrap().data, data(42));
    drop(a);
    drained(&g);
}

#[test]
fn registry_arc_clones_are_not_execution_or_lifetime_counts() {
    let g = Gate::new();
    let clones = (0..100).map(|_| g.clone()).collect::<Vec<_>>();
    drained(&g);
    g.begin_close().unwrap();
    assert_eq!(g.try_destroy(), Ok(1));
    assert_eq!(clones[0].snapshot().generation, None);
    assert_eq!(clones[1].open_next_generation(), Ok(2));
}
