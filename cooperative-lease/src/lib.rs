//! 独立执行权 / generation pin 模型；不包含 QuickJS 或 FFI。
#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::marker::PhantomData;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, ThreadId};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Closing,
    NoGeneration,
    Busy,
    Poisoned,
    WrongActivation,
    Transition,
    SuspendFailed,
    ResumeFailed,
    HookPanicked,
    ExternalFailed,
    ExternalPanicked,
    ExecutionPanicked,
}

/// 外部调用失败时,必须携带「external 是否已经执行」的事实。
/// - `executed == None`  → 拒绝发生在 external 之前,副作用未发生,调用方可安全重试/重放。
/// - `executed == Some(Ok(v))` → external 已执行且拿到结果;严禁重放,结果应当尽力交还调用方。
/// - `executed == Some(Err(()))` → external 已执行但自身返回失败;严禁重放。
/// 该类型是本次 mixed-lock 事故(Already Executed 双执行)的语义修正核心:
/// 外部副作用(Java one-shot 调用)发生在返回值传递之前,而 resume/gate 阶段
/// 失败只影响「能否把事实带回 JS」,不影响「事实本身」。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExternalFailure<T> {
    pub error: Error,
    pub executed: Option<Result<T, ()>>,
}

impl<T> ExternalFailure<T> {
    pub fn before_external(error: Error) -> Self {
        Self { error, executed: None }
    }
    pub fn after_external(error: Error, executed: Result<T, ()>) -> Self {
        Self { error, executed: Some(executed) }
    }
    /// 兼容旧用法:只关心错误类别。
    pub fn kind(&self) -> Error {
        self.error
    }
}

/// Suspend 失败需明确区分 C 状态未改变与无法确认。后者必须 fail-closed。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SuspendError {
    Unchanged,
    Uncertain,
}

/// 已计算的绝对墙钟 deadline；暂停不重新计时。checkpoint 仅为不透明测试标记。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ActivationData {
    pub deadline_ms: Option<u64>,
    pub checkpoint: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ActivationView {
    pub generation: u64,
    pub activation: u64,
    pub thread: ThreadId,
    pub depth: usize,
    pub data: ActivationData,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Running,
    Suspending,
    Suspended,
    Resuming,
}

struct Activation {
    view: ActivationView,
    phase: Phase,
}

/// 故意不承载任何真实 runtime，也不暴露 runtime 引用。
struct Generation {
    id: u64,
}

struct State {
    generation: Option<Arc<Generation>>,
    next_generation: u64,
    next_activation: u64,
    closing: bool,
    poisoned: bool,
    owner: Option<(ThreadId, u64)>,
    activations: HashMap<u64, Activation>,
    active: usize,
    suspended: usize,
    pending: usize, // callback registrations + admitted waiters
}

struct Shared {
    state: Mutex<State>,
    changed: Condvar,
}

impl Drop for Shared {
    fn drop(&mut self) {
        let s = match self.state.get_mut() {
            Ok(s) => s,
            Err(error) => {
                let s = error.into_inner();
                s.poisoned = true;
                s
            }
        };
        if s.poisoned
            || s.active != 0
            || s.suspended != 0
            || s.pending != 0
            || s.owner.is_some()
            || !s.activations.is_empty()
        {
            // 即使管理器最后一个 Arc 被丢弃，也不能绕过 fail-closed 拒绝销毁。
            // 无可靠 C 恢复路径时有意保留 generation；绝不自动清毒。
            if let Some(pin) = s.generation.take() {
                std::mem::forget(pin);
            }
        }
    }
}

/// Arc 保活与 active/suspended/pending 计数独立；Mutex 仅用于短状态转换。
#[derive(Clone)]
pub struct Gate(Arc<Shared>);

/// 可跨线程存放的注册能力。只能在 Open 时注册，不提供 bypass-closing bool。
/// 不可 Clone；如需多入口共享，应在注销协议内共享 Arc<CallbackPermit>。
pub struct CallbackPermit {
    gate: Gate,
    pin: Arc<Generation>,
}

/// Owned 和 reentrant 均为同一个 gate 上的逻辑 scope，不持有 MutexGuard。
/// Rc 标记使其 !Send + !Sync，必须在原 OS 线程暂停和恢复。
///
/// ```compile_fail
/// use cooperative_lease::{ActivationData, Gate};
/// let scope = Gate::new().enter(ActivationData::default()).unwrap();
/// std::thread::spawn(move || drop(scope));
/// ```
pub struct Scope {
    gate: Gate,
    pin: Arc<Generation>,
    activation: u64,
    thread: ThreadId,
    _local: PhantomData<Rc<()>>,
}

/// 只在 with_external 内部存在，不允许遗失/重复 resume 的公开 token API。
struct SuspendedToken {
    view: ActivationView,
    _local: PhantomData<Rc<()>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub generation: Option<u64>,
    pub closing: bool,
    pub poisoned: bool,
    pub owner: Option<(ThreadId, u64)>,
    pub active: usize,
    pub suspended: usize,
    pub pending: usize,
    pub activations: usize,
}

impl Default for Gate {
    fn default() -> Self {
        Self::new()
    }
}

impl Gate {
    pub fn new() -> Self {
        Self(Arc::new(Shared {
            state: Mutex::new(State {
                generation: Some(Arc::new(Generation { id: 1 })),
                next_generation: 2,
                next_activation: 1,
                closing: false,
                poisoned: false,
                owner: None,
                activations: HashMap::new(),
                active: 0,
                suspended: 0,
                pending: 0,
            }),
            changed: Condvar::new(),
        }))
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        match self.0.state.lock() {
            Ok(state) => state,
            Err(error) => {
                let mut state = error.into_inner();
                state.poisoned = true;
                self.0.changed.notify_all();
                state
            }
        }
    }

    fn wait<'a>(&self, state: MutexGuard<'a, State>) -> MutexGuard<'a, State> {
        match self.0.changed.wait(state) {
            Ok(state) => state,
            Err(error) => {
                let mut state = error.into_inner();
                state.poisoned = true;
                self.0.changed.notify_all();
                state
            }
        }
    }

    /// Permanently quarantine a generation after the C adapter reports a
    /// state transition that cannot be safely rolled back.
    pub fn poison(&self) {
        self.lock().poisoned = true;
        self.0.changed.notify_all();
    }

    pub fn snapshot(&self) -> Snapshot {
        let s = self.lock();
        Snapshot {
            generation: s.generation.as_ref().map(|g| g.id),
            closing: s.closing,
            poisoned: s.poisoned,
            owner: s.owner,
            active: s.active,
            suspended: s.suspended,
            pending: s.pending,
            activations: s.activations.len(),
        }
    }

    pub fn register_callback(&self) -> Result<CallbackPermit, Error> {
        let mut s = self.lock();
        Self::healthy(&s)?;
        if s.closing {
            return Err(Error::Closing);
        }
        let pin = s.generation.clone().ok_or(Error::NoGeneration)?;
        s.pending += 1;
        Ok(CallbackPermit {
            gate: self.clone(),
            pin,
        })
    }

    pub fn enter(&self, data: ActivationData) -> Result<Scope, Error> {
        self.enter_impl(data, None, false)
    }

    /// Destruction code may acquire the final lease after Closing has stopped
    /// ordinary admission. It must still wait behind every admitted callback.
    pub fn enter_for_retirement(&self, data: ActivationData) -> Result<Scope, Error> {
        self.enter_impl(data, None, true)
    }

    fn healthy(s: &State) -> Result<(), Error> {
        if s.poisoned {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }

    fn enter_impl(
        &self,
        data: ActivationData,
        permit: Option<&CallbackPermit>,
        retirement: bool,
    ) -> Result<Scope, Error> {
        let tid = thread::current().id();
        let mut s = self.lock();
        Self::healthy(&s)?;
        if retirement && !s.closing {
            return Err(Error::Transition);
        }
        if permit.is_none() && s.closing && !retirement {
            return Err(Error::Closing);
        }
        let pin = s.generation.clone().ok_or(Error::NoGeneration)?;
        if let Some(permit) = permit {
            if !Arc::ptr_eq(&permit.pin, &pin) || !Arc::ptr_eq(&permit.gate.0, &self.0) {
                return Err(Error::WrongActivation);
            }
        }
        // Admission 自此保活；Closing 不撤销已被接受的 waiter。
        s.pending += 1;
        loop {
            if s.poisoned {
                s.pending -= 1;
                self.0.changed.notify_all();
                return Err(Error::Poisoned);
            }
            if let Some((owner, id)) = s.owner {
                if owner == tid {
                    let Some(activation) = s.activations.get_mut(&id) else {
                        s.pending -= 1;
                        s.poisoned = true;
                        self.0.changed.notify_all();
                        return Err(Error::WrongActivation);
                    };
                    if activation.phase != Phase::Running {
                        s.pending -= 1;
                        return Err(Error::Transition);
                    }
                    // 活跃同线程递归继承 activation 的 deadline/checkpoint。
                    activation.view.depth += 1;
                    s.active += 1;
                    s.pending -= 1;
                    return Ok(Scope {
                        gate: self.clone(),
                        pin,
                        activation: id,
                        thread: tid,
                        _local: PhantomData,
                    });
                }
                s = self.wait(s);
                continue;
            }
            let id = s.next_activation;
            let Some(next) = id.checked_add(1) else {
                s.pending -= 1;
                s.poisoned = true;
                self.0.changed.notify_all();
                return Err(Error::Poisoned);
            };
            s.next_activation = next;
            s.activations.insert(
                id,
                Activation {
                    view: ActivationView {
                        generation: pin.id,
                        activation: id,
                        thread: tid,
                        depth: 1,
                        data,
                    },
                    phase: Phase::Running,
                },
            );
            s.owner = Some((tid, id));
            s.active += 1;
            s.pending -= 1;
            return Ok(Scope {
                gate: self.clone(),
                pin,
                activation: id,
                thread: tid,
                _local: PhantomData,
            });
        }
    }

    pub fn begin_close(&self) -> Result<(), Error> {
        let mut s = self.lock();
        Self::healthy(&s)?;
        if s.generation.is_none() {
            return Err(Error::NoGeneration);
        }
        s.closing = true;
        self.0.changed.notify_all();
        Ok(())
    }

    /// 非阻塞排空检查，避免在当前线程还有 scope 时等待自身。这里只退役模型 generation。
    pub fn try_destroy(&self) -> Result<u64, Error> {
        let mut s = self.lock();
        Self::healthy(&s)?;
        if !s.closing
            || s.active != 0
            || s.suspended != 0
            || s.pending != 0
            || s.owner.is_some()
            || !s.activations.is_empty()
        {
            return Err(Error::Busy);
        }
        Ok(s.generation.take().ok_or(Error::NoGeneration)?.id)
    }

    pub fn open_next_generation(&self) -> Result<u64, Error> {
        let mut s = self.lock();
        Self::healthy(&s)?;
        if s.generation.is_some() {
            return Err(Error::Busy);
        }
        let id = s.next_generation;
        let Some(next) = id.checked_add(1) else {
            s.poisoned = true;
            return Err(Error::Poisoned);
        };
        s.next_generation = next;
        s.generation = Some(Arc::new(Generation { id }));
        s.closing = false;
        Ok(id)
    }
}

impl CallbackPermit {
    pub fn enter(&self, data: ActivationData) -> Result<Scope, Error> {
        self.gate.enter_impl(data, Some(self), false)
    }
}

impl Drop for CallbackPermit {
    fn drop(&mut self) {
        let mut s = self.gate.lock();
        if s.generation
            .as_ref()
            .is_some_and(|g| Arc::ptr_eq(g, &self.pin))
            && s.pending > 0
        {
            s.pending -= 1;
        } else {
            s.poisoned = true;
        }
        self.gate.0.changed.notify_all();
    }
}

impl Scope {
    fn validate(&self, s: &State, phase: Phase) -> Result<ActivationView, Error> {
        Gate::healthy(s)?;
        if thread::current().id() != self.thread
            || s.owner != Some((self.thread, self.activation))
            || !s
                .generation
                .as_ref()
                .is_some_and(|g| Arc::ptr_eq(g, &self.pin))
        {
            return Err(Error::WrongActivation);
        }
        let activation = s
            .activations
            .get(&self.activation)
            .ok_or(Error::WrongActivation)?;
        if activation.phase != phase {
            return Err(Error::Transition);
        }
        Ok(activation.view)
    }

    pub fn current(&self) -> Result<ActivationView, Error> {
        self.validate(&self.gate.lock(), Phase::Running)
    }

    pub fn set_data(&mut self, data: ActivationData) -> Result<(), Error> {
        let mut s = self.gate.lock();
        self.validate(&s, Phase::Running)?;
        if let Some(a) = s.activations.get_mut(&self.activation) {
            a.view.data = data;
        }
        Ok(())
    }

    /// 仅模拟进入 JS；phase 未恢复前拒绝运行闭包。所有用户闭包在 Mutex 外调用。
    pub fn execute<T>(&mut self, body: impl FnOnce(ActivationView) -> T) -> Result<T, Error> {
        let view = self.current()?;
        caught(|| body(view)).map_err(|_| Error::ExecutionPanicked)
    }

    /// suspend 返回 Ok 后才交还 owner；重取 owner 后先 resume，成功后才恢复 Running。
    /// SuspendError::Unchanged 必须保证 C 完全未变更；无法保证时返回 Uncertain，
    /// 显式 poison。hook panic 也会 fail-closed，不允许 panic 穿过真实 C 边界。
    /// external 的 Err / Rust unwind 均先 resume，随后作为显式 Error 返回。
    pub fn with_external<T>(
        &mut self,
        suspend: impl FnOnce(ActivationView) -> Result<(), SuspendError>,
        external: impl FnOnce() -> Result<T, ()>,
        resume: impl FnOnce(ActivationView) -> Result<(), ()>,
    ) -> Result<T, ExternalFailure<T>> {
        let view = {
            let mut s = self.gate.lock();
            let view = self.validate(&s, Phase::Running)
                .map_err(ExternalFailure::before_external)?;
            if let Some(a) = s.activations.get_mut(&self.activation) {
                a.phase = Phase::Suspending;
            }
            view
        };
        match caught(|| suspend(view)) {
            Ok(Ok(())) => {}
            Ok(Err(SuspendError::Unchanged)) => {
                let mut s = self.gate.lock();
                self.validate(&s, Phase::Suspending)
                    .map_err(ExternalFailure::before_external)?;
                if let Some(a) = s.activations.get_mut(&self.activation) {
                    a.phase = Phase::Running;
                }
                return Err(ExternalFailure::before_external(Error::SuspendFailed));
            }
            Ok(Err(SuspendError::Uncertain)) => {
                self.gate.poison();
                return Err(ExternalFailure::before_external(Error::SuspendFailed));
            }
            Err(()) => {
                self.gate.poison();
                return Err(ExternalFailure::before_external(Error::HookPanicked));
            }
        }
        let token = SuspendedToken {
            view,
            _local: PhantomData,
        };
        {
            let mut s = self.gate.lock();
            self.validate(&s, Phase::Suspending)
                .map_err(ExternalFailure::before_external)?;
            if let Some(a) = s.activations.get_mut(&self.activation) {
                a.phase = Phase::Suspended;
            }
            s.active -= token.view.depth;
            s.suspended += token.view.depth;
            s.owner = None;
            self.gate.0.changed.notify_all();
        }
        let output = caught(external);
        // 从这里起,external 已经执行:任何失败都必须携带它的事实与结果。
        let external_panicked = output.is_err();
        let mut executed: Option<Result<T, ()>> = Some(match output {
            Ok(inner) => inner,
            Err(()) => Err(()), // panic:副作用已发生,结果不可信,按已执行失败上报
        });
        // token 按 activation ID 定位，绝不能从全局 suspended 栈 pop。
        {
            let mut s = self.gate.lock();
            loop {
                if let Err(e) = Gate::healthy(&s) {
                    s.poisoned = true;
                    self.gate.0.changed.notify_all();
                    return Err(ExternalFailure::after_external(e, executed.take().unwrap()));
                }
                if let Some((tid, _)) = s.owner {
                    if tid == self.thread {
                        // 外部闭包遗留了同线程新 activation 的 scope，不能自等或夺权。
                        s.poisoned = true;
                        self.gate.0.changed.notify_all();
                        return Err(ExternalFailure::after_external(
                            Error::WrongActivation,
                            executed.take().unwrap(),
                        ));
                    }
                    s = self.gate.wait(s);
                    continue;
                }
                let valid = s
                    .activations
                    .get(&token.view.activation)
                    .is_some_and(|a| a.phase == Phase::Suspended && a.view == token.view);
                if !valid {
                    s.poisoned = true;
                    self.gate.0.changed.notify_all();
                    return Err(ExternalFailure::after_external(
                        Error::WrongActivation,
                        executed.take().unwrap(),
                    ));
                }
                s.owner = Some((self.thread, token.view.activation));
                if let Some(a) = s.activations.get_mut(&token.view.activation) {
                    a.phase = Phase::Resuming;
                }
                break;
            }
        }
        match caught(|| resume(token.view)) {
            Ok(Ok(())) => {}
            Ok(Err(())) => {
                self.gate.poison();
                return Err(ExternalFailure::after_external(
                    Error::ResumeFailed,
                    executed.take().unwrap(),
                ));
            }
            Err(()) => {
                self.gate.poison();
                return Err(ExternalFailure::after_external(
                    Error::HookPanicked,
                    executed.take().unwrap(),
                ));
            }
        }
        {
            let mut s = self.gate.lock();
            if let Err(e) = self.validate(&s, Phase::Resuming) {
                return Err(ExternalFailure::after_external(e, executed.take().unwrap()));
            }
            if let Some(a) = s.activations.get_mut(&self.activation) {
                a.phase = Phase::Running;
            }
            s.suspended -= token.view.depth;
            s.active += token.view.depth;
        }
        // 尾段:三段全成则 external 的结果正式交还;external 自体失败保留旧语义。
        match executed.take().unwrap_or(Err(())) {
            Ok(value) => Ok(value),
            Err(()) => Err(ExternalFailure::after_external(
                if external_panicked { Error::ExecutionPanicked } else { Error::ExternalFailed },
                Err(()),
            )),
        }
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        let mut s = self.gate.lock();
        // 无法证明 C 可恢复时保留 owner / 计数，永久拒绝执行和销毁；不在 Drop 中 panic。
        if self.validate(&s, Phase::Running).is_err() {
            s.poisoned = true;
            self.gate.0.changed.notify_all();
            return;
        }
        if let Some(a) = s.activations.get_mut(&self.activation) {
            a.view.depth -= 1;
            if a.view.depth == 0 {
                s.activations.remove(&self.activation);
                s.owner = None;
            }
        }
        s.active -= 1;
        self.gate.0.changed.notify_all();
    }
}

// 捕获测试闭包的 Rust panic，不 rethrow。自定义 panic payload 的 Drop 也可能 panic；
// 仅在异常路径泄漏 payload，避免双重 unwind。panic=abort / abort / longjmp 不可恢复。
fn caught<T>(body: impl FnOnce() -> T) -> Result<T, ()> {
    catch_unwind(AssertUnwindSafe(body)).map_err(|payload| {
        std::mem::forget(payload);
    })
}

#[cfg(test)]
mod lifetime_tests {
    use super::*;

    #[test]
    fn generation_is_freed_only_after_explicit_pins_drain() {
        let gate = Gate::new();
        let weak = Arc::downgrade(gate.lock().generation.as_ref().unwrap());
        let permit = gate.register_callback().unwrap();
        let mut scope = gate.enter(ActivationData::default()).unwrap();
        gate.begin_close().unwrap();
        scope
            .with_external(
                |_| Ok(()),
                || {
                    assert!(weak.upgrade().is_some());
                    assert_eq!(gate.try_destroy(), Err(Error::Busy));
                    Ok(())
                },
                |_| Ok(()),
            )
            .unwrap();
        drop(scope);
        assert!(weak.upgrade().is_some());
        assert_eq!(gate.try_destroy(), Err(Error::Busy));
        drop(permit);
        assert_eq!(gate.try_destroy(), Ok(1));
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn poisoned_generation_survives_last_handle_drop() {
        let gate = Gate::new();
        let weak = Arc::downgrade(gate.lock().generation.as_ref().unwrap());
        let mut scope = gate.enter(ActivationData::default()).unwrap();
        let f = scope
            .with_external(|_| Ok(()), || Ok(4321), |_| Err(()))
            .err()
            .expect("resume must fail");
        assert_eq!(f.kind(), Error::ResumeFailed);
        // resume 拒绝时 external 已经执行:其结果必须随失败透出,调用方严禁重放。
        assert_eq!(f.executed, Some(Ok(4321)));
        drop(scope);
        drop(gate);
        // 有意隔离保留，不依赖外部管理器永远不 Drop。
        assert!(weak.upgrade().is_some());
    }

    #[test]
    fn poisoned_std_mutex_is_not_silently_recovered() {
        let gate = Gate::new();
        let _ = catch_unwind(AssertUnwindSafe(|| {
            let _state = gate.0.state.lock().unwrap();
            panic!("injected state-lock poison");
        }));
        assert!(gate.snapshot().poisoned);
        assert!(matches!(
            gate.enter(ActivationData::default()),
            Err(Error::Poisoned)
        ));
        assert_eq!(gate.try_destroy(), Err(Error::Poisoned));
    }
}
