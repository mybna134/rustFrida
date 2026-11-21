//! The single execution lease for the process-wide QuickJS runtime.
//!
//! A suspended C activation stays on its original OS thread. The gate lets a
//! different activation run while the original thread is inside a blocking
//! Java or native call, then restores the original activation before JS runs.

use cooperative_lease::{ActivationData, Error, ExternalFailure, Gate, Scope, SuspendError};
use std::cell::Cell;
use std::sync::OnceLock;

/// 外部调用失败的完整事实。「是否已执行」决定下游是否允许重放。
pub(crate) enum ExternalOutcome<T> {
    /// 拒绝发生在 external 之前:副作用未发生,调用方可安全重试/重放。
    NotExecuted(Error),
    /// external 已执行(sub-closure 成功返回值或自身失败)。
    /// 严禁重放:副作用已在真实世界生效,结果/失败必须尽力向调用方交代。
    Executed(Error, Option<T>),
}

impl<T> ExternalOutcome<T> {
    pub(crate) fn kind(&self) -> Error {
        match self {
            ExternalOutcome::NotExecuted(e) | ExternalOutcome::Executed(e, _) => *e,
        }
    }
}

thread_local! {
    /// 本线程最近一次 $orig 外部调用「已执行」的事实(js_call_original 设置,
    /// java hook 收尾读取;回调驱动完成时强制清零防串扰)。
    static EXECUTED_EXTERNAL: Cell<bool> = const { Cell::new(false) };
    /// external 已执行且原方法抛出的 Java 异常(全局引用,收尾按身份 Throw 回 JVM)。
    static EXECUTED_PENDING_THROWABLE: Cell<u64> = const { Cell::new(0) };
}

pub(crate) fn mark_external_executed(pending_throwable: u64) {
    EXECUTED_EXTERNAL.with(|c| c.set(true));
    EXECUTED_PENDING_THROWABLE.with(|c| c.set(pending_throwable));
}

pub(crate) fn take_external_executed() -> bool {
    EXECUTED_EXTERNAL.with(|c| c.replace(false))
}

pub(crate) fn take_pending_executed_throwable() -> u64 {
    EXECUTED_PENDING_THROWABLE.with(|c| c.replace(0))
}

/// 回调驱动每轮结束的防串扰兜底:未消费的标记一律丢弃。
pub(crate) fn clear_external_markers() {
    let throwable = take_pending_executed_throwable();
    if throwable != 0 {
        // 标记未被收尾消费;全局引用泄漏一个,绝不复用(防止错误恢复陈旧异常)。
    }
    let _ = take_external_executed();
}

use crate::ffi;

static GATE: OnceLock<Gate> = OnceLock::new();

thread_local! {
    static CURRENT_SCOPE: Cell<*mut Scope> = const { Cell::new(std::ptr::null_mut()) };
    static EXTERNAL_SCOPE: Cell<*mut Scope> = const { Cell::new(std::ptr::null_mut()) };
}

struct ExternalScopeGuard(*mut Scope);

impl Drop for ExternalScopeGuard {
    fn drop(&mut self) {
        EXTERNAL_SCOPE.with(|cell| cell.set(self.0));
    }
}

fn gate() -> &'static Gate {
    GATE.get_or_init(Gate::new)
}

pub(crate) struct EngineScope {
    scope: Option<Box<Scope>>,
    previous: *mut Scope,
}

impl EngineScope {
    pub(crate) fn enter(timeout_ms: Option<u64>) -> Result<Self, Error> {
        let deadline_ms = timeout_ms.map(|timeout| crate::monotonic_ms().saturating_add(timeout));
        let scope = gate().enter(ActivationData {
            deadline_ms,
            checkpoint: None,
        })?;
        Ok(Self::from_scope(scope))
    }

    pub(crate) fn enter_retirement() -> Result<Self, Error> {
        Ok(Self::from_scope(gate().enter_for_retirement(ActivationData::default())?))
    }

    pub(crate) fn enter_cleanup() -> Result<Self, Error> {
        match Self::enter(None) {
            Err(Error::Closing) => Self::enter_retirement(),
            result => result,
        }
    }

    fn from_scope(scope: Scope) -> Self {
        let mut scope = Box::new(scope);
        let previous = CURRENT_SCOPE.with(|cell| cell.replace(&mut *scope));
        Self { scope: Some(scope), previous }
    }
}

impl Drop for EngineScope {
    fn drop(&mut self) {
        if self.scope.as_ref().is_some_and(|scope| scope.current().is_ok()) {
            if let Some(engine) = unsafe { crate::JS_ENGINE.get() }.as_ref() {
                // BUSY means an outer JS frame still owns this host job. That
                // outer scope will clear its keep-alive roots on return.
                unsafe { ffi::JS_ClearKeptObjects(engine.runtime().as_ptr()) };
            }
        }
        drop(self.scope.take());
        CURRENT_SCOPE.with(|cell| cell.set(self.previous));
    }
}

pub(crate) fn current_deadline_expired() -> bool {
    CURRENT_SCOPE.with(|cell| {
        let ptr = cell.get();
        if ptr.is_null() {
            return false;
        }
        let Ok(view) = (unsafe { &*ptr }).current() else {
            return true;
        };
        view.data.deadline_ms.is_some_and(|deadline| crate::monotonic_ms() >= deadline)
    })
}

/// # Safety
/// `external` must neither touch QuickJS values nor destroy an object whose
/// destructor can access QuickJS. It must return plain native data only.
/// The caller must retain the original thread's JNI references until return.
pub(crate) unsafe fn with_external<T>(
    ctx: *mut ffi::JSContext,
    external: impl FnOnce() -> Result<T, ()>,
) -> Result<T, ExternalOutcome<T>> {
    let ptr = CURRENT_SCOPE.with(Cell::get);
    if ptr.is_null() {
        return Err(ExternalOutcome::NotExecuted(Error::WrongActivation));
    }
    let state = ffi::JS_NewCooperativeState(ctx);
    if state.is_null() {
        return Err(ExternalOutcome::NotExecuted(Error::SuspendFailed));
    }
    let owner = crate::current_thread_id_u64();
    let suspended = Cell::new(false);
    let result = (&mut *ptr).with_external(
        |_| {
            if ffi::JS_SuspendExecution(ctx, state, owner) == 0 {
                suspended.set(true);
                Ok(())
            } else {
                Err(SuspendError::Unchanged)
            }
        },
        || {
            let previous = EXTERNAL_SCOPE.with(|cell| cell.replace(ptr));
            let _external_scope = ExternalScopeGuard(previous);
            external()
        },
        |_| {
            if ffi::JS_ResumeExecution(ctx, state, owner) == 0 {
                suspended.set(false);
                Ok(())
            } else {
                Err(())
            }
        },
    );
    if !suspended.get() && ffi::JS_FreeCooperativeState(ctx, state) != 0 {
        gate().poison();
        // 此分支 external 必已执行(suspend 成功过才会走到 Free):持有其结果并透出,
        // 严禁下游重放。
        let executed_value = match result {
            Ok(v) => Some(v),
            Err(_) => None,
        };
        return Err(ExternalOutcome::Executed(Error::ResumeFailed, executed_value));
    }
    match result {
        Ok(value) => Ok(value),
        Err(failure) => Err(ExternalFailureToOutcome(failure).into_outcome()),
    }
}

/// lease 层失败到 quickjs-hook 三态的翻译。
struct ExternalFailureToOutcome<T>(ExternalFailure<T>);

impl<T> ExternalFailureToOutcome<T> {
    fn into_outcome(self) -> ExternalOutcome<T> {
        let ExternalFailure { error, executed } = self.0;
        match executed {
            None => ExternalOutcome::NotExecuted(error),
            Some(Ok(value)) => ExternalOutcome::Executed(error, Some(value)),
            Some(Err(())) => ExternalOutcome::Executed(error, None),
        }
    }
}

/// Runs a native-only executor operation from a JS callback or from an
/// ordinary non-JS thread. The latter has no QuickJS activation to suspend.
pub(crate) unsafe fn with_external_current<T>(
    external: impl FnOnce() -> Result<T, ()>,
) -> Result<T, Error> {
    let current = CURRENT_SCOPE.with(Cell::get);
    if current.is_null() || EXTERNAL_SCOPE.with(Cell::get) == current {
        return external().map_err(|_| Error::ExternalFailed);
    }
    let Some(engine) = crate::JS_ENGINE.get().as_ref() else {
        return external().map_err(|_| Error::ExternalFailed);
    };
    let ctx = engine.context().as_ptr();
    // 这些调用点(JS executor/reflect/jni 桥)没有重放语义,失败类别即所需全部信息;
    // 「已执行」事实仅对 $orig(hook.rs 收尾)有决策意义,由 js_call_original 自行处理。
    with_external(ctx, external).map_err(|o| o.kind())
}

pub(crate) unsafe fn error_value_outcome<T>(
    ctx: *mut ffi::JSContext,
    outcome: ExternalOutcome<T>,
) -> ffi::JSValue {
    error_value(ctx, outcome.kind())
}

pub(crate) unsafe fn error_value(ctx: *mut ffi::JSContext, error: Error) -> ffi::JSValue {
    if matches!(error, Error::SuspendFailed | Error::ExternalFailed | Error::ExternalPanicked) {
        ffi::JS_ThrowInternalError(
            ctx,
            b"cooperative external call failed\0".as_ptr() as *const _,
        )
    } else {
        // The activation may still be suspended. Do not call another QuickJS
        // API against a poisoned runtime to manufacture an exception.
        ffi::qjs_exception()
    }
}

pub(crate) fn begin_close() -> Result<(), Error> {
    gate().begin_close()
}

pub(crate) fn ready_to_retire() -> bool {
    let state = gate().snapshot();
    !state.poisoned && state.closing && state.owner.is_some() &&
        state.active == 1 && state.suspended == 0 && state.pending == 0 &&
        state.activations == 1
}

pub(crate) fn cleanup_scope_is_safe() -> bool {
    let state = gate().snapshot();
    !state.closing || ready_to_retire()
}

pub(crate) fn try_destroy() -> Result<u64, Error> {
    gate().try_destroy()
}

pub(crate) fn open_next_generation() -> Result<u64, Error> {
    gate().open_next_generation()
}
