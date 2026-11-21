//! agent 侧 raw 线程包装。
//!
//! 真正的 raw clone 实现在 `quickjs_hook::raw_thread`（两个 crate 各有一份
//! 历史副本；本文件现在只做转发，保证 detached 线程计数/卸载闸门全局统一）。
//!
//! 背景：loader 在 agent 卸载时会把 agent ELF munmap。任何存活的 detached
//! raw 线程（如 Java hook 安装后的延迟 entry-refresh 线程）醒来取指会落在
//! 已 unmap 的 text 上，整个宿主进程 SIGSEGV —— 被壳兜底弹成"检测到调试器"。
//! 因此 spawn_detached 统一计数，unload 前必须 wait_detached_drain。

use quickjs_hook as _;

pub(crate) fn spawn_detached<F>(name: &'static [u8], func: F) -> Result<pid_t, String>
where
    F: FnOnce() + Send + 'static,
{
    quickjs_hook::raw_thread::spawn_detached_shared(name, func)
}

pub(crate) fn wait_detached_drain(timeout_ms: u64) -> bool {
    quickjs_hook::raw_thread::wait_detached_drain(timeout_ms)
}

pub(crate) fn close_detached_gate() {
    quickjs_hook::raw_thread::close_detached_gate();
}

pub(crate) fn detached_gate_closed() -> bool {
    quickjs_hook::raw_thread::detached_gate_closed()
}

pub(crate) fn sleep_ms(ms: i64) {
    quickjs_hook::raw_thread::sleep_ms_pub(ms);
}

use libc::pid_t;
