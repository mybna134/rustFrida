#![no_std]
#![no_main]

use aya_ebpf::{
    helpers::bpf_probe_read_user_str_bytes,
    macros::{map, uprobe},
    maps::{PerCpuArray, PerfEventArray},
    programs::ProbeContext,
    EbpfContext,
};
use ldmonitor_common::{DlopenEvent, MAX_PATH_LEN};

#[map]
static EVENTS: PerfEventArray<DlopenEvent> = PerfEventArray::new(0);

// Keep the 272-byte event outside the 512-byte eBPF stack limit.
#[map]
static mut EVENT_BUF: PerCpuArray<DlopenEvent> = PerCpuArray::with_max_entries(1, 0);

#[uprobe]
pub fn ldmonitor(ctx: ProbeContext) -> u32 {
    match try_ldmonitor(&ctx) {
        Ok(ret) => ret,
        Err(ret) => ret,
    }
}

fn try_ldmonitor(ctx: &ProbeContext) -> Result<u32, u32> {
    // android_dlopen_ext(const char* filename, int flag, const android_dlextinfo* extinfo)
    // 第一个参数是路径指针
    let path_ptr: *const u8 = ctx.arg(0).ok_or(1u32)?;

    // The per-CPU map value is zero-initialized by the kernel and avoids
    // putting the 272-byte event on the eBPF stack.
    let event = unsafe {
        let ptr = EVENT_BUF.get_ptr_mut(0).ok_or(1u32)?;
        &mut *ptr
    };
    event.pid = ctx.pid();
    event.uid = ctx.uid();
    event.path_len = 0;

    // 从用户空间读取路径字符串
    if let Ok(path_bytes) = unsafe { bpf_probe_read_user_str_bytes(path_ptr, &mut event.path) } {
        event.path_len = path_bytes.len() as u32;
    }

    EVENTS.output(ctx, &event, 0);
    Ok(0)
}

#[cfg(all(not(test), target_arch = "bpf"))]
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    loop {}
}

#[unsafe(link_section = "license")]
#[unsafe(no_mangle)]
static LICENSE: [u8; 13] = *b"Dual MIT/GPL\0";
