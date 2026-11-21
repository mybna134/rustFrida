<div align="center">

# rustFrida

ARM64 Android 动态插桩框架

[![Android](https://img.shields.io/badge/Android-ARM64-3DDC84?style=for-the-badge&logo=android)](https://developer.android.com/ndk)
[![Upstream](https://img.shields.io/badge/Upstream-kkkbbb%2FrustFrida-181717?style=for-the-badge&logo=github)](https://github.com/kkkbbb/rustFrida)

</div>

基于 [kkkbbb/rustFrida](https://github.com/kkkbbb/rustFrida) 修改，保留 Java / Native Hook、QuickJS 脚本、Spawn 注入和 HTTP RPC 等能力，主要改进运行稳定性。

## 修改

- **并发执行**：新增执行租约与 QuickJS 协作暂停机制，外部阻塞调用期间可让出 JS 执行权。
- **Hook 调用**：防止恢复失败时重复执行原方法，改进 Java 异常传递。
- **安全卸载**：控制端断开后进入清理，等待后台线程退出；无法确认安全时保留资源。
- **驻留模式**：新增 `--stay-alive`，stdin 关闭后继续运行，通过 SIGINT / SIGTERM 退出。
- **Android 兼容性**：改进后台进程解冻、ELF 加载、内存分配与重编译，更新 NDK 和 eBPF 构建配置。


## 构建

需要 Rust、Android NDK、Python 3，以及 eBPF 所需的 `nightly-2026-07-14` 和匹配 LLVM 版本的 `bpf-linker`。先将 `.cargo/config.toml` 中的 NDK 路径改为本机路径。

```bash
git submodule update --init --recursive
rustup target add aarch64-linux-android
python3 loader/build_helpers.py
cargo build -p agent --release
cargo build -p rust_frida --release
```

产物：`target/aarch64-linux-android/release/rustfrida`。

## 使用

将产物部署到 Android 设备后运行：

```bash
# 注入运行中的进程并加载脚本
./rustfrida --pid <pid> -l script.js

# 非交互驻留，Ctrl+C 退出（需配合 -l，不支持 Spawn）
./rustfrida --pid <pid> -l script.js --stay-alive

# 启动时注入
./rustfrida --spawn com.example.app -l script.js
```

更多 API 与使用说明见[原仓库文档](https://github.com/kkkbbb/rustFrida#readme)。
