use anyhow::{anyhow, Context as _};
use aya_build::Toolchain;

fn main() -> anyhow::Result<()> {
    let cargo_metadata::Metadata { packages, .. } = cargo_metadata::MetadataCommand::new()
        .no_deps()
        .exec()
        .context("MetadataCommand::exec")?;
    let ebpf_package = packages
        .into_iter()
        .find(|cargo_metadata::Package { name, .. }| name.as_str() == "ldmonitor-ebpf")
        .ok_or_else(|| anyhow!("ldmonitor-ebpf package not found"))?;
    let cargo_metadata::Package {
        name, manifest_path, ..
    } = ebpf_package;
    let ebpf_package = aya_build::Package {
        name: name.as_str(),
        root_dir: manifest_path
            .parent()
            .ok_or_else(|| anyhow!("no parent for {manifest_path}"))?
            .as_str(),
        ..Default::default()
    };
    // Keep the eBPF compiler and bpf-linker on the same LLVM major version.
    // The host currently provides LLVM 22 through bpf-linker.
    aya_build::build_ebpf([ebpf_package], Toolchain::Custom("nightly-2026-07-14"))
}
