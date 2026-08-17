//! build.rs — 用 prost-build 从 proto/bailian.proto 生成 Rust 类型
//!
//! 用 `protoc-bin-vendored` 提供预编译 protoc 二进制，避免要求 host 安装 protoc。
//!
//! 注：prost 0.13 默认 `bytes` 字段会生成 `bytes::Bytes`；这里保持默认行为，使用侧用
//! `.to_vec()` 取得 `Vec<u8>`。

fn main() -> anyhow::Result<()> {
    // 让 prost-build 用 vendored 的 protoc（无需 host 系统装 protoc）
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    let mut cfg = prost_build::Config::new();
    cfg.compile_protos(&["proto/bailian.proto"], &["proto/"])?;
    println!("cargo:rerun-if-changed=proto/bailian.proto");
    println!("cargo:rerun-if-changed=build.rs");
    Ok(())
}