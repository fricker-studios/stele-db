//! Build script: generate the admin / control-plane gRPC code from the
//! `v1alpha1` `.proto` contract ([STL-254], [ADR-0016]).
//!
//! The contract is the shared, repo-root `proto/` tree (so the future
//! `stele-client` SDK, [STL-255], builds against the same source of truth).
//! Codegen uses a **vendored** `protoc` (`protoc-bin-vendored`) rather than a
//! system one, so no protobuf compiler need be installed on any build host — the
//! Windows and MSRV CI legs build `stele-server` and must do so hermetically.

use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The proto tree lives at the repo root, two levels up from this crate
    // (`crates/stele-server`). Workspace builds (every CI leg, the release
    // pipeline) resolve it; `stele-server` is a daemon, never `cargo publish`ed,
    // so the package-relative escape is not a concern.
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let proto_root = Path::new(&manifest_dir).join("../../proto");
    let proto = proto_root.join("stele/admin/v1alpha1/admin.proto");

    // Point prost-build at the vendored `protoc` instead of a system install.
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    // SAFETY: a build script runs single-threaded with nothing else reading the
    // environment; setting `PROTOC` before codegen is prost-build's documented
    // way to select the compiler binary.
    #[allow(unsafe_code)]
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }

    tonic_prost_build::configure()
        .build_server(true)
        // The client is generated too: the in-crate integration test drives the
        // service with it, and it is the seed the `stele-client` SDK reuses.
        .build_client(true)
        .compile_protos(&[proto.as_path()], &[proto_root.as_path()])?;

    println!("cargo:rerun-if-changed={}", proto.display());
    Ok(())
}
