# Module core-bridge

JDK 25 FFM bindings onto Temporal's Rust SDK-Core, plus the Kotlin/Java protobuf classes the SDK
speaks in (published separately as `:protos`).

Interop goes through **kt-bridge** (`rust/kt-bridge`), a small `cdylib` written for this SDK. It
depends on `temporalio-sdk-core` from crates.io like any other cargo dependency: there is no
submodule, no fork, and no vendored source tree.

## Why not the C API

The bridge used to be Temporal's official `sdk-core-c-bridge`. That meant 21.5k lines of
hand-maintained FFM struct layouts whose staleness was silent (Rust reads past the end of a
Java-allocated struct rather than failing to compile), callbacks arriving on Tokio threads via FFM
upcall stubs, and a fork of SDK-Core carrying two patches. kt-bridge is built on four decisions
that make those failure classes unrepresentable rather than worked around:

- **A completion queue, zero upcalls.** Async calls take a caller-supplied `req_id` and return
  immediately; platform pump threads block in `kt_poller_poll` and drain batches. Nothing Rust
  does ever enters the JVM, so there are no upcall stubs to crash in, no arena that can be freed
  underneath one, and cancellation is real.
- **Rust owns the poll loops.** Core's "lang must keep polling until `ShutDown`" contract is
  discharged inside Rust, so Kotlin just reads a `Channel` and cancelling the consumer cannot
  break Core.
- **Protobuf is the config ABI.** Exactly one `#[repr(C)]` struct crosses the boundary
  (`KtCompletion`, 48 bytes of naturally-aligned scalars), checked against `kt_abi_probe` at
  class-init. Adding a worker option is a field addition, not a coordinated struct-layout change
  in two languages.
- **Generation-counted handles, never pointers.** Use-after-free returns `KT_ERR_STALE_HANDLE`
  and type confusion returns `KT_ERR_WRONG_HANDLE_KIND`, instead of corrupting memory.

Both fork patches turned out to be C-bridge defects rather than Core limitations, and neither is
needed: `EphemeralServer::child_process_id()` has always been public Rust API, and the finalized
worker is a `WorkerState::Finalized` variant with no field to unwrap.

## Build

```bash
./gradlew :core-bridge:build          # host platform
./gradlew :core-bridge:copyAllNativeLibs   # all four shipped classifiers
```

Requires a Rust toolchain matching `rust/kt-bridge/rust-toolchain.toml` and `protoc`. Cross
targets must be installed (`rustup target add ...`); the toolchain file lists them.

To run the JVM against a library you built by hand, skip packaging entirely:

```bash
./gradlew :core-bridge:test -Dtemporal.native.libraryPath=$PWD/rust/kt-bridge/target/release/libkt_bridge.dylib
```

## Updating SDK-Core

Bump the version in `rust/kt-bridge/Cargo.toml`, run `cargo update -p temporalio-sdk-core`, and
set `sdkCoreVersion` in `gradle.properties` so the published coordinate says which Core this
speaks to. Dependabot handles routine dependency bumps.

If Core's proto schema changed, refresh `:protos` as well — see `protos/PINNED.toml`.

## Regenerating the RPC table

`rust/kt-bridge/src/rpc.rs` is generated. Core's `RawGrpcCaller::call` is `pub(crate)`, so a
generic bytes-passthrough RPC is not reachable through the public API and each RPC needs its own
dispatch arm. `tools/generate_rpc_table.py` reads the `proxier!` blocks out of the
`temporalio-client` source and emits all 140 across WorkflowService, OperatorService and
TestService:

```bash
python3 rust/kt-bridge/tools/generate_rpc_table.py
```
