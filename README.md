# temporal-kt-bridge

The native seam between [temporal-kt](https://github.com/Snipesy/temporal-kt) and Temporal's Rust
[SDK-Core](https://github.com/temporalio/sdk-core), published as its own set of artifacts.

This repository exists so that the everyday temporal-kt build needs neither a Rust toolchain nor
`protoc`. Building the bridge means compiling a multi-minute Rust workspace and generating ~2,100
protobuf sources; both change far less often than the SDK above them, so they are released
separately and consumed as binaries.

| Artifact | Version scheme | Contents |
|---|---|---|
| `com.surrealdev.temporal:core-common` | `<temporal-kt version>` | Hand-written config vocabulary (`TlsConfig`, `WorkerConfig`, `SlotSupplier`, `TemporalCoreException`, ...) |
| `com.surrealdev.temporal:protos` | `<sdk-core version>-<temporal-kt version>` | Generated protobuf classes for the Temporal API and SDK-Core's `coresdk.*` messages |
| `com.surrealdev.temporal:core-bridge` | `<sdk-core version>-<temporal-kt version>` | Kotlin/FFM bindings onto SDK-Core |
| `com.surrealdev.temporal:core-bridge:<classifier>` | same | The native library, one JAR per platform |

### Why two version components

`core-bridge` and `protos` are versioned `<sdkCoreVersion>-<version>` — for example `0.6.0-0.1.11`
— because their content is determined by an SDK-Core release as much as by temporal-kt: `protos`
*is* that release's schema, and `core-bridge` is the binding to its API. The coordinate tells you
which Core an artifact speaks to without opening it.

`core-common` is not composite-versioned: it is hand-written Kotlin with no generated content and
no direct SDK-Core coupling.

Consumers should not write these versions by hand. Import `com.surrealdev.temporal:bom` from the
temporal-kt repository, or apply the `com.surrealdev.temporal` Gradle plugin, and omit them.

## Supported platforms

`linux-x86_64-gnu`, `linux-aarch64-gnu`, `linux-x86_64-musl`, `linux-aarch64-musl`,
`macos-aarch64`, `windows-x86_64`. The Linux libraries are cross-compiled with zig; the gnu ones
link against glibc 2.17 (RHEL 7 and newer), the musl ones are for Alpine. CI fails if a gnu
library needs anything newer than that floor.

## Building

Requires JDK 25, a Rust toolchain meeting `core-bridge/rust/kt-bridge/rust-toolchain.toml`
(currently 1.94; note that rustup will not pick that file up from the outer workspace directory,
so your default toolchain must satisfy it), and `protoc` is supplied by Gradle.

```bash
git clone https://github.com/SurrealDevelopment/temporal-kt-bridge.git
cd temporal-kt-bridge
./gradlew build
```

SDK-Core comes from crates.io -- no submodule, no fork. See `core-bridge/README.md` for
the procedure for moving to a newer upstream.

## Working on the bridge together with temporal-kt

The common case is changing Rust only. Build here, then point temporal-kt's tests at the result —
no Gradle wiring, no JAR packaging:

```bash
cd temporal-kt-bridge && cargo build --release --manifest-path core-bridge/rust/Cargo.toml
cd ../temporal-kt && ./gradlew :core:test \
  -Ptemporal.nativeLib=$PWD/../temporal-kt-bridge/core-bridge/rust/target/release/libtemporalio_sdk_core_c_bridge.dylib
```

To change Kotlin on both sides at once, build temporal-kt against this repo as a composite build.
Coordinates match, so Gradle substitutes the projects automatically:

```bash
cd temporal-kt && ./gradlew build -Ptemporal.bridgePath=../temporal-kt-bridge
```

Note that classifier dependencies are not substitutable in a composite build, so the native
library still comes from the path override above.

## Relationship to temporal-kt

`bridgeAbi` in `gradle.properties` is a compatibility number for the
`com.surrealdev.temporal.core.*` seam that temporal-kt's `core` compiles against. It **must** stay
in step with `bridgeAbi` in the temporal-kt repository. `core` records the value it was built
against and refuses to start against a bridge reporting a different one, so a mismatched pin fails
with one clear message instead of a `NoSuchMethodError` deep in worker startup. Bump it only on a
breaking change to that seam.

## License

Apache 2.0. See [LICENSE](LICENSE).
