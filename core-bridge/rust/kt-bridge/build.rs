fn main() {
    println!("cargo:rerun-if-changed=proto/kt_bridge.proto");
    println!("cargo:rerun-if-changed=Cargo.lock");
    let lock = std::fs::read_to_string("Cargo.lock").expect("Cargo.lock");
    let version = lock
        .split("[[package]]")
        .find(|package| {
            package
                .lines()
                .any(|line| line == "name = \"temporalio-sdk-core\"")
        })
        .and_then(|package| {
            package
                .lines()
                .find_map(|line| line.strip_prefix("version = \""))
        })
        .and_then(|version| version.strip_suffix('"'))
        .expect("temporalio-sdk-core version in Cargo.lock");
    println!("cargo:rustc-env=TEMPORAL_SDK_CORE_VERSION={version}");
    prost_build::compile_protos(&["proto/kt_bridge.proto"], &["proto"])
        .expect("failed to compile kt_bridge.proto");
}
