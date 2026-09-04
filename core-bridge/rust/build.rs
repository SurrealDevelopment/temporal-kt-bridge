fn main() {
    println!("cargo:rerun-if-changed=proto/kt_bridge.proto");
    prost_build::compile_protos(&["proto/kt_bridge.proto"], &["proto"])
        .expect("failed to compile kt_bridge.proto");
}
