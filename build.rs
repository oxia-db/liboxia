fn main() {
    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .compile_protos(&["proto/client.proto"], &["proto"])
        .unwrap();
}
