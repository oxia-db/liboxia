fn main() {
    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        // Generate `bytes::Bytes` for all proto `bytes` fields (record values):
        // decoding borrows the receive buffer and cloning is refcounted, so
        // values move through the client without byte copies.
        .bytes(["."])
        .compile_protos(&["proto/client.proto"], &["proto"])
        .unwrap();
}
