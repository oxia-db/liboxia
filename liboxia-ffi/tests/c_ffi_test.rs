use std::process::Command;
use testcontainers::core::ports::ContainerPort;
use testcontainers::core::wait::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

const OXIA_PORT: u16 = 6648;
const DEFAULT_OXIA_IMAGE: &str = "oxia/oxia";
const DEFAULT_OXIA_TAG: &str = "main";

#[tokio::test]
async fn test_c_ffi_integration() {
    let image = std::env::var("OXIA_IMAGE").unwrap_or_else(|_| DEFAULT_OXIA_IMAGE.to_string());
    let tag = std::env::var("OXIA_TAG").unwrap_or_else(|_| DEFAULT_OXIA_TAG.to_string());

    // Start Oxia container
    let container = GenericImage::new(image, tag)
        .with_exposed_port(ContainerPort::Tcp(OXIA_PORT))
        .with_wait_for(WaitFor::message_on_stdout("Started Grpc server"))
        .with_cmd(vec!["oxia", "standalone"])
        .start()
        .await
        .expect("Failed to start Oxia container");

    let host_port = container
        .get_host_port_ipv4(OXIA_PORT)
        .await
        .expect("Failed to get host port");

    let address = format!("http://127.0.0.1:{}", host_port);

    // Find the library output directory
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let workspace_dir = std::path::Path::new(manifest_dir)
        .parent()
        .expect("Failed to find workspace dir");

    // Build the FFI library first (should already be built by cargo test)
    let target_dir = workspace_dir.join("target").join("debug");

    let c_test_src = std::path::Path::new(manifest_dir)
        .join("tests")
        .join("c")
        .join("test_ffi.c");
    let c_test_bin = target_dir.join("test_ffi_c");

    // Compile the C test
    let compile = Command::new("cc")
        .args([
            c_test_src.to_str().unwrap(),
            "-o",
            c_test_bin.to_str().unwrap(),
            &format!("-L{}", target_dir.display()),
            "-lliboxia_ffi",
            &format!("-Wl,-rpath,{}", target_dir.display()),
            "-Wall",
            "-Wextra",
        ])
        .output()
        .expect("Failed to run cc compiler");

    assert!(
        compile.status.success(),
        "C test compilation failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&compile.stdout),
        String::from_utf8_lossy(&compile.stderr)
    );

    // Run the C test
    let run = Command::new(c_test_bin.to_str().unwrap())
        .env("OXIA_ADDRESS", &address)
        .env("LD_LIBRARY_PATH", target_dir.to_str().unwrap())
        .env("DYLD_LIBRARY_PATH", target_dir.to_str().unwrap())
        .output()
        .expect("Failed to run C test binary");

    let stdout = String::from_utf8_lossy(&run.stdout);
    let stderr = String::from_utf8_lossy(&run.stderr);

    println!("C test stdout:\n{}", stdout);
    if !stderr.is_empty() {
        eprintln!("C test stderr:\n{}", stderr);
    }

    assert!(
        run.status.success(),
        "C FFI test failed with exit code {:?}\nstdout: {}\nstderr: {}",
        run.status.code(),
        stdout,
        stderr
    );
}
