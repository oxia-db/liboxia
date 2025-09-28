use oxia_client_rust::client::Client;
use oxia_client_rust::client::PutOptions;
use oxia_client_rust::client_builder::OxiaClientBuilder;
use tracing::level_filters::LevelFilter;
#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with_target(false)
        .init();

    let client = OxiaClientBuilder::new().build().await.unwrap();
    let result = client
        .put(String::from("key"), Vec::new(), PutOptions {})
        .await
        .unwrap();

    client.shutdown().await.unwrap();
}
