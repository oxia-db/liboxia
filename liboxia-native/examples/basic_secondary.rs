use liboxia::client::Client;
use liboxia::client_builder::OxiaClientBuilder;
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

    let key = "key".to_string();
    let payload = "value1".to_string().into_bytes();
    client.put(key.clone(), payload.clone()).await.unwrap();

    client.shutdown().await.unwrap();
}
