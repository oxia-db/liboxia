use log::info;
use oxia_client_rust::client::PutOptions;
use oxia_client_rust::client::{Client, GetOptions};
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
    let key = String::from("key");
    let payload = "payload".to_string().into_bytes();
    let put_result = client
        .put(key.clone(), payload.clone(), PutOptions {})
        .await
        .unwrap();
    info!(
        "put the value. key {:?} value {:?} version {:?}",
        put_result.key, payload, put_result.version
    );
    let get_result = client.get(key.clone(), GetOptions {}).await.unwrap();
    info!(
        "get the value. key {:?} value {:?} version {:?}",
        get_result.key, get_result.value, get_result.version
    );
    client.shutdown().await.unwrap();
}
