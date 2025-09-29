use liboxia::client::PutOption;
use liboxia::client::{Client, GetOptions};
use liboxia::client_builder::OxiaClientBuilder;
use log::info;
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

    let mut client = OxiaClientBuilder::new().build().await.unwrap();
    let key = String::from("key1");
    let payload = "payload".to_string().into_bytes();

    // put ephemeral record
    let put_result = client
        .put(key.clone(), payload.clone(), vec![PutOption::Ephemeral()])
        .await
        .unwrap();
    info!(
        "put the ephemeral record. key {:?} value {:?} version {:?}",
        put_result.key, payload, put_result.version
    );
    // close the client
    client.shutdown().await.unwrap();

    // wait for the session expired
    tokio::time::sleep(std::time::Duration::from_secs(16)).await;

    // create a new client
    client = OxiaClientBuilder::new().build().await.unwrap();
    let result = client.get(key.clone(), GetOptions {}).await;
    info!("get the value again. error: {:?}", result.unwrap_err());

    client.shutdown().await.unwrap();
}
