use liboxia::client::PutOption;
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

    let client = OxiaClientBuilder::new().build().await.unwrap();
    let key = String::from("key1");
    let payload = "payload".to_string().into_bytes();

    // put ephemeral record
    let put_result = client
        .put_with_options(key.clone(), payload.clone(), vec![PutOption::Ephemeral()])
        .await
        .unwrap();
    info!(
        "put the ephemeral record. key {:?} value {:?} version {:?}",
        put_result.key, payload, put_result.version
    );
    // close the client (this closes the session, which should delete ephemeral records)
    client.shutdown().await.unwrap();

    // create a new client and verify the ephemeral record is gone
    let client2 = OxiaClientBuilder::new().build().await.unwrap();
    let result = client2.get(key.clone()).await;
    info!("get the value again. error: {:?}", result.unwrap_err());

    client2.shutdown().await.unwrap();
}
