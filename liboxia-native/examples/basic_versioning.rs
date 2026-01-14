use liboxia::client::PutOption;
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
    let key = String::from("key");
    let payload = "payload".to_string().into_bytes();
    let put_response = client.put(key.clone(), payload.clone()).await.unwrap();
    println!(
        "put the value. key {:?} version {:?}",
        put_response.key, put_response.version
    );
    let failed_put_response = client
        .put_with_options(
            key.clone(),
            payload.clone(),
            vec![PutOption::ExpectVersionId(-1)],
        )
        .await;
    println!("put with error: {:?}", failed_put_response.unwrap_err());

    let put_response = client
        .put_with_options(
            key.clone(),
            payload.clone(),
            vec![PutOption::ExpectVersionId(put_response.version.version_id)],
        )
        .await
        .unwrap();
    println!(
        "put the value. key {:?} version {:?}",
        put_response.key, put_response.version
    );

    client.shutdown().await.unwrap();
}
