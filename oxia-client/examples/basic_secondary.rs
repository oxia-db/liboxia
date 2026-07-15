use oxia::OxiaClient;
use tracing::info;
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

    let client = OxiaClient::connect("localhost:6648").await.unwrap();

    // Put records, each also indexed by name in the "by-name" secondary index.
    for (key, name) in [("user/1", "alice"), ("user/2", "bob"), ("user/3", "carol")] {
        let put_result = client
            .put(key, name)
            .secondary_index("by-name", name)
            .await
            .unwrap();
        info!(
            "Put {key} with secondary index by-name={name}. version={:?}",
            put_result.version
        );
    }

    // Query the secondary index with get.
    let result = client.get("bob").use_index("by-name").await.unwrap();
    info!(
        "get('bob') via by-name => key={:?} value={:?}",
        result.key,
        result.value.as_ref().map(|v| String::from_utf8_lossy(v))
    );

    // List keys through the secondary index.
    let keys = client.list("a", "z").use_index("by-name").await.unwrap();
    info!("list('a'..'z') via by-name => {:?}", keys);

    // Range-scan through the secondary index.
    let records = client
        .range_scan("a", "z")
        .use_index("by-name")
        .await
        .unwrap();
    for record in &records {
        info!(
            "scan via by-name => key={:?} value={:?}",
            record.key,
            record.value.as_ref().map(|v| String::from_utf8_lossy(v))
        );
    }

    // Cleanup
    client.delete_range("user/", "user/~").await.unwrap();
    info!("Cleaned up all user/* records.");

    client.close().await.unwrap();
}
