use oxia::{ComparisonType, OxiaClient};
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

    // Put some records with keys we can query
    for key in ["key/a", "key/c", "key/e", "key/g", "key/i"] {
        client.put(key, format!("value-{key}")).await.unwrap();
        info!("Put {:?}", key);
    }

    // EQUAL comparison (default) - exact match
    let result = client.get("key/c").await.unwrap();
    info!(
        "EQUAL  get('key/c') => key={:?} value={:?}",
        result.key,
        result.value.as_ref().map(|v| String::from_utf8_lossy(v))
    );

    // FLOOR - highest key <= requested key
    let result = client
        .get("key/d")
        .comparison(ComparisonType::Floor)
        .await
        .unwrap();
    info!(
        "FLOOR  get('key/d') => key={:?} (highest key <= 'key/d')",
        result.key
    );

    // CEILING - lowest key >= requested key
    let result = client
        .get("key/d")
        .comparison(ComparisonType::Ceiling)
        .await
        .unwrap();
    info!(
        "CEILING get('key/d') => key={:?} (lowest key >= 'key/d')",
        result.key
    );

    // LOWER - highest key strictly < requested key
    let result = client
        .get("key/e")
        .comparison(ComparisonType::Lower)
        .await
        .unwrap();
    info!(
        "LOWER  get('key/e') => key={:?} (highest key < 'key/e')",
        result.key
    );

    // HIGHER - lowest key strictly > requested key
    let result = client
        .get("key/e")
        .comparison(ComparisonType::Higher)
        .await
        .unwrap();
    info!(
        "HIGHER get('key/e') => key={:?} (lowest key > 'key/e')",
        result.key
    );

    // Conditional put: only create if record does NOT exist
    let result = client
        .put("key/new", "new-value")
        .expected_record_not_exists()
        .await;
    info!(
        "Create-only put for 'key/new' (should succeed): {:?}",
        result.is_ok()
    );

    // Try again - should fail because it already exists
    let result = client
        .put("key/new", "new-value-2")
        .expected_record_not_exists()
        .await;
    info!(
        "Create-only put for 'key/new' (should fail): {:?}",
        result.unwrap_err()
    );

    // Cleanup
    client.delete_range("key/", "key/~").await.unwrap();
    info!("Cleaned up all key/* records.");

    client.close().await.unwrap();
}
