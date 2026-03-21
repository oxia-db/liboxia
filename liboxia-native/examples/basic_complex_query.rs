use liboxia::client::{GetOption, PutOption};
use liboxia::client_builder::OxiaClientBuilder;
use liboxia::oxia::KeyComparisonType;
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

    // Put some records with keys we can query
    let keys = vec!["key/a", "key/c", "key/e", "key/g", "key/i"];
    for key in &keys {
        client
            .put(key.to_string(), format!("value-{}", key).into_bytes())
            .await
            .unwrap();
        info!("Put {:?}", key);
    }

    // EQUAL comparison (default) - exact match
    let result = client.get("key/c".to_string()).await.unwrap();
    info!(
        "EQUAL  get('key/c') => key={:?} value={:?}",
        result.key,
        result.value.as_ref().map(|v| String::from_utf8_lossy(v))
    );

    // FLOOR - highest key <= requested key
    let result = client
        .get_with_options(
            "key/d".to_string(),
            vec![GetOption::ComparisonType(KeyComparisonType::Floor)],
        )
        .await
        .unwrap();
    info!(
        "FLOOR  get('key/d') => key={:?} (highest key <= 'key/d')",
        result.key
    );

    // CEILING - lowest key >= requested key
    let result = client
        .get_with_options(
            "key/d".to_string(),
            vec![GetOption::ComparisonType(KeyComparisonType::Ceiling)],
        )
        .await
        .unwrap();
    info!(
        "CEILING get('key/d') => key={:?} (lowest key >= 'key/d')",
        result.key
    );

    // LOWER - highest key strictly < requested key
    let result = client
        .get_with_options(
            "key/e".to_string(),
            vec![GetOption::ComparisonType(KeyComparisonType::Lower)],
        )
        .await
        .unwrap();
    info!(
        "LOWER  get('key/e') => key={:?} (highest key < 'key/e')",
        result.key
    );

    // HIGHER - lowest key strictly > requested key
    let result = client
        .get_with_options(
            "key/e".to_string(),
            vec![GetOption::ComparisonType(KeyComparisonType::Higher)],
        )
        .await
        .unwrap();
    info!(
        "HIGHER get('key/e') => key={:?} (lowest key > 'key/e')",
        result.key
    );

    // Conditional put: only create if record does NOT exist
    let result = client
        .put_with_options(
            "key/new".to_string(),
            b"new-value".to_vec(),
            vec![PutOption::ExpectedRecordNotExists()],
        )
        .await;
    info!(
        "Create-only put for 'key/new' (should succeed): {:?}",
        result.is_ok()
    );

    // Try again - should fail because it already exists
    let result = client
        .put_with_options(
            "key/new".to_string(),
            b"new-value-2".to_vec(),
            vec![PutOption::ExpectedRecordNotExists()],
        )
        .await;
    info!(
        "Create-only put for 'key/new' (should fail): {:?}",
        result.unwrap_err()
    );

    // Cleanup
    client
        .delete_range("key/".to_string(), "key/~".to_string())
        .await
        .unwrap();
    info!("Cleaned up all key/* records.");

    client.shutdown().await.unwrap();
}
