use log::info;
use oxia::client::{ListOption, PutOption, RangeScanOption};
use oxia::client_builder::OxiaClientBuilder;
use oxia::oxia::SecondaryIndex;
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

    // Put records with secondary indexes
    let put_result = client
        .put_with_options(
            "user/1".to_string(),
            b"alice".to_vec(),
            vec![PutOption::SecondaryIndexes(vec![SecondaryIndex {
                index_name: "by-name".to_string(),
                secondary_key: "alice".to_string(),
            }])],
        )
        .await
        .unwrap();
    info!(
        "Put user/1 with secondary index. version={:?}",
        put_result.version
    );

    let put_result = client
        .put_with_options(
            "user/2".to_string(),
            b"bob".to_vec(),
            vec![PutOption::SecondaryIndexes(vec![SecondaryIndex {
                index_name: "by-name".to_string(),
                secondary_key: "bob".to_string(),
            }])],
        )
        .await
        .unwrap();
    info!(
        "Put user/2 with secondary index. version={:?}",
        put_result.version
    );

    let put_result = client
        .put_with_options(
            "user/3".to_string(),
            b"charlie".to_vec(),
            vec![PutOption::SecondaryIndexes(vec![SecondaryIndex {
                index_name: "by-name".to_string(),
                secondary_key: "charlie".to_string(),
            }])],
        )
        .await
        .unwrap();
    info!(
        "Put user/3 with secondary index. version={:?}",
        put_result.version
    );

    // List keys using the secondary index
    let list_result = client
        .list_with_options(
            "a".to_string(),
            "z".to_string(),
            vec![ListOption::UseIndex("by-name".to_string())],
        )
        .await
        .unwrap();
    info!(
        "List keys via secondary index 'by-name': {:?}",
        list_result.keys
    );

    // Range scan using the secondary index
    let range_scan_result = client
        .range_scan_with_options(
            "a".to_string(),
            "z".to_string(),
            vec![RangeScanOption::UseIndex("by-name".to_string())],
        )
        .await
        .unwrap();
    info!(
        "Range scan via secondary index 'by-name': {} records",
        range_scan_result.records.len()
    );
    for record in &range_scan_result.records {
        info!(
            "  key={:?} value={:?}",
            record.key,
            record.value.as_ref().map(|v| String::from_utf8_lossy(v))
        );
    }

    // Cleanup
    client
        .delete_range("user/".to_string(), "user/~".to_string())
        .await
        .unwrap();
    info!("Cleaned up all user records.");

    client.shutdown().await.unwrap();
}
