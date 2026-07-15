use log::info;
use oxia::client::{GetSequenceUpdatesOption, PutOption};
use oxia::client_builder::OxiaClientBuilder;
use std::time::Duration;
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

    let partition_key = "my-partition".to_string();
    let base_key = "seq/events".to_string();

    // Subscribe to sequence updates for the key with partition key option
    let mut sequence_rx = client
        .get_sequence_updates_with_options(
            base_key.clone(),
            vec![
                GetSequenceUpdatesOption::PartitionKey(partition_key.clone()),
                GetSequenceUpdatesOption::BufferSize(50),
            ],
        )
        .await
        .unwrap();
    info!("Subscribed to sequence updates for key '{}'", base_key);

    // Spawn a task to listen for sequence updates
    let listener = tokio::spawn(async move {
        let mut count = 0;
        while let Some(highest_key) = sequence_rx.recv().await {
            info!("Sequence update: highest_sequence_key={:?}", highest_key);
            count += 1;
            if count >= 5 {
                break;
            }
        }
        info!("Received {} sequence updates", count);
    });

    // Give the listener time to connect
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Put records with sequence key deltas - the server will assign sequential suffixes
    for i in 1..=5 {
        let result = client
            .put_with_options(
                base_key.clone(),
                format!("event-{}", i).into_bytes(),
                vec![
                    PutOption::PartitionKey(partition_key.clone()),
                    PutOption::SequenceKeyDelta(vec![1]),
                ],
            )
            .await
            .unwrap();
        info!("Put sequence event {}: generated key={:?}", i, result.key);
    }

    // Wait for listener to process updates (with timeout)
    let _ = tokio::time::timeout(Duration::from_secs(5), listener).await;

    // Cleanup
    client
        .delete_range_with_options(
            "seq/".to_string(),
            "seq/~".to_string(),
            vec![oxia::client::DeleteRangeOption::PartitionKey(partition_key)],
        )
        .await
        .unwrap();
    info!("Cleaned up sequence keys.");

    client.shutdown().await.unwrap();
}
