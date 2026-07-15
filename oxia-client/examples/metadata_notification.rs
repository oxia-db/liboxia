use log::info;
use oxia::client::Notification;
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

    // Subscribe to notifications
    let mut notification_rx = client.get_notifications().await.unwrap();
    info!("Subscribed to notifications, performing operations...");

    // Spawn a task to listen for notifications
    let listener = tokio::spawn(async move {
        let mut count = 0;
        while let Some(notification) = notification_rx.recv().await {
            match &notification {
                Notification::KeyCreated(created) => {
                    info!(
                        "Notification: KeyCreated key={:?} version_id={:?}",
                        created.key, created.version_id
                    );
                }
                Notification::KeyModified(modified) => {
                    info!(
                        "Notification: KeyModified key={:?} version_id={:?}",
                        modified.key, modified.version_id
                    );
                }
                Notification::KeyDeleted(deleted) => {
                    info!("Notification: KeyDeleted key={:?}", deleted.key);
                }
                Notification::KeyRangeDeleted(range_deleted) => {
                    info!(
                        "Notification: KeyRangeDeleted key={:?} key_range_last={:?}",
                        range_deleted.key, range_deleted.key_range_last
                    );
                }
                Notification::Unknown() => {
                    info!("Notification: Unknown type");
                }
            }
            count += 1;
            if count >= 5 {
                break;
            }
        }
        info!("Received {} notifications total", count);
    });

    // Give the notification listener time to connect
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Perform operations that will generate notifications
    // 1. Create a key
    client
        .put("notify/key1".to_string(), b"value1".to_vec())
        .await
        .unwrap();
    info!("Put notify/key1");

    // 2. Modify the key
    client
        .put("notify/key1".to_string(), b"value2".to_vec())
        .await
        .unwrap();
    info!("Modified notify/key1");

    // 3. Create another key
    client
        .put("notify/key2".to_string(), b"value3".to_vec())
        .await
        .unwrap();
    info!("Put notify/key2");

    // 4. Delete a key
    client.delete("notify/key1".to_string()).await.unwrap();
    info!("Deleted notify/key1");

    // 5. Delete range
    client
        .delete_range("notify/".to_string(), "notify/~".to_string())
        .await
        .unwrap();
    info!("Delete range notify/*");

    // Wait for listener to process notifications (with timeout)
    let _ = tokio::time::timeout(Duration::from_secs(5), listener).await;

    client.shutdown().await.unwrap();
}
