use oxia::{ComparisonType, Notification, OxiaClient, OxiaClientBuilder, OxiaError};
use std::time::Duration;
use testcontainers::core::ports::ContainerPort;
use testcontainers::core::wait::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

const OXIA_PORT: u16 = 6648;
const DEFAULT_OXIA_IMAGE: &str = "oxia/oxia";
const DEFAULT_OXIA_TAG: &str = "main";

async fn start_oxia() -> (ContainerAsync<GenericImage>, String) {
    let image = std::env::var("OXIA_IMAGE").unwrap_or_else(|_| DEFAULT_OXIA_IMAGE.to_string());
    let tag = std::env::var("OXIA_TAG").unwrap_or_else(|_| DEFAULT_OXIA_TAG.to_string());

    let container = GenericImage::new(image, tag)
        .with_exposed_port(ContainerPort::Tcp(OXIA_PORT))
        .with_wait_for(WaitFor::message_on_stdout("Started Grpc server"))
        .with_cmd(vec!["oxia", "standalone"])
        .start()
        .await
        .expect("Failed to start Oxia container");

    let host_port = container
        .get_host_port_ipv4(OXIA_PORT)
        .await
        .expect("Failed to get host port");

    let address = format!("http://127.0.0.1:{}", host_port);
    (container, address)
}

async fn new_client(address: &str) -> OxiaClient {
    OxiaClientBuilder::new()
        .service_address(address.to_string())
        .request_timeout(Duration::from_secs(10))
        .build()
        .await
        .unwrap()
}

// ============================================================
// Basic CRUD Tests
// ============================================================

#[tokio::test]
async fn test_put_and_get() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    let put_result = client
        .put("test/key1".to_string(), b"hello world".to_vec())
        .await
        .unwrap();

    assert_eq!(put_result.key, "test/key1");
    assert!(put_result.version.version_id >= 0);

    let get_result = client.get("test/key1".to_string()).await.unwrap();
    assert_eq!(get_result.key, "test/key1");
    assert_eq!(
        get_result.value.as_deref(),
        Some(b"hello world".to_vec()).as_deref()
    );
    assert_eq!(get_result.version.version_id, put_result.version.version_id);

    client.close().await.unwrap();
}

#[tokio::test]
async fn test_put_overwrite() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    let r1 = client
        .put("test/overwrite".to_string(), b"v1".to_vec())
        .await
        .unwrap();
    let r2 = client
        .put("test/overwrite".to_string(), b"v2".to_vec())
        .await
        .unwrap();

    assert!(r2.version.version_id > r1.version.version_id);

    let get = client.get("test/overwrite".to_string()).await.unwrap();
    assert_eq!(get.value.as_deref(), Some(b"v2".to_vec()).as_deref());

    client.close().await.unwrap();
}

#[tokio::test]
async fn test_delete() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    client
        .put("test/del".to_string(), b"val".to_vec())
        .await
        .unwrap();
    client.delete("test/del".to_string()).await.unwrap();

    let result = client.get("test/del".to_string()).await;
    assert!(matches!(result, Err(OxiaError::KeyNotFound)));

    client.close().await.unwrap();
}

#[tokio::test]
async fn test_get_nonexistent() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    let result = client.get("nonexistent/key".to_string()).await;
    assert!(matches!(result, Err(OxiaError::KeyNotFound)));

    client.close().await.unwrap();
}

#[tokio::test]
async fn test_delete_range() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    for i in 0..5 {
        client
            .put(format!("range/{}", i), format!("val-{}", i).into_bytes())
            .await
            .unwrap();
    }

    let list_before = client
        .list("range/".to_string(), "range/~".to_string())
        .await
        .unwrap();
    assert_eq!(list_before.len(), 5);

    client
        .delete_range("range/".to_string(), "range/~".to_string())
        .await
        .unwrap();

    let list_after = client
        .list("range/".to_string(), "range/~".to_string())
        .await
        .unwrap();
    assert_eq!(list_after.len(), 0);

    client.close().await.unwrap();
}

// ============================================================
// List and RangeScan Tests
// ============================================================

#[tokio::test]
async fn test_list() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    for key in &["list/a", "list/b", "list/c"] {
        client.put(key.to_string(), b"v".to_vec()).await.unwrap();
    }

    let result = client
        .list("list/".to_string(), "list/~".to_string())
        .await
        .unwrap();

    assert_eq!(result.len(), 3);
    assert!(result.contains(&"list/a".to_string()));
    assert!(result.contains(&"list/b".to_string()));
    assert!(result.contains(&"list/c".to_string()));

    // Cleanup
    client
        .delete_range("list/".to_string(), "list/~".to_string())
        .await
        .unwrap();
    client.close().await.unwrap();
}

#[tokio::test]
async fn test_range_scan() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    for i in 0..3 {
        client
            .put(format!("scan/{}", i), format!("value-{}", i).into_bytes())
            .await
            .unwrap();
    }

    let result = client
        .range_scan("scan/".to_string(), "scan/~".to_string())
        .await
        .unwrap();

    assert_eq!(result.len(), 3);
    for record in &result {
        assert!(record.value.is_some());
        assert!(record.key.starts_with("scan/"));
    }

    // Cleanup
    client
        .delete_range("scan/".to_string(), "scan/~".to_string())
        .await
        .unwrap();
    client.close().await.unwrap();
}

#[tokio::test]
async fn test_list_empty_range() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    let result = client
        .list("empty/".to_string(), "empty/~".to_string())
        .await
        .unwrap();
    assert_eq!(result.len(), 0);

    client.close().await.unwrap();
}

// ============================================================
// Versioning / Conditional Operations Tests
// ============================================================

#[tokio::test]
async fn test_expected_version_id() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    let r1 = client
        .put("ver/key".to_string(), b"v1".to_vec())
        .await
        .unwrap();

    // Update with correct version should succeed
    let r2 = client
        .put("ver/key".to_string(), b"v2".to_vec())
        .expected_version_id(r1.version.version_id)
        .await
        .unwrap();
    assert!(r2.version.version_id > r1.version.version_id);

    // Update with stale version should fail
    let err = client
        .put("ver/key".to_string(), b"v3".to_vec())
        .expected_version_id(r1.version.version_id)
        .await;
    assert!(matches!(err, Err(OxiaError::UnexpectedVersionId)));

    client
        .delete_range("ver/".to_string(), "ver/~".to_string())
        .await
        .unwrap();
    client.close().await.unwrap();
}

#[tokio::test]
async fn test_expected_record_not_exists() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    // First put should succeed
    client
        .put("new/key".to_string(), b"v1".to_vec())
        .expected_record_not_exists()
        .await
        .unwrap();

    // Second put should fail - record already exists
    let err = client
        .put("new/key".to_string(), b"v2".to_vec())
        .expected_record_not_exists()
        .await;
    assert!(matches!(err, Err(OxiaError::UnexpectedVersionId)));

    client
        .delete_range("new/".to_string(), "new/~".to_string())
        .await
        .unwrap();
    client.close().await.unwrap();
}

#[tokio::test]
async fn test_delete_with_expected_version() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    let put_result = client
        .put("delver/key".to_string(), b"value".to_vec())
        .await
        .unwrap();

    // Delete with wrong version should fail
    let err = client
        .delete("delver/key".to_string())
        .expected_version_id(999)
        .await;
    assert!(matches!(err, Err(OxiaError::UnexpectedVersionId)));

    // Delete with correct version should succeed
    client
        .delete("delver/key".to_string())
        .expected_version_id(put_result.version.version_id)
        .await
        .unwrap();

    let get = client.get("delver/key".to_string()).await;
    assert!(matches!(get, Err(OxiaError::KeyNotFound)));

    client.close().await.unwrap();
}

// ============================================================
// Comparison Query Tests (Floor, Ceiling, Lower, Higher)
// ============================================================

#[tokio::test]
async fn test_get_floor() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    for key in &["cmp/a", "cmp/c", "cmp/e"] {
        client.put(key.to_string(), b"v".to_vec()).await.unwrap();
    }

    // Floor of "cmp/d" should be "cmp/c" (highest key <= "cmp/d")
    let result = client
        .get("cmp/d".to_string())
        .comparison(ComparisonType::Floor)
        .await
        .unwrap();
    assert_eq!(result.key, "cmp/c");

    client
        .delete_range("cmp/".to_string(), "cmp/~".to_string())
        .await
        .unwrap();
    client.close().await.unwrap();
}

#[tokio::test]
async fn test_get_ceiling() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    for key in &["cmp2/a", "cmp2/c", "cmp2/e"] {
        client.put(key.to_string(), b"v".to_vec()).await.unwrap();
    }

    // Ceiling of "cmp2/b" should be "cmp2/c" (lowest key >= "cmp2/b")
    let result = client
        .get("cmp2/b".to_string())
        .comparison(ComparisonType::Ceiling)
        .await
        .unwrap();
    assert_eq!(result.key, "cmp2/c");

    client
        .delete_range("cmp2/".to_string(), "cmp2/~".to_string())
        .await
        .unwrap();
    client.close().await.unwrap();
}

#[tokio::test]
async fn test_get_lower() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    for key in &["cmp3/a", "cmp3/c", "cmp3/e"] {
        client.put(key.to_string(), b"v".to_vec()).await.unwrap();
    }

    // Lower of "cmp3/c" should be "cmp3/a" (highest key < "cmp3/c")
    let result = client
        .get("cmp3/c".to_string())
        .comparison(ComparisonType::Lower)
        .await
        .unwrap();
    assert_eq!(result.key, "cmp3/a");

    client
        .delete_range("cmp3/".to_string(), "cmp3/~".to_string())
        .await
        .unwrap();
    client.close().await.unwrap();
}

#[tokio::test]
async fn test_get_higher() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    for key in &["cmp4/a", "cmp4/c", "cmp4/e"] {
        client.put(key.to_string(), b"v".to_vec()).await.unwrap();
    }

    // Higher of "cmp4/c" should be "cmp4/e" (lowest key > "cmp4/c")
    let result = client
        .get("cmp4/c".to_string())
        .comparison(ComparisonType::Higher)
        .await
        .unwrap();
    assert_eq!(result.key, "cmp4/e");

    client
        .delete_range("cmp4/".to_string(), "cmp4/~".to_string())
        .await
        .unwrap();
    client.close().await.unwrap();
}

// ============================================================
// Ephemeral Key Tests
// ============================================================

#[tokio::test]
async fn test_ephemeral_keys() {
    let (_container, address) = start_oxia().await;

    // Create client and put ephemeral key
    let client1 = new_client(&address).await;
    client1
        .put("eph/key1".to_string(), b"ephemeral-value".to_vec())
        .ephemeral()
        .await
        .unwrap();

    // Verify it exists
    let get_result = client1.get("eph/key1".to_string()).await.unwrap();
    assert_eq!(
        get_result.value.as_deref(),
        Some(b"ephemeral-value".to_vec()).as_deref()
    );

    // Shutdown client (which closes session, deleting ephemeral keys)
    client1.close().await.unwrap();

    // Wait for session to be cleaned up (retry with backoff)
    let client2 = new_client(&address).await;
    let mut found_deleted = false;
    for attempt in 0..10 {
        tokio::time::sleep(Duration::from_millis(500 * (attempt + 1))).await;
        let result = client2.get("eph/key1".to_string()).await;
        if matches!(result, Err(OxiaError::KeyNotFound)) {
            found_deleted = true;
            break;
        }
    }
    assert!(
        found_deleted,
        "Expected ephemeral key to be deleted after client shutdown"
    );

    client2.close().await.unwrap();
}

// ============================================================
// Multiple Records / Batch Tests
// ============================================================

#[tokio::test]
async fn test_multiple_puts_batch() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    // Put multiple records rapidly to exercise batching
    let mut handles = Vec::new();
    for i in 0..20 {
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            c.put(format!("batch/{:03}", i), format!("val-{}", i).into_bytes())
                .await
        }));
    }

    for handle in handles {
        handle.await.unwrap().unwrap();
    }

    let list = client
        .list("batch/".to_string(), "batch/~".to_string())
        .await
        .unwrap();
    assert_eq!(list.len(), 20);

    // Cleanup
    client
        .delete_range("batch/".to_string(), "batch/~".to_string())
        .await
        .unwrap();
    client.close().await.unwrap();
}

// ============================================================
// Notification Tests
// ============================================================

#[tokio::test]
async fn test_notifications() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    let mut notification_rx = client.notifications().await.unwrap();

    // Give notification listener time to connect
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create a key
    client
        .put("notif/key1".to_string(), b"v1".to_vec())
        .await
        .unwrap();

    // Modify the key
    client
        .put("notif/key1".to_string(), b"v2".to_vec())
        .await
        .unwrap();

    // Delete the key
    client.delete("notif/key1".to_string()).await.unwrap();

    // Collect notifications with timeout
    let mut notifications = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        tokio::select! {
            Some(n) = notification_rx.recv() => {
                notifications.push(n);
                if notifications.len() >= 3 {
                    break;
                }
            }
            _ = tokio::time::sleep_until(deadline) => {
                break;
            }
        }
    }

    assert!(
        notifications.len() >= 3,
        "Expected at least 3 notifications, got {}",
        notifications.len()
    );

    // First notification should be KeyCreated
    assert!(matches!(
        &notifications[0],
        Notification::KeyCreated { key, .. } if key == "notif/key1"
    ));

    // Second should be KeyModified
    assert!(matches!(
        &notifications[1],
        Notification::KeyModified { key, .. } if key == "notif/key1"
    ));

    // Third should be KeyDeleted
    assert!(matches!(
        &notifications[2],
        Notification::KeyDeleted { key } if key == "notif/key1"
    ));

    client.close().await.unwrap();
}

// ============================================================
// Partition Key Tests
// ============================================================

#[tokio::test]
async fn test_partition_key() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    // Put with partition key
    let result = client
        .put("pk/key1".to_string(), b"value1".to_vec())
        .partition_key("my-partition".to_string())
        .await
        .unwrap();
    assert_eq!(result.key, "pk/key1");

    // Get with partition key
    let get = client
        .get("pk/key1".to_string())
        .partition_key("my-partition".to_string())
        .await
        .unwrap();
    assert_eq!(get.value.as_deref(), Some(b"value1".to_vec()).as_deref());

    // List with partition key
    let list = client
        .list("pk/".to_string(), "pk/~".to_string())
        .partition_key("my-partition".to_string())
        .await
        .unwrap();
    assert!(list.contains(&"pk/key1".to_string()));

    // Delete with partition key
    client
        .delete("pk/key1".to_string())
        .partition_key("my-partition".to_string())
        .await
        .unwrap();

    client.close().await.unwrap();
}

// ============================================================
// Sequence Key Tests
// ============================================================

#[tokio::test]
async fn test_sequence_keys() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    let partition_key = "seq-part".to_string();

    // Put records with sequence key deltas
    let r1 = client
        .put("seq/events".to_string(), b"event-1".to_vec())
        .partition_key(partition_key.clone())
        .sequence_key_deltas(vec![1])
        .await
        .unwrap();

    let r2 = client
        .put("seq/events".to_string(), b"event-2".to_vec())
        .partition_key(partition_key.clone())
        .sequence_key_deltas(vec![1])
        .await
        .unwrap();

    // Server should have generated sequential keys
    assert_ne!(r1.key, r2.key, "Sequence keys should be different");

    // Cleanup
    client
        .delete_range("seq/".to_string(), "seq/~".to_string())
        .partition_key(partition_key)
        .await
        .unwrap();

    client.close().await.unwrap();
}

// ============================================================
// Secondary Index Tests
// ============================================================

#[tokio::test]
async fn test_secondary_index() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    // Put records with secondary indexes
    client
        .put("idx/user1".to_string(), b"alice".to_vec())
        .secondary_index("by-name".to_string(), "alice".to_string())
        .await
        .unwrap();

    client
        .put("idx/user2".to_string(), b"bob".to_vec())
        .secondary_index("by-name".to_string(), "bob".to_string())
        .await
        .unwrap();

    // List using secondary index
    let list = client
        .list("a".to_string(), "z".to_string())
        .use_index("by-name".to_string())
        .await
        .unwrap();

    assert!(
        list.len() >= 2,
        "Expected at least 2 keys via secondary index, got {}",
        list.len()
    );

    // Range scan using secondary index
    let scan = client
        .range_scan("a".to_string(), "z".to_string())
        .use_index("by-name".to_string())
        .await
        .unwrap();

    assert!(
        scan.len() >= 2,
        "Expected at least 2 records via secondary index range scan, got {}",
        scan.len()
    );

    // Cleanup
    client
        .delete_range("idx/".to_string(), "idx/~".to_string())
        .await
        .unwrap();
    client.close().await.unwrap();
}

// ============================================================
// Client Builder Tests
// ============================================================

#[tokio::test]
async fn test_client_builder_options() {
    let (_container, address) = start_oxia().await;

    let client = OxiaClientBuilder::new()
        .service_address(address)
        .namespace("default".to_string())
        .identity("test-client".to_string())
        .batch_linger(Duration::from_millis(10))
        .batch_max_size(256 * 1024)
        .session_timeout(Duration::from_secs(30))
        .request_timeout(Duration::from_secs(15))
        .build()
        .await
        .unwrap();

    // Verify client works
    client
        .put("builder/test".to_string(), b"value".to_vec())
        .await
        .unwrap();
    let result = client.get("builder/test".to_string()).await.unwrap();
    assert_eq!(result.value.as_deref(), Some(b"value".to_vec()).as_deref());

    client
        .delete_range("builder/".to_string(), "builder/~".to_string())
        .await
        .unwrap();
    client.close().await.unwrap();
}

// ============================================================
// Clone / Multi-reference Tests
// ============================================================

#[tokio::test]
async fn test_client_clone_concurrent() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    // Use cloned clients concurrently from multiple tasks
    let mut handles = Vec::new();
    for i in 0..10 {
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            c.put(format!("clone/{}", i), format!("value-{}", i).into_bytes())
                .await
                .unwrap();
            let get = c.get(format!("clone/{}", i)).await.unwrap();
            assert_eq!(
                get.value.as_deref(),
                Some(format!("value-{}", i).into_bytes()).as_deref()
            );
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let list = client
        .list("clone/".to_string(), "clone/~".to_string())
        .await
        .unwrap();
    assert_eq!(list.len(), 10);

    client
        .delete_range("clone/".to_string(), "clone/~".to_string())
        .await
        .unwrap();
    client.close().await.unwrap();
}

// ============================================================
// Version metadata Tests
// ============================================================

#[tokio::test]
async fn test_version_metadata() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    let r1 = client
        .put("meta/key".to_string(), b"v1".to_vec())
        .await
        .unwrap();

    assert!(r1.version.version_id >= 0);
    assert!(r1.version.created_timestamp > 0);
    assert!(r1.version.modified_timestamp > 0);

    let r2 = client
        .put("meta/key".to_string(), b"v2".to_vec())
        .await
        .unwrap();

    assert!(r2.version.version_id > r1.version.version_id);
    assert!(r2.version.modifications_count >= 1);
    assert_eq!(r2.version.created_timestamp, r1.version.created_timestamp);
    assert!(r2.version.modified_timestamp >= r1.version.modified_timestamp);

    client
        .delete_range("meta/".to_string(), "meta/~".to_string())
        .await
        .unwrap();
    client.close().await.unwrap();
}

// ============================================================
// Get without value (metadata only)
// ============================================================

#[tokio::test]
async fn test_get_without_value() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    client
        .put("noval/key".to_string(), b"some-value".to_vec())
        .await
        .unwrap();

    // Get without value
    let result = client
        .get("noval/key".to_string())
        .include_value(false)
        .await
        .unwrap();

    assert_eq!(result.key, "noval/key");
    assert_eq!(result.value.as_deref(), None::<&[u8]>);
    assert!(result.version.version_id >= 0);

    // Get with value (default)
    let result = client.get("noval/key".to_string()).await.unwrap();
    assert_eq!(
        result.value.as_deref(),
        Some(b"some-value".to_vec()).as_deref()
    );

    client
        .delete_range("noval/".to_string(), "noval/~".to_string())
        .await
        .unwrap();
    client.close().await.unwrap();
}

// ============================================================
// Large value Tests
// ============================================================

#[tokio::test]
async fn test_large_value() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    // 1MB value
    let large_value: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();

    client
        .put("large/key".to_string(), large_value.clone())
        .await
        .unwrap();

    let result = client.get("large/key".to_string()).await.unwrap();
    assert_eq!(result.value.as_deref(), Some(large_value).as_deref());

    client
        .delete_range("large/".to_string(), "large/~".to_string())
        .await
        .unwrap();
    client.close().await.unwrap();
}

// ============================================================
// Delete non-existent key
// ============================================================

#[tokio::test]
async fn test_delete_nonexistent_key() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    // Deleting a key that doesn't exist should succeed (idempotent)
    let result = client.delete("nonexistent/key123".to_string()).await;
    // Oxia returns KeyNotFound when deleting non-existent keys
    assert!(result.is_ok() || matches!(result, Err(OxiaError::KeyNotFound)));

    client.close().await.unwrap();
}

// ============================================================
// Multiple sequential operations on same key
// ============================================================

#[tokio::test]
async fn test_sequential_operations() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    // Create
    let r1 = client
        .put("seq-ops/key".to_string(), b"v1".to_vec())
        .await
        .unwrap();

    // Read
    let get1 = client.get("seq-ops/key".to_string()).await.unwrap();
    assert_eq!(get1.value.as_deref(), Some(b"v1".to_vec()).as_deref());

    // Update with version check (CAS)
    let r2 = client
        .put("seq-ops/key".to_string(), b"v2".to_vec())
        .expected_version_id(r1.version.version_id)
        .await
        .unwrap();

    // Read updated
    let get2 = client.get("seq-ops/key".to_string()).await.unwrap();
    assert_eq!(get2.value.as_deref(), Some(b"v2".to_vec()).as_deref());
    assert_eq!(get2.version.version_id, r2.version.version_id);

    // Delete with version check
    client
        .delete("seq-ops/key".to_string())
        .expected_version_id(r2.version.version_id)
        .await
        .unwrap();

    // Verify deleted
    let get3 = client.get("seq-ops/key".to_string()).await;
    assert!(matches!(get3, Err(OxiaError::KeyNotFound)));

    client.close().await.unwrap();
}

// ============================================================
// Range scan with values verification
// ============================================================

#[tokio::test]
async fn test_range_scan_values() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    let pairs = vec![("rsv/a", "apple"), ("rsv/b", "banana"), ("rsv/c", "cherry")];
    for (key, val) in &pairs {
        client
            .put(key.to_string(), val.as_bytes().to_vec())
            .await
            .unwrap();
    }

    let result = client
        .range_scan("rsv/".to_string(), "rsv/~".to_string())
        .await
        .unwrap();

    assert_eq!(result.len(), 3);
    // Records should be sorted
    for (i, (key, val)) in pairs.iter().enumerate() {
        assert_eq!(result[i].key, *key);
        assert_eq!(
            result[i].value.as_deref(),
            Some(val.as_bytes().to_vec()).as_deref()
        );
    }

    client
        .delete_range("rsv/".to_string(), "rsv/~".to_string())
        .await
        .unwrap();
    client.close().await.unwrap();
}

// ============================================================
// Empty value test
// ============================================================

#[tokio::test]
async fn test_empty_value() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    client
        .put("empty-val/key".to_string(), b"".to_vec())
        .await
        .unwrap();

    let result = client.get("empty-val/key".to_string()).await.unwrap();
    // Empty bytes may be represented as None or Some([]) depending on protobuf encoding
    assert!(
        result.value.is_none() || result.value.as_deref() == Some(b"".as_ref()),
        "Expected None or Some([]) for empty value, got: {:?}",
        result.value
    );

    client
        .delete_range("empty-val/".to_string(), "empty-val/~".to_string())
        .await
        .unwrap();
    client.close().await.unwrap();
}

// ============================================================
// Special characters in keys
// ============================================================

#[tokio::test]
async fn test_special_chars_in_keys() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    let special_keys = vec![
        "special/key-with-dashes",
        "special/key_with_underscores",
        "special/key.with.dots",
        "special/key:with:colons",
    ];

    for key in &special_keys {
        client
            .put(key.to_string(), b"value".to_vec())
            .await
            .unwrap();
    }

    let list = client
        .list("special/".to_string(), "special/~".to_string())
        .await
        .unwrap();
    assert_eq!(list.len(), special_keys.len());

    client
        .delete_range("special/".to_string(), "special/~".to_string())
        .await
        .unwrap();
    client.close().await.unwrap();
}

// ============================================================
// Notification buffer size option
// ============================================================

#[tokio::test]
async fn test_notifications_with_buffer_size() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    let mut notification_rx = client.notifications().buffer_size(10).await.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    client
        .put("notif2/key1".to_string(), b"v1".to_vec())
        .await
        .unwrap();

    // Should receive at least 1 notification
    let notification = tokio::time::timeout(Duration::from_secs(5), notification_rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert!(matches!(notification, Notification::KeyCreated { .. }));

    client
        .delete_range("notif2/".to_string(), "notif2/~".to_string())
        .await
        .unwrap();
    client.close().await.unwrap();
}

// ============================================================
// Concurrent reads and writes
// ============================================================

#[tokio::test]
async fn test_concurrent_read_write() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    // Write initial data
    for i in 0..10 {
        client
            .put(format!("crw/{}", i), format!("initial-{}", i).into_bytes())
            .await
            .unwrap();
    }

    // Concurrently read and write
    let mut handles = Vec::new();

    // Writers
    for i in 0..10 {
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            c.put(format!("crw/{}", i), format!("updated-{}", i).into_bytes())
                .await
                .unwrap();
        }));
    }

    // Readers
    for i in 0..10 {
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            let _ = c.get(format!("crw/{}", i)).await;
            // Value should be either initial or updated - both are valid
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // Verify final state
    for i in 0..10 {
        let result = client.get(format!("crw/{}", i)).await.unwrap();
        assert_eq!(
            result.value.as_deref(),
            Some(format!("updated-{}", i).into_bytes()).as_deref()
        );
    }

    client
        .delete_range("crw/".to_string(), "crw/~".to_string())
        .await
        .unwrap();
    client.close().await.unwrap();
}

// ============================================================
// Delete range with partition key
// ============================================================

#[tokio::test]
async fn test_delete_range_with_partition_key() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    let pk = "dr-part".to_string();

    for i in 0..3 {
        client
            .put(format!("drpk/{}", i), format!("val-{}", i).into_bytes())
            .partition_key(pk.clone())
            .await
            .unwrap();
    }

    // Delete range with partition key
    client
        .delete_range("drpk/".to_string(), "drpk/~".to_string())
        .partition_key(pk.clone())
        .await
        .unwrap();

    // Verify keys are deleted
    let list = client
        .list("drpk/".to_string(), "drpk/~".to_string())
        .partition_key(pk)
        .await
        .unwrap();
    assert_eq!(list.len(), 0);

    client.close().await.unwrap();
}

// ============================================================
// Range scan with partition key
// ============================================================

#[tokio::test]
async fn test_range_scan_with_partition_key() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    let pk = "rspk-part".to_string();

    for i in 0..3 {
        client
            .put(format!("rspk/{}", i), format!("val-{}", i).into_bytes())
            .partition_key(pk.clone())
            .await
            .unwrap();
    }

    let result = client
        .range_scan("rspk/".to_string(), "rspk/~".to_string())
        .partition_key(pk.clone())
        .await
        .unwrap();

    assert_eq!(result.len(), 3);

    client
        .delete_range("rspk/".to_string(), "rspk/~".to_string())
        .partition_key(pk)
        .await
        .unwrap();
    client.close().await.unwrap();
}

// ============================================================
// Stress / throughput test
// ============================================================

#[tokio::test]
async fn test_high_throughput_writes() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    let num_ops = 100;
    let mut handles = Vec::new();

    // Flood with concurrent put operations
    for i in 0..num_ops {
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            c.put(
                format!("stress/{:04}", i),
                format!("val-{}", i).into_bytes(),
            )
            .await
        }));
    }

    let mut success_count = 0;
    for handle in handles {
        if handle.await.unwrap().is_ok() {
            success_count += 1;
        }
    }
    assert_eq!(success_count, num_ops, "All puts should succeed");

    // Verify all keys exist
    let list = client
        .list("stress/".to_string(), "stress/~".to_string())
        .await
        .unwrap();
    assert_eq!(list.len(), num_ops);

    // Range scan all
    let scan = client
        .range_scan("stress/".to_string(), "stress/~".to_string())
        .await
        .unwrap();
    assert_eq!(scan.len(), num_ops);

    // Flood with concurrent delete operations
    let mut del_handles = Vec::new();
    for i in 0..num_ops {
        let c = client.clone();
        del_handles.push(tokio::spawn(async move {
            c.delete(format!("stress/{:04}", i)).await
        }));
    }
    for handle in del_handles {
        handle.await.unwrap().unwrap();
    }

    // Verify all keys deleted
    let list = client
        .list("stress/".to_string(), "stress/~".to_string())
        .await
        .unwrap();
    assert_eq!(list.len(), 0);

    client.close().await.unwrap();
}

// ============================================================
// Notification Display test
// ============================================================

#[tokio::test]
async fn test_notification_display() {
    let created = Notification::KeyCreated {
        key: "test/key".to_string(),
        version_id: Some(42),
    };
    assert!(format!("{}", created).contains("test/key"));
    assert!(format!("{}", created).contains("42"));

    let deleted = Notification::KeyDeleted {
        key: "test/key".to_string(),
    };
    assert!(format!("{}", deleted).contains("test/key"));

    let modified = Notification::KeyModified {
        key: "test/key".to_string(),
        version_id: Some(43),
    };
    assert!(format!("{}", modified).contains("test/key"));

    let range_deleted = Notification::KeyRangeDeleted {
        key: "test/a".to_string(),
        key_range_last: Some("test/z".to_string()),
    };
    assert!(format!("{}", range_deleted).contains("test/a"));
    assert_eq!(range_deleted.key(), "test/a");
}

// ============================================================
// Multiple secondary indexes per record
// ============================================================

#[tokio::test]
async fn test_multiple_secondary_indexes() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    // Put a record with multiple secondary indexes
    client
        .put("multi-idx/record1".to_string(), b"some-data".to_vec())
        .secondary_index("by-type".to_string(), "document".to_string())
        .secondary_index("by-status".to_string(), "active".to_string())
        .await
        .unwrap();

    // Query via first secondary index
    let list1 = client
        .list("d".to_string(), "e".to_string())
        .use_index("by-type".to_string())
        .await
        .unwrap();
    assert!(!list1.is_empty(), "Should find record via by-type index");

    // Query via second secondary index
    let list2 = client
        .list("a".to_string(), "b".to_string())
        .use_index("by-status".to_string())
        .await
        .unwrap();
    assert!(!list2.is_empty(), "Should find record via by-status index");

    client
        .delete_range("multi-idx/".to_string(), "multi-idx/~".to_string())
        .await
        .unwrap();
    client.close().await.unwrap();
}

// ============================================================
// List and scan partial ranges
// ============================================================

#[tokio::test]
async fn test_list_partial_range() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    for key in &["pr/a", "pr/b", "pr/c", "pr/d", "pr/e"] {
        client.put(key.to_string(), b"v".to_vec()).await.unwrap();
    }

    // List only a subset of the range
    let result = client
        .list("pr/b".to_string(), "pr/d".to_string())
        .await
        .unwrap();
    assert_eq!(result.len(), 2);
    assert!(result.contains(&"pr/b".to_string()));
    assert!(result.contains(&"pr/c".to_string()));

    client
        .delete_range("pr/".to_string(), "pr/~".to_string())
        .await
        .unwrap();
    client.close().await.unwrap();
}

// ============================================================
// Put and immediately get - verify consistency
// ============================================================

#[tokio::test]
async fn test_read_your_writes() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    for i in 0..50 {
        let key = format!("ryw/{}", i);
        let value = format!("value-{}", i);
        client
            .put(key.clone(), value.clone().into_bytes())
            .await
            .unwrap();
        let result = client.get(key).await.unwrap();
        assert_eq!(result.value.as_deref(), Some(value.into_bytes()).as_deref());
    }

    client
        .delete_range("ryw/".to_string(), "ryw/~".to_string())
        .await
        .unwrap();
    client.close().await.unwrap();
}

// ============================================================
// Multiple clients operating concurrently
// ============================================================

#[tokio::test]
async fn test_multiple_clients() {
    let (_container, address) = start_oxia().await;
    let client1 = new_client(&address).await;
    let client2 = new_client(&address).await;

    // Client 1 writes
    client1
        .put("mc/key1".to_string(), b"from-client-1".to_vec())
        .await
        .unwrap();

    // Client 2 reads
    let result = client2.get("mc/key1".to_string()).await.unwrap();
    assert_eq!(
        result.value.as_deref(),
        Some(b"from-client-1".to_vec()).as_deref()
    );

    // Client 2 writes
    client2
        .put("mc/key2".to_string(), b"from-client-2".to_vec())
        .await
        .unwrap();

    // Client 1 reads
    let result = client1.get("mc/key2".to_string()).await.unwrap();
    assert_eq!(
        result.value.as_deref(),
        Some(b"from-client-2".to_vec()).as_deref()
    );

    client1
        .delete_range("mc/".to_string(), "mc/~".to_string())
        .await
        .unwrap();
    client1.close().await.unwrap();
    client2.close().await.unwrap();
}

// ============================================================
// CAS (Compare-And-Swap) loop pattern
// ============================================================

#[tokio::test]
async fn test_cas_loop() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    // Create initial record
    let initial = client
        .put("cas/counter".to_string(), b"0".to_vec())
        .await
        .unwrap();

    // Simulate a CAS loop: read-modify-write with version check
    let mut current_version = initial.version.version_id;
    for i in 1..=5 {
        let result = client
            .put("cas/counter".to_string(), format!("{}", i).into_bytes())
            .expected_version_id(current_version)
            .await
            .unwrap();
        current_version = result.version.version_id;
    }

    // Verify final value
    let final_result = client.get("cas/counter".to_string()).await.unwrap();
    assert_eq!(
        final_result.value.as_deref(),
        Some(b"5".to_vec()).as_deref()
    );

    client
        .delete_range("cas/".to_string(), "cas/~".to_string())
        .await
        .unwrap();
    client.close().await.unwrap();
}

// ============================================================
// Binary (non-UTF8) data
// ============================================================

#[tokio::test]
async fn test_binary_values() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    // Store binary data that isn't valid UTF-8
    let binary_data: Vec<u8> = vec![0x00, 0x01, 0xFF, 0xFE, 0x80, 0x90, 0xAB, 0xCD];
    client
        .put("bin/data".to_string(), binary_data.clone())
        .await
        .unwrap();

    let result = client.get("bin/data".to_string()).await.unwrap();
    assert_eq!(result.value.as_deref(), Some(binary_data).as_deref());

    client
        .delete_range("bin/".to_string(), "bin/~".to_string())
        .await
        .unwrap();
    client.close().await.unwrap();
}

// ============================================================
// Overwrite with different value sizes
// ============================================================

#[tokio::test]
async fn test_overwrite_different_sizes() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    // Start with a small value
    client
        .put("sizes/key".to_string(), b"small".to_vec())
        .await
        .unwrap();

    // Overwrite with a larger value
    let large_value = vec![b'x'; 10000];
    client
        .put("sizes/key".to_string(), large_value.clone())
        .await
        .unwrap();

    let result = client.get("sizes/key".to_string()).await.unwrap();
    assert_eq!(result.value.as_deref(), Some(large_value).as_deref());

    // Overwrite back to a small value
    client
        .put("sizes/key".to_string(), b"tiny".to_vec())
        .await
        .unwrap();

    let result = client.get("sizes/key".to_string()).await.unwrap();
    assert_eq!(result.value.as_deref(), Some(b"tiny".to_vec()).as_deref());

    client
        .delete_range("sizes/".to_string(), "sizes/~".to_string())
        .await
        .unwrap();
    client.close().await.unwrap();
}

// ============================================================
// Concurrent CAS conflict detection
// ============================================================

#[tokio::test]
async fn test_concurrent_cas_conflict() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    // Create a key
    let initial = client
        .put("conflict/key".to_string(), b"v0".to_vec())
        .await
        .unwrap();
    let version = initial.version.version_id;

    // Two concurrent CAS updates with the same version - one should fail
    let c1 = client.clone();
    let c2 = client.clone();

    let h1 = tokio::spawn(async move {
        c1.put("conflict/key".to_string(), b"v1-from-c1".to_vec())
            .expected_version_id(version)
            .await
    });

    let h2 = tokio::spawn(async move {
        c2.put("conflict/key".to_string(), b"v1-from-c2".to_vec())
            .expected_version_id(version)
            .await
    });

    let r1 = h1.await.unwrap();
    let r2 = h2.await.unwrap();

    // Exactly one should succeed and one should fail with UnexpectedVersionId
    let (successes, failures): (Vec<_>, Vec<_>) = vec![r1, r2].into_iter().partition(|r| r.is_ok());

    assert_eq!(successes.len(), 1, "Exactly one CAS should succeed");
    assert_eq!(failures.len(), 1, "Exactly one CAS should fail");
    assert!(matches!(failures[0], Err(OxiaError::UnexpectedVersionId)));

    client
        .delete_range("conflict/".to_string(), "conflict/~".to_string())
        .await
        .unwrap();
    client.close().await.unwrap();
}

/// P1-3: building a client against an unreachable cluster must fail fast within
/// the request timeout instead of returning a client with an empty routing
/// table (or hanging). No server is started here.
#[tokio::test]
async fn build_fails_fast_when_cluster_unreachable() {
    let build = OxiaClientBuilder::new()
        .service_address("http://127.0.0.1:1".to_string())
        .request_timeout(Duration::from_secs(2))
        .build();
    // Bound the whole thing well above the request timeout: if `build` returns
    // an error we failed fast; if this outer timeout fires, it hung.
    match tokio::time::timeout(Duration::from_secs(15), build).await {
        Ok(Ok(_)) => panic!("build unexpectedly succeeded against an unreachable cluster"),
        Ok(Err(_)) => {}
        Err(_) => panic!("build hung instead of failing fast within the request timeout"),
    }
}
