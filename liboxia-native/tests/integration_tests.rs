use liboxia::client::{
    DeleteOption, DeleteRangeOption, GetOption, KeyCreated, KeyDeleted, KeyModified, ListOption,
    Notification, PutOption, RangeScanOption,
};
use liboxia::client_builder::OxiaClientBuilder;
use liboxia::errors::OxiaError;
use liboxia::oxia::{KeyComparisonType, SecondaryIndex};
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

async fn new_client(address: &str) -> liboxia::client::OxiaClient {
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
    assert_eq!(get_result.value, Some(b"hello world".to_vec()));
    assert_eq!(get_result.version.version_id, put_result.version.version_id);

    client.shutdown().await.unwrap();
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
    assert_eq!(get.value, Some(b"v2".to_vec()));

    client.shutdown().await.unwrap();
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
    assert!(matches!(result, Err(OxiaError::KeyNotFound())));

    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_get_nonexistent() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    let result = client.get("nonexistent/key".to_string()).await;
    assert!(matches!(result, Err(OxiaError::KeyNotFound())));

    client.shutdown().await.unwrap();
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
    assert_eq!(list_before.keys.len(), 5);

    client
        .delete_range("range/".to_string(), "range/~".to_string())
        .await
        .unwrap();

    let list_after = client
        .list("range/".to_string(), "range/~".to_string())
        .await
        .unwrap();
    assert_eq!(list_after.keys.len(), 0);

    client.shutdown().await.unwrap();
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

    assert_eq!(result.keys.len(), 3);
    assert!(result.keys.contains(&"list/a".to_string()));
    assert!(result.keys.contains(&"list/b".to_string()));
    assert!(result.keys.contains(&"list/c".to_string()));

    // Cleanup
    client
        .delete_range("list/".to_string(), "list/~".to_string())
        .await
        .unwrap();
    client.shutdown().await.unwrap();
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

    assert_eq!(result.records.len(), 3);
    for record in &result.records {
        assert!(record.value.is_some());
        assert!(record.key.starts_with("scan/"));
    }

    // Cleanup
    client
        .delete_range("scan/".to_string(), "scan/~".to_string())
        .await
        .unwrap();
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_list_empty_range() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    let result = client
        .list("empty/".to_string(), "empty/~".to_string())
        .await
        .unwrap();
    assert_eq!(result.keys.len(), 0);

    client.shutdown().await.unwrap();
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
        .put_with_options(
            "ver/key".to_string(),
            b"v2".to_vec(),
            vec![PutOption::ExpectVersionId(r1.version.version_id)],
        )
        .await
        .unwrap();
    assert!(r2.version.version_id > r1.version.version_id);

    // Update with stale version should fail
    let err = client
        .put_with_options(
            "ver/key".to_string(),
            b"v3".to_vec(),
            vec![PutOption::ExpectVersionId(r1.version.version_id)],
        )
        .await;
    assert!(matches!(err, Err(OxiaError::UnexpectedVersionId())));

    client
        .delete_range("ver/".to_string(), "ver/~".to_string())
        .await
        .unwrap();
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_expected_record_not_exists() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    // First put should succeed
    client
        .put_with_options(
            "new/key".to_string(),
            b"v1".to_vec(),
            vec![PutOption::ExpectedRecordNotExists()],
        )
        .await
        .unwrap();

    // Second put should fail - record already exists
    let err = client
        .put_with_options(
            "new/key".to_string(),
            b"v2".to_vec(),
            vec![PutOption::ExpectedRecordNotExists()],
        )
        .await;
    assert!(matches!(err, Err(OxiaError::UnexpectedVersionId())));

    client
        .delete_range("new/".to_string(), "new/~".to_string())
        .await
        .unwrap();
    client.shutdown().await.unwrap();
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
        .delete_with_options(
            "delver/key".to_string(),
            vec![DeleteOption::ExpectVersionId(999)],
        )
        .await;
    assert!(matches!(err, Err(OxiaError::UnexpectedVersionId())));

    // Delete with correct version should succeed
    client
        .delete_with_options(
            "delver/key".to_string(),
            vec![DeleteOption::ExpectVersionId(put_result.version.version_id)],
        )
        .await
        .unwrap();

    let get = client.get("delver/key".to_string()).await;
    assert!(matches!(get, Err(OxiaError::KeyNotFound())));

    client.shutdown().await.unwrap();
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
        .get_with_options(
            "cmp/d".to_string(),
            vec![GetOption::ComparisonType(KeyComparisonType::Floor)],
        )
        .await
        .unwrap();
    assert_eq!(result.key, "cmp/c");

    client
        .delete_range("cmp/".to_string(), "cmp/~".to_string())
        .await
        .unwrap();
    client.shutdown().await.unwrap();
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
        .get_with_options(
            "cmp2/b".to_string(),
            vec![GetOption::ComparisonType(KeyComparisonType::Ceiling)],
        )
        .await
        .unwrap();
    assert_eq!(result.key, "cmp2/c");

    client
        .delete_range("cmp2/".to_string(), "cmp2/~".to_string())
        .await
        .unwrap();
    client.shutdown().await.unwrap();
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
        .get_with_options(
            "cmp3/c".to_string(),
            vec![GetOption::ComparisonType(KeyComparisonType::Lower)],
        )
        .await
        .unwrap();
    assert_eq!(result.key, "cmp3/a");

    client
        .delete_range("cmp3/".to_string(), "cmp3/~".to_string())
        .await
        .unwrap();
    client.shutdown().await.unwrap();
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
        .get_with_options(
            "cmp4/c".to_string(),
            vec![GetOption::ComparisonType(KeyComparisonType::Higher)],
        )
        .await
        .unwrap();
    assert_eq!(result.key, "cmp4/e");

    client
        .delete_range("cmp4/".to_string(), "cmp4/~".to_string())
        .await
        .unwrap();
    client.shutdown().await.unwrap();
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
        .put_with_options(
            "eph/key1".to_string(),
            b"ephemeral-value".to_vec(),
            vec![PutOption::Ephemeral()],
        )
        .await
        .unwrap();

    // Verify it exists
    let get_result = client1.get("eph/key1".to_string()).await.unwrap();
    assert_eq!(get_result.value, Some(b"ephemeral-value".to_vec()));

    // Shutdown client (which closes session, deleting ephemeral keys)
    client1.shutdown().await.unwrap();

    // Wait for session to be cleaned up
    tokio::time::sleep(Duration::from_secs(2)).await;

    // New client should not find the ephemeral key
    let client2 = new_client(&address).await;
    let result = client2.get("eph/key1".to_string()).await;
    assert!(
        matches!(result, Err(OxiaError::KeyNotFound())),
        "Expected KeyNotFound for ephemeral key after client shutdown, got: {:?}",
        result
    );

    client2.shutdown().await.unwrap();
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
    assert_eq!(list.keys.len(), 20);

    // Cleanup
    client
        .delete_range("batch/".to_string(), "batch/~".to_string())
        .await
        .unwrap();
    client.shutdown().await.unwrap();
}

// ============================================================
// Notification Tests
// ============================================================

#[tokio::test]
async fn test_notifications() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    let mut notification_rx = client.get_notifications().await.unwrap();

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
        Notification::KeyCreated(KeyCreated { key, .. }) if key == "notif/key1"
    ));

    // Second should be KeyModified
    assert!(matches!(
        &notifications[1],
        Notification::KeyModified(KeyModified { key, .. }) if key == "notif/key1"
    ));

    // Third should be KeyDeleted
    assert!(matches!(
        &notifications[2],
        Notification::KeyDeleted(KeyDeleted { key }) if key == "notif/key1"
    ));

    client.shutdown().await.unwrap();
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
        .put_with_options(
            "pk/key1".to_string(),
            b"value1".to_vec(),
            vec![PutOption::PartitionKey("my-partition".to_string())],
        )
        .await
        .unwrap();
    assert_eq!(result.key, "pk/key1");

    // Get with partition key
    let get = client
        .get_with_options(
            "pk/key1".to_string(),
            vec![GetOption::PartitionKey("my-partition".to_string())],
        )
        .await
        .unwrap();
    assert_eq!(get.value, Some(b"value1".to_vec()));

    // List with partition key
    let list = client
        .list_with_options(
            "pk/".to_string(),
            "pk/~".to_string(),
            vec![ListOption::PartitionKey("my-partition".to_string())],
        )
        .await
        .unwrap();
    assert!(list.keys.contains(&"pk/key1".to_string()));

    // Delete with partition key
    client
        .delete_with_options(
            "pk/key1".to_string(),
            vec![DeleteOption::PartitionKey("my-partition".to_string())],
        )
        .await
        .unwrap();

    client.shutdown().await.unwrap();
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
        .put_with_options(
            "seq/events".to_string(),
            b"event-1".to_vec(),
            vec![
                PutOption::PartitionKey(partition_key.clone()),
                PutOption::SequenceKeyDelta(vec![1]),
            ],
        )
        .await
        .unwrap();

    let r2 = client
        .put_with_options(
            "seq/events".to_string(),
            b"event-2".to_vec(),
            vec![
                PutOption::PartitionKey(partition_key.clone()),
                PutOption::SequenceKeyDelta(vec![1]),
            ],
        )
        .await
        .unwrap();

    // Server should have generated sequential keys
    assert_ne!(r1.key, r2.key, "Sequence keys should be different");

    // Cleanup
    client
        .delete_range_with_options(
            "seq/".to_string(),
            "seq/~".to_string(),
            vec![DeleteRangeOption::PartitionKey(partition_key)],
        )
        .await
        .unwrap();

    client.shutdown().await.unwrap();
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
        .put_with_options(
            "idx/user1".to_string(),
            b"alice".to_vec(),
            vec![PutOption::SecondaryIndexes(vec![SecondaryIndex {
                index_name: "by-name".to_string(),
                secondary_key: "alice".to_string(),
            }])],
        )
        .await
        .unwrap();

    client
        .put_with_options(
            "idx/user2".to_string(),
            b"bob".to_vec(),
            vec![PutOption::SecondaryIndexes(vec![SecondaryIndex {
                index_name: "by-name".to_string(),
                secondary_key: "bob".to_string(),
            }])],
        )
        .await
        .unwrap();

    // List using secondary index
    let list = client
        .list_with_options(
            "a".to_string(),
            "z".to_string(),
            vec![ListOption::UseIndex("by-name".to_string())],
        )
        .await
        .unwrap();

    assert!(
        list.keys.len() >= 2,
        "Expected at least 2 keys via secondary index, got {}",
        list.keys.len()
    );

    // Range scan using secondary index
    let scan = client
        .range_scan_with_options(
            "a".to_string(),
            "z".to_string(),
            vec![RangeScanOption::UseIndex("by-name".to_string())],
        )
        .await
        .unwrap();

    assert!(
        scan.records.len() >= 2,
        "Expected at least 2 records via secondary index range scan, got {}",
        scan.records.len()
    );

    // Cleanup
    client
        .delete_range("idx/".to_string(), "idx/~".to_string())
        .await
        .unwrap();
    client.shutdown().await.unwrap();
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
    assert_eq!(result.value, Some(b"value".to_vec()));

    client
        .delete_range("builder/".to_string(), "builder/~".to_string())
        .await
        .unwrap();
    client.shutdown().await.unwrap();
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
            assert_eq!(get.value, Some(format!("value-{}", i).into_bytes()));
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let list = client
        .list("clone/".to_string(), "clone/~".to_string())
        .await
        .unwrap();
    assert_eq!(list.keys.len(), 10);

    client
        .delete_range("clone/".to_string(), "clone/~".to_string())
        .await
        .unwrap();
    client.shutdown().await.unwrap();
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
    client.shutdown().await.unwrap();
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
        .get_with_options(
            "noval/key".to_string(),
            vec![GetOption::IncludeValue(false)],
        )
        .await
        .unwrap();

    assert_eq!(result.key, "noval/key");
    assert_eq!(result.value, None);
    assert!(result.version.version_id >= 0);

    // Get with value (default)
    let result = client.get("noval/key".to_string()).await.unwrap();
    assert_eq!(result.value, Some(b"some-value".to_vec()));

    client
        .delete_range("noval/".to_string(), "noval/~".to_string())
        .await
        .unwrap();
    client.shutdown().await.unwrap();
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
    assert_eq!(result.value, Some(large_value));

    client
        .delete_range("large/".to_string(), "large/~".to_string())
        .await
        .unwrap();
    client.shutdown().await.unwrap();
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
    assert!(result.is_ok() || matches!(result, Err(OxiaError::KeyNotFound())));

    client.shutdown().await.unwrap();
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
    assert_eq!(get1.value, Some(b"v1".to_vec()));

    // Update with version check (CAS)
    let r2 = client
        .put_with_options(
            "seq-ops/key".to_string(),
            b"v2".to_vec(),
            vec![PutOption::ExpectVersionId(r1.version.version_id)],
        )
        .await
        .unwrap();

    // Read updated
    let get2 = client.get("seq-ops/key".to_string()).await.unwrap();
    assert_eq!(get2.value, Some(b"v2".to_vec()));
    assert_eq!(get2.version.version_id, r2.version.version_id);

    // Delete with version check
    client
        .delete_with_options(
            "seq-ops/key".to_string(),
            vec![DeleteOption::ExpectVersionId(r2.version.version_id)],
        )
        .await
        .unwrap();

    // Verify deleted
    let get3 = client.get("seq-ops/key".to_string()).await;
    assert!(matches!(get3, Err(OxiaError::KeyNotFound())));

    client.shutdown().await.unwrap();
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

    assert_eq!(result.records.len(), 3);
    // Records should be sorted
    for (i, (key, val)) in pairs.iter().enumerate() {
        assert_eq!(result.records[i].key, *key);
        assert_eq!(result.records[i].value, Some(val.as_bytes().to_vec()));
    }

    client
        .delete_range("rsv/".to_string(), "rsv/~".to_string())
        .await
        .unwrap();
    client.shutdown().await.unwrap();
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
        result.value.is_none() || result.value == Some(b"".to_vec()),
        "Expected None or Some([]) for empty value, got: {:?}",
        result.value
    );

    client
        .delete_range("empty-val/".to_string(), "empty-val/~".to_string())
        .await
        .unwrap();
    client.shutdown().await.unwrap();
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
    assert_eq!(list.keys.len(), special_keys.len());

    client
        .delete_range("special/".to_string(), "special/~".to_string())
        .await
        .unwrap();
    client.shutdown().await.unwrap();
}

// ============================================================
// Notification buffer size option
// ============================================================

#[tokio::test]
async fn test_notifications_with_buffer_size() {
    let (_container, address) = start_oxia().await;
    let client = new_client(&address).await;

    let mut notification_rx = client
        .get_notifications_with_options(vec![liboxia::client::GetNotificationOption::BufferSize(
            10,
        )])
        .await
        .unwrap();

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

    assert!(matches!(notification, Notification::KeyCreated(_)));

    client
        .delete_range("notif2/".to_string(), "notif2/~".to_string())
        .await
        .unwrap();
    client.shutdown().await.unwrap();
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
        assert_eq!(result.value, Some(format!("updated-{}", i).into_bytes()));
    }

    client
        .delete_range("crw/".to_string(), "crw/~".to_string())
        .await
        .unwrap();
    client.shutdown().await.unwrap();
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
            .put_with_options(
                format!("drpk/{}", i),
                format!("val-{}", i).into_bytes(),
                vec![PutOption::PartitionKey(pk.clone())],
            )
            .await
            .unwrap();
    }

    // Delete range with partition key
    client
        .delete_range_with_options(
            "drpk/".to_string(),
            "drpk/~".to_string(),
            vec![DeleteRangeOption::PartitionKey(pk.clone())],
        )
        .await
        .unwrap();

    // Verify keys are deleted
    let list = client
        .list_with_options(
            "drpk/".to_string(),
            "drpk/~".to_string(),
            vec![ListOption::PartitionKey(pk)],
        )
        .await
        .unwrap();
    assert_eq!(list.keys.len(), 0);

    client.shutdown().await.unwrap();
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
            .put_with_options(
                format!("rspk/{}", i),
                format!("val-{}", i).into_bytes(),
                vec![PutOption::PartitionKey(pk.clone())],
            )
            .await
            .unwrap();
    }

    let result = client
        .range_scan_with_options(
            "rspk/".to_string(),
            "rspk/~".to_string(),
            vec![RangeScanOption::PartitionKey(pk.clone())],
        )
        .await
        .unwrap();

    assert_eq!(result.records.len(), 3);

    client
        .delete_range_with_options(
            "rspk/".to_string(),
            "rspk/~".to_string(),
            vec![DeleteRangeOption::PartitionKey(pk)],
        )
        .await
        .unwrap();
    client.shutdown().await.unwrap();
}
