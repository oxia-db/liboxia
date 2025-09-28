use log::info;
use oxia_client_rust::client::{
    Client, DeleteRangeOptions, GetOptions, ListOptions, RangeScanOptions,
};
use oxia_client_rust::client::{DeleteOptions, PutOptions};
use oxia_client_rust::client_builder::OxiaClientBuilder;
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
    let key1 = String::from("key1");
    let key2 = String::from("key2");
    let key3 = String::from("key3");
    let payload = "payload".to_string().into_bytes();
    // put key - 1
    let put_result = client
        .put(key1.clone(), payload.clone(), PutOptions {})
        .await
        .unwrap();
    info!(
        "put the value. key {:?} value {:?} version {:?}",
        put_result.key, payload, put_result.version
    );
    // put key - 2
    let put_result = client
        .put(key2.clone(), payload.clone(), PutOptions {})
        .await
        .unwrap();
    info!(
        "put the value. key {:?} value {:?} version {:?}",
        put_result.key, payload, put_result.version
    );
    // put key - 3
    let put_result = client
        .put(key3.clone(), payload.clone(), PutOptions {})
        .await
        .unwrap();
    info!(
        "put the value. key {:?} value {:?} version {:?}",
        put_result.key, payload, put_result.version
    );
    // list keys
    let list_result = client
        .list("".to_string(), "/".to_string(), ListOptions {})
        .await
        .unwrap();
    info!("list the keys. keys {:?}", list_result.keys);

    // range-scan
    let range_scan_result = client
        .range_scan("".to_string(), "/".to_string(), RangeScanOptions {})
        .await
        .unwrap();
    info!("range_scan result: {:?}", range_scan_result);

    // get key-1
    let get_result = client.get(key1.clone(), GetOptions {}).await.unwrap();
    info!(
        "get the value. key {:?} value {:?} version {:?}",
        get_result.key, get_result.value, get_result.version
    );
    // delete key-1
    client.delete(key1.clone(), DeleteOptions {}).await.unwrap();
    info!("deleted the key-1. key {:?} ", key1.clone());
    let result = client.get(key1.clone(), GetOptions {}).await;
    info!("get the value again. error: {:?}", result.unwrap_err());

    // delete range
    client
        .delete_range("".to_string(), "/".to_string(), DeleteRangeOptions {})
        .await
        .unwrap();
    info!("delete range keys.");
    let list_result = client
        .list("".to_string(), "/".to_string(), ListOptions {})
        .await
        .unwrap();
    info!("list the keys. keys {:?}", list_result.keys);

    client.shutdown().await.unwrap();
}
