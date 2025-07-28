use crate::client_options::OxiaClientOptions;
use crate::errors::OxiaError;
use crate::oxia::oxia_client_client::OxiaClientClient;
use crate::shard_manager::{ShardManagerImpl, ShardManagerOptions};
use std::sync::Arc;
use tokio::runtime;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedReceiver;
use tonic::transport::Channel;

pub struct PutOptions {}

pub struct PutResult {}

pub struct DeleteOptions {}

pub struct DeleteRangeOptions {}

pub struct GetOptions {}

pub struct GetResult {}

pub struct ListOptions {}

pub struct ListResult {}

pub struct RangeScanOptions {}

pub struct RangeScanResult {}

pub struct GetSequenceUpdatesOptions {}

pub struct Notification {}

pub trait Client {
    async fn put(key: String, value: Vec<u8>, options: PutOptions) -> Result<PutResult, OxiaError>;
    async fn delete(key: String, options: DeleteOptions) -> Result<(), OxiaError>;
    async fn delete_range(
        min_key_inclusive: String,
        max_key_exclusive: String,
        options: DeleteRangeOptions,
    ) -> Result<(), OxiaError>;
    async fn get(key: String, options: GetOptions) -> Result<GetResult, OxiaError>;
    async fn list(
        min_key_inclusive: String,
        max_key_exclusive: String,
        options: ListOptions,
    ) -> Result<ListResult, OxiaError>;
    async fn range_scan(
        min_key_inclusive: String,
        max_key_exclusive: String,
        options: RangeScanOptions,
    ) -> Result<RangeScanResult, OxiaError>;
    async fn get_notifications() -> Result<mpsc::UnboundedReceiver<Notification>, OxiaError>;
    async fn get_sequence_updates(
        key: String,
        options: GetSequenceUpdatesOptions,
    ) -> Result<mpsc::UnboundedReceiver<String>, OxiaError>;
}

pub(crate) struct ClientImpl {
    pub(crate) provider: OxiaClientClient<Channel>,
    pub(crate) runtime: Arc<Runtime>,
    pub(crate) shard_manager: Arc<ShardManagerImpl>,
}

impl Client for ClientImpl {
    async fn put(key: String, value: Vec<u8>, options: PutOptions) -> Result<PutResult, OxiaError> {
        todo!()
    }

    async fn delete(key: String, options: DeleteOptions) -> Result<(), OxiaError> {
        todo!()
    }

    async fn delete_range(
        min_key_inclusive: String,
        max_key_exclusive: String,
        options: DeleteRangeOptions,
    ) -> Result<(), OxiaError> {
        todo!()
    }

    async fn get(key: String, options: GetOptions) -> Result<GetResult, OxiaError> {
        todo!()
    }

    async fn list(
        min_key_inclusive: String,
        max_key_exclusive: String,
        options: ListOptions,
    ) -> Result<ListResult, OxiaError> {
        todo!()
    }

    async fn range_scan(
        min_key_inclusive: String,
        max_key_exclusive: String,
        options: RangeScanOptions,
    ) -> Result<RangeScanResult, OxiaError> {
        todo!()
    }

    async fn get_notifications() -> Result<UnboundedReceiver<Notification>, OxiaError> {
        todo!()
    }

    async fn get_sequence_updates(
        key: String,
        options: GetSequenceUpdatesOptions,
    ) -> Result<UnboundedReceiver<String>, OxiaError> {
        todo!()
    }
}

impl ClientImpl {
    pub fn new(option: OxiaClientOptions) -> Result<impl Client, OxiaError> {
        let runtime = runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("failed to build tokio runtime");
        let provider = runtime
            .block_on(OxiaClientClient::connect(option.service_address))
            .map_err(|e| OxiaError::Connection(format!("failed to connect: {e}")))?;
        let shard_runtime = Arc::new(runtime);
        let sm = ShardManagerImpl::new(ShardManagerOptions {
            namespace: option.namespace,
            runtime: shard_runtime.clone(),
            provider: provider.clone(),
        })?;

        Ok(ClientImpl {
            provider,
            runtime: shard_runtime,
            shard_manager: Arc::new(sm),
        })
    }
}
