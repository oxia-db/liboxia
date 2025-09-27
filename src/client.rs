use crate::batch_manager::{BatchManager, Batcher};
use crate::client_options::OxiaClientOptions;
use crate::errors::OxiaError;
use crate::errors::OxiaError::{KeyLeaderNotFound, UnexpectedStatus};
use crate::operations::{Operation, PutOperation};
use crate::oxia::Version;
use crate::provider_manager::ProviderManager;
use crate::shard_manager::{ShardManager, ShardManagerOptions};
use crate::write_stream_manager::WriteStreamManager;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{mpsc, oneshot};
use tonic::async_trait;

pub struct PutOptions {}

pub struct PutResult {
    key: String,
    version: Version,
}

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

#[async_trait]
pub trait Client {
    async fn put(
        &self,
        key: String,
        value: Vec<u8>,
        options: PutOptions,
    ) -> Result<PutResult, OxiaError>;

    async fn delete(&self, key: String, options: DeleteOptions) -> Result<(), OxiaError>;
    async fn delete_range(
        &self,
        min_key_inclusive: String,
        max_key_exclusive: String,
        options: DeleteRangeOptions,
    ) -> Result<(), OxiaError>;
    async fn get(&self, key: String, options: GetOptions) -> Result<GetResult, OxiaError>;
    async fn list(
        &self,
        min_key_inclusive: String,
        max_key_exclusive: String,
        options: ListOptions,
    ) -> Result<ListResult, OxiaError>;
    async fn range_scan(
        &self,
        min_key_inclusive: String,
        max_key_exclusive: String,
        options: RangeScanOptions,
    ) -> Result<RangeScanResult, OxiaError>;
    async fn get_notifications(&self) -> Result<mpsc::UnboundedReceiver<Notification>, OxiaError>;
    async fn get_sequence_updates(
        &self,
        key: String,
        options: GetSequenceUpdatesOptions,
    ) -> Result<mpsc::UnboundedReceiver<String>, OxiaError>;
}

pub(crate) struct ClientImpl {
    pub(crate) options: OxiaClientOptions,
    pub(crate) provider_manager: Arc<ProviderManager>,
    pub(crate) shard_manager: Arc<ShardManager>,
    pub(crate) write_stream_manager: Arc<WriteStreamManager>,
    pub(crate) write_batch_manager: DashMap<i64, BatchManager>,
    pub(crate) read_batch_manager: DashMap<i64, BatchManager>,
}

#[async_trait]
impl Client for ClientImpl {
    async fn put(
        &self,
        key: String,
        value: Vec<u8>,
        options: PutOptions,
    ) -> Result<PutResult, OxiaError> {
        match self.shard_manager.get_shard(&key) {
            Some(shard_id) => {
                let batch_manager = self.write_batch_manager.entry(shard_id).or_insert_with(|| {
                    BatchManager::new(
                        shard_id,
                        Batcher::Write,
                        self.shard_manager.clone(),
                        self.provider_manager.clone(),
                        self.write_stream_manager.clone(),
                        self.options.batch_linger.clone(),
                        self.options.batch_max_size.clone(),
                    )
                });
                let (tx, rx) = oneshot::channel();
                batch_manager.add(Operation::Put(PutOperation {
                    callback: Some(tx),
                    key: key.clone(),
                    value,
                    expected_version_id: None,
                    session_id: None,
                    client_identity: None,
                    partition_key: None,
                    sequence_key_delta: vec![],
                    secondary_indexes: vec![],
                }))?;
                let put_response = rx
                    .await
                    .map_err(|err| UnexpectedStatus(err.to_string()))??;
                Ok(PutResult {
                    key: put_response.key.or(Some(key.clone())).unwrap(),
                    version: put_response.version.unwrap(),
                })
            }
            None => Err(KeyLeaderNotFound(key.clone())),
        }
    }

    async fn delete(&self, key: String, options: DeleteOptions) -> Result<(), OxiaError> {
        todo!()
    }

    async fn delete_range(
        &self,
        min_key_inclusive: String,
        max_key_exclusive: String,
        options: DeleteRangeOptions,
    ) -> Result<(), OxiaError> {
        todo!()
    }

    async fn get(&self, key: String, options: GetOptions) -> Result<GetResult, OxiaError> {
        todo!()
    }

    async fn list(
        &self,
        min_key_inclusive: String,
        max_key_exclusive: String,
        options: ListOptions,
    ) -> Result<ListResult, OxiaError> {
        todo!()
    }

    async fn range_scan(
        &self,
        min_key_inclusive: String,
        max_key_exclusive: String,
        options: RangeScanOptions,
    ) -> Result<RangeScanResult, OxiaError> {
        todo!()
    }

    async fn get_notifications(&self) -> Result<UnboundedReceiver<Notification>, OxiaError> {
        todo!()
    }

    async fn get_sequence_updates(
        &self,
        key: String,
        options: GetSequenceUpdatesOptions,
    ) -> Result<UnboundedReceiver<String>, OxiaError> {
        todo!()
    }
}

impl ClientImpl {
    pub async fn new(options: OxiaClientOptions) -> Result<impl Client, OxiaError> {
        let provider_manager = Arc::new(ProviderManager::new());
        let shard_manager = Arc::new(
            ShardManager::new(ShardManagerOptions {
                address: options.service_address.clone(),
                namespace: options.namespace.clone(),
                provider_manager: provider_manager.clone(),
            })
            .await?,
        );
        let write_stream_manager = Arc::new(WriteStreamManager::new(
            options.namespace.clone(),
            shard_manager.clone(),
            provider_manager.clone(),
        ));
        Ok(ClientImpl {
            options,
            write_stream_manager,
            provider_manager,
            shard_manager,
            write_batch_manager: DashMap::new(),
            read_batch_manager: DashMap::new(),
        })
    }
}
