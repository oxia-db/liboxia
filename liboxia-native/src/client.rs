use crate::batch_manager::{BatchManager, Batcher};
use crate::client_options::OxiaClientOptions;
use crate::errors::OxiaError;
use crate::errors::OxiaError::{
    KeyLeaderNotFound, KeyNotFound, SessionDoesNotExist, UnexpectedStatus, UnexpectedVersionId,
};
use crate::key;
use crate::operations::{
    DeleteOperation, DeleteRangeOperation, GetOperation, Operation, PutOperation,
};
use crate::oxia::{
    KeyComparisonType, ListRequest, RangeScanRequest, SecondaryIndex, Status, Version,
};
use crate::provider_manager::ProviderManager;
use crate::session_manager::SessionManager;
use crate::shard_manager::{ShardManager, ShardManagerOptions};
use crate::write_stream_manager::WriteStreamManager;
use dashmap::DashMap;
use std::cmp::Ordering;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tonic::codegen::tokio_stream::StreamExt;
use tonic::{async_trait, Request};

pub enum PutOption {
    ExpectVersionId(i64),
    PartitionKey(String),
    SequenceKeyDelta(Vec<u64>),
    SecondaryIndexes(Vec<SecondaryIndex>),
    Ephemeral(),
}

#[derive(Clone, Debug, PartialEq)]
pub struct PutResult {
    pub key: String,
    pub version: Version,
}

pub enum DeleteOption {
    PartitionKey(String),
    ExpectVersionId(i64),
    RecordDoesNotExist(),
}

pub enum DeleteRangeOption {
    PartitionKey(String),
}

pub enum GetOption {
    ComparisonType(KeyComparisonType),
    PartitionKey(String),
    IncludeValue(),
    UseIndex(String),
}

#[derive(Clone, Debug, PartialEq)]
pub struct GetResult {
    pub key: String,
    pub value: Option<Vec<u8>>,
    pub version: Version,
}
impl Eq for GetResult {}

impl PartialOrd for GetResult {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for GetResult {
    fn cmp(&self, other: &Self) -> Ordering {
        key::compare(&self.key, &other.key)
    }
}

pub enum ListOption {
    PartitionKey(String),
    UseIndex(String),
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListResult {
    pub keys: Vec<String>,
}

pub enum RangeScanOption {
    PartitionKey(String),
    UseIndex(String),
}

#[derive(Clone, Debug, PartialEq)]
pub struct RangeScanResult {
    pub records: Vec<GetResult>,
}

pub enum GetSequenceUpdatesOption {
    PartitionKey(String),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Notification {}

#[async_trait]
pub trait Client: Send + Sync + Clone {
    async fn put(
        &self,
        key: String,
        value: Vec<u8>,
        options: Vec<PutOption>,
    ) -> Result<PutResult, OxiaError>;

    async fn delete(&self, key: String, options: Vec<DeleteOption>) -> Result<(), OxiaError>;

    async fn get(&self, key: String, options: Vec<GetOption>) -> Result<GetResult, OxiaError>;

    async fn list(
        &self,
        min_key_inclusive: String,
        max_key_exclusive: String,
        options: Vec<ListOption>,
    ) -> Result<ListResult, OxiaError>;

    async fn range_scan(
        &self,
        min_key_inclusive: String,
        max_key_exclusive: String,
        options: Vec<RangeScanOption>,
    ) -> Result<RangeScanResult, OxiaError>;

    async fn delete_range(
        &self,
        min_key_inclusive: String,
        max_key_exclusive: String,
        options: Vec<DeleteRangeOption>,
    ) -> Result<(), OxiaError>;

    async fn get_notifications(&self) -> Result<UnboundedReceiver<Notification>, OxiaError>;
    async fn get_sequence_updates(
        &self,
        key: String,
        options: Vec<GetSequenceUpdatesOption>,
    ) -> Result<UnboundedReceiver<String>, OxiaError>;

    async fn shutdown(self) -> Result<(), OxiaError>;
}

pub(crate) struct Inner {
    pub(crate) options: OxiaClientOptions,
    pub(crate) provider_manager: Arc<ProviderManager>,
    pub(crate) shard_manager: Arc<ShardManager>,
    pub(crate) session_manager: Arc<SessionManager>,
    pub(crate) write_stream_manager: Arc<WriteStreamManager>,
    pub(crate) write_batch_manager: DashMap<i64, Arc<BatchManager>>,
    pub(crate) read_batch_manager: DashMap<i64, Arc<BatchManager>>,
}

#[derive(Clone)]
pub struct ClientImpl {
    inner: Arc<Inner>,
}

#[async_trait]
impl Client for ClientImpl {
    async fn put(
        &self,
        key: String,
        value: Vec<u8>,
        options: Vec<PutOption>,
    ) -> Result<PutResult, OxiaError> {
        let (tx, rx) = oneshot::channel();
        let mut operation: PutOperation = options.into();
        operation.callback = Some(tx);
        operation.key = key.clone();
        operation.value = value.clone();
        operation.client_identity = Some(self.inner.options.identity.clone());
        let (shard_id, batch_manager) = match &operation.partition_key {
            None => self.get_or_init_batch_manager(Batcher::Write, &key)?,
            Some(partition_key) => self.get_or_init_batch_manager(Batcher::Write, partition_key)?,
        };
        if operation.ephemeral {
            operation.session_id = Some(self.inner.session_manager.get_session_id(shard_id).await?)
        }
        batch_manager.add(Operation::Put(operation))?;
        let put_response = rx
            .await
            .map_err(|err| UnexpectedStatus(err.to_string()))??;
        check_status(put_response.status)?;
        Ok(PutResult {
            key: put_response.key.or(Some(key.clone())).unwrap(),
            version: put_response.version.unwrap(),
        })
    }

    async fn delete(&self, key: String, options: Vec<DeleteOption>) -> Result<(), OxiaError> {
        let (tx, rx) = oneshot::channel();
        let mut operation: DeleteOperation = options.into();
        operation.callback = Some(tx);
        operation.key = key.clone();
        let (_, batch_manager) = match &operation.partition_key {
            None => self.get_or_init_batch_manager(Batcher::Write, &key)?,
            Some(partition_key) => self.get_or_init_batch_manager(Batcher::Write, partition_key)?,
        };
        batch_manager.add(Operation::Delete(operation))?;
        let response = rx
            .await
            .map_err(|err| UnexpectedStatus(err.to_string()))??;

        Ok(check_status(response.status)?)
    }

    async fn get(&self, key: String, options: Vec<GetOption>) -> Result<GetResult, OxiaError> {
        let mut operation: GetOperation = options.into();
        operation.key = key.clone();
        if operation.partition_key.is_some() {
            let (_, batch_manager) = match &operation.partition_key {
                None => self.get_or_init_batch_manager(Batcher::Read, &key)?,
                Some(partition_key) => {
                    self.get_or_init_batch_manager(Batcher::Read, partition_key)?
                }
            }
            .clone();
            get_from_single_shard(operation.clone(), batch_manager).await
        } else {
            let mut join_set = JoinSet::new();
            for (shard, leader) in self.inner.shard_manager.get_shards_leader() {
                let (_, batch_manager) = self
                    .get_or_init_batch_manager_with_shard(Batcher::Read, shard)?
                    .clone();
                join_set.spawn(get_from_single_shard(
                    operation.clone(),
                    batch_manager.clone(),
                ));
            }
            let mut results = join_set
                .join_all()
                .await
                .into_iter()
                .collect::<Result<Vec<GetResult>, OxiaError>>()?;
            results.sort();
            let index = match operation.comparison_type {
                KeyComparisonType::Equal
                | KeyComparisonType::Ceiling
                | KeyComparisonType::Higher => 0,
                KeyComparisonType::Floor | KeyComparisonType::Lower => results.len() - 1,
            };
            Ok(results.swap_remove(index))
        }
    }

    async fn list(
        &self,
        min_key_inclusive: String,
        max_key_exclusive: String,
        options: Vec<ListOption>,
    ) -> Result<ListResult, OxiaError> {
        let mut join_set = JoinSet::new();
        for (shard, leader) in self.inner.shard_manager.get_shards_leader() {
            let provider_manager = self.inner.provider_manager.clone();
            let min_key_inclusive_clone = min_key_inclusive.clone();
            let max_key_exclusive_clone = max_key_exclusive.clone();
            join_set.spawn(async move {
                let client = provider_manager
                    .get_provider(leader.service_address)
                    .await?;
                let mut client_guard = client.lock().await;
                let mut streaming = client_guard
                    .list(Request::new(ListRequest {
                        shard: Some(shard),
                        start_inclusive: min_key_inclusive_clone,
                        end_exclusive: max_key_exclusive_clone,
                        secondary_index_name: None,
                    }))
                    .await?
                    .into_inner();
                let mut keys = Vec::new();
                loop {
                    match streaming.next().await {
                        Some(response) => match response {
                            Ok(mut response) => keys.append(&mut response.keys),
                            Err(err) => {
                                return Err(UnexpectedStatus(err.to_string()));
                            }
                        },
                        None => break,
                    }
                }
                return Ok(keys);
            });
        }
        // todo: ordering by oxia sort
        let mut output_keys = Vec::new();
        for result in join_set.join_all().await {
            let mut keys = result?;
            output_keys.append(&mut keys);
        }
        output_keys.sort_by(|left, right| key::compare(left, right));
        Ok(ListResult { keys: output_keys })
    }

    async fn range_scan(
        &self,
        min_key_inclusive: String,
        max_key_exclusive: String,
        options: Vec<RangeScanOption>,
    ) -> Result<RangeScanResult, OxiaError> {
        let mut join_set = JoinSet::new();
        for (shard, leader) in self.inner.shard_manager.get_shards_leader() {
            let provider_manager = self.inner.provider_manager.clone();
            let min_key_inclusive_clone = min_key_inclusive.clone();
            let max_key_exclusive_clone = max_key_exclusive.clone();
            join_set.spawn(async move {
                let client = provider_manager
                    .get_provider(leader.service_address)
                    .await?;
                let mut client_guard = client.lock().await;
                let mut streaming = client_guard
                    .range_scan(Request::new(RangeScanRequest {
                        shard: Some(shard),
                        start_inclusive: min_key_inclusive_clone,
                        end_exclusive: max_key_exclusive_clone,
                        secondary_index_name: None,
                    }))
                    .await?
                    .into_inner();
                let mut records = Vec::new();
                loop {
                    match streaming.next().await {
                        Some(response) => match response {
                            Ok(mut response) => records.append(&mut response.records),
                            Err(err) => {
                                return Err(UnexpectedStatus(err.to_string()));
                            }
                        },
                        None => break,
                    }
                }
                return Ok(records);
            });
        }
        // todo: ordering by oxia sort
        let mut output_records = Vec::new();
        for result in join_set.join_all().await {
            let records = result?;
            for record in records {
                output_records.push(GetResult {
                    key: record.key.unwrap(),
                    value: record.value,
                    version: record.version.unwrap(),
                });
            }
        }
        Ok(RangeScanResult {
            records: output_records,
        })
    }

    async fn delete_range(
        &self,
        min_key_inclusive: String,
        max_key_exclusive: String,
        options: Vec<DeleteRangeOption>,
    ) -> Result<(), OxiaError> {
        let mut join_set = JoinSet::new();
        for (shard, _) in self.inner.shard_manager.get_shards_leader() {
            let min_key_inclusive_clone = min_key_inclusive.clone();
            let max_key_exclusive_clone = max_key_exclusive.clone();
            let (_, batch_manager) = self
                .get_or_init_batch_manager_with_shard(Batcher::Write, shard)?
                .clone();
            join_set.spawn(async move {
                let (tx, rx) = oneshot::channel();
                batch_manager.add(Operation::DeleteRange(DeleteRangeOperation {
                    callback: Some(tx),
                    start_inclusive: min_key_inclusive_clone,
                    end_exclusive: max_key_exclusive_clone,
                }))?;
                let response = rx
                    .await
                    .map_err(|err| UnexpectedStatus(err.to_string()))??;
                check_status(response.status)
            });
        }
        for result in join_set.join_all().await {
            result?;
        }
        Ok(())
    }

    async fn get_notifications(&self) -> Result<UnboundedReceiver<Notification>, OxiaError> {
        todo!()
    }

    async fn get_sequence_updates(
        &self,
        key: String,
        options: Vec<GetSequenceUpdatesOption>,
    ) -> Result<UnboundedReceiver<String>, OxiaError> {
        todo!()
    }

    async fn shutdown(self) -> Result<(), OxiaError> {
        let inner = match Arc::try_unwrap(self.inner) {
            Ok(inner) => inner,
            Err(arc) => {
                return Err(UnexpectedStatus(format!(
                    "Cannot shutdown inner: {} other references exist",
                    Arc::strong_count(&arc)
                )))
            }
        };

        let mut joiner = JoinSet::new();
        for (_, wb) in inner.write_batch_manager.into_iter() {
            let manager = match Arc::try_unwrap(wb) {
                Ok(inner) => inner,
                Err(arc) => {
                    return Err(UnexpectedStatus(format!(
                        "Cannot shutdown write batch manager: {} other references exist",
                        Arc::strong_count(&arc)
                    )))
                }
            };
            joiner.spawn(async move { manager.shutdown().await });
        }
        for (_, rb) in inner.read_batch_manager.into_iter() {
            let manager = match Arc::try_unwrap(rb) {
                Ok(inner) => inner,
                Err(arc) => {
                    return Err(UnexpectedStatus(format!(
                        "Cannot shutdown read batch manager: {} other references exist",
                        Arc::strong_count(&arc)
                    )))
                }
            };
            joiner.spawn(async move { manager.shutdown().await });
        }
        while let Some(result) = joiner.join_next().await {
            result.map_err(|err| {
                UnexpectedStatus(format!("batcher task failed to join: {}", err))
            })??;
        }

        match Arc::try_unwrap(inner.write_stream_manager) {
            Ok(wsm) => wsm,
            Err(arc) => {
                return Err(UnexpectedStatus(format!(
                    "Cannot shutdown write stream manager: {} other references exist",
                    Arc::strong_count(&arc)
                )))
            }
        }
        .shutdown()
        .await?;

        match Arc::try_unwrap(inner.session_manager) {
            Ok(sm) => sm,
            Err(arc) => {
                return Err(UnexpectedStatus(format!(
                    "Cannot shutdown session manager: {} other references exist",
                    Arc::strong_count(&arc)
                )));
            }
        }
        .shutdown()
        .await?;

        match Arc::try_unwrap(inner.shard_manager) {
            Ok(sm) => sm,
            Err(arc) => {
                return Err(UnexpectedStatus(format!(
                    "Cannot shutdown shard manager: {} other references exist",
                    Arc::strong_count(&arc)
                )));
            }
        }
        .shutdown()
        .await?;

        match Arc::try_unwrap(inner.provider_manager) {
            Ok(pm) => pm,
            Err(arc) => {
                return Err(UnexpectedStatus(format!(
                    "Cannot shutdown provider manager: {} other references exist",
                    Arc::strong_count(&arc)
                )))
            }
        }
        .shutdown()
        .await?;
        Ok(())
    }
}

impl ClientImpl {
    pub async fn new(options: OxiaClientOptions) -> Result<ClientImpl, OxiaError> {
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
        let session_manager = Arc::new(SessionManager::new(
            options.identity.clone(),
            options.session_timeout,
            shard_manager.clone(),
            provider_manager.clone(),
        ));
        Ok(ClientImpl {
            inner: Arc::new(Inner {
                options,
                write_stream_manager,
                provider_manager,
                shard_manager,
                session_manager,
                write_batch_manager: DashMap::new(),
                read_batch_manager: DashMap::new(),
            }),
        })
    }

    fn get_or_init_batch_manager(
        &self,
        batcher: Batcher,
        key: &str,
    ) -> Result<(i64, Arc<BatchManager>), OxiaError> {
        match self.inner.shard_manager.get_shard(&key) {
            Some(shard_id) => self.get_or_init_batch_manager_with_shard(batcher, shard_id),
            None => Err(KeyLeaderNotFound(key.to_string())),
        }
    }

    fn get_or_init_batch_manager_with_shard(
        &self,
        batcher: Batcher,
        shard_id: i64,
    ) -> Result<(i64, Arc<BatchManager>), OxiaError> {
        let closure = || {
            Arc::new(BatchManager::new(
                shard_id,
                batcher.clone(),
                self.inner.shard_manager.clone(),
                self.inner.provider_manager.clone(),
                self.inner.write_stream_manager.clone(),
                self.inner.options.batch_linger.clone(),
                self.inner.options.batch_max_size.clone(),
            ))
        };
        let ref_mut = match batcher {
            Batcher::Read => self
                .inner
                .read_batch_manager
                .entry(shard_id)
                .or_insert_with(closure),
            Batcher::Write => self
                .inner
                .write_batch_manager
                .entry(shard_id)
                .or_insert_with(closure),
        };
        Ok((ref_mut.key().clone(), ref_mut.value().clone()))
    }
}

#[inline]
async fn get_from_single_shard(
    mut operation: GetOperation,
    batch_manager: Arc<BatchManager>,
) -> Result<GetResult, OxiaError> {
    let extra_key = operation.key.clone();
    let (tx, rx) = oneshot::channel();
    operation.callback = Some(tx);
    batch_manager.add(Operation::Get(operation))?;
    let get_response = rx
        .await
        .map_err(|err| UnexpectedStatus(err.to_string()))??;
    check_status(get_response.status)?;
    Ok(GetResult {
        key: get_response.key.or(Some(extra_key)).unwrap(),
        value: get_response.value,
        version: get_response.version.unwrap(),
    })
}

fn check_status(status: i32) -> Result<(), OxiaError> {
    match Status::try_from(status) {
        Ok(status) => match status {
            Status::Ok => Ok(()),
            Status::KeyNotFound => Err(KeyNotFound()),
            Status::UnexpectedVersionId => Err(UnexpectedVersionId()),
            Status::SessionDoesNotExist => Err(SessionDoesNotExist()),
        },
        Err(err) => Err(UnexpectedStatus(err.to_string())),
    }
}
