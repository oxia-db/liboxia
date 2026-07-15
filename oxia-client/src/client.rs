//! The Oxia client: construction, per-operation execution, and shutdown.

use crate::batch_manager::{BatchManager, Batcher, BatcherContext};
use crate::client_builder::OxiaClientBuilder;
use crate::client_options::OxiaClientOptions;
use crate::errors::OxiaError;
use crate::notification_manager::NotificationManager;
use crate::operations::{Operation, Pending};
use crate::proto;
use crate::provider_manager::ProviderManager;
use crate::requests::{
    DeleteBuilder, DeleteRangeBuilder, GetBuilder, ListBuilder, NotificationsBuilder, PutBuilder,
    RangeScanBuilder, SequenceUpdatesBuilder,
};
use crate::sequence_updates_manager::SequenceUpdatesManager;
use crate::session_manager::SessionManager;
use crate::shard_manager::{ShardManager, ShardManagerOptions};
use crate::streams::{
    ListStream, Notifications, RangeScanStream, SequenceUpdates, ShardStream, merged,
};
use crate::types::{ComparisonType, GetResult};
use crate::write_stream_manager::WriteStreamManager;
use bytes::Bytes;
use dashmap::DashMap;
use futures::StreamExt;
use futures::future::try_join_all;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinSet;
use tracing::warn;

pub(crate) struct Inner {
    pub(crate) options: OxiaClientOptions,
    pub(crate) provider_manager: Arc<ProviderManager>,
    pub(crate) shard_manager: Arc<ShardManager>,
    pub(crate) session_manager: Arc<SessionManager>,
    pub(crate) write_stream_manager: Arc<WriteStreamManager>,
    notification_tasks: Mutex<Vec<NotificationManager>>,
    sequence_tasks: Mutex<Vec<SequenceUpdatesManager>>,
    write_batchers: DashMap<i64, Arc<BatchManager>>,
    read_batchers: DashMap<i64, Arc<BatchManager>>,
    closed: AtomicBool,
}

/// The Oxia client.
///
/// Cheap to clone (all clones share one set of connections, batchers and
/// sessions) and safe to share across tasks. Obtain one with
/// [`OxiaClient::connect`] or [`OxiaClient::builder`]; every data operation
/// returns a request builder that is executed by `.await`ing it.
///
/// Dropping the last clone tears down all background work abruptly; call
/// [`close`](OxiaClient::close) for a graceful shutdown that flushes pending
/// batches and closes sessions.
#[derive(Clone)]
pub struct OxiaClient {
    inner: Arc<Inner>,
}

impl fmt::Debug for OxiaClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OxiaClient")
            .field("namespace", &self.inner.options.namespace)
            .field("service_address", &self.inner.options.service_address)
            .finish_non_exhaustive()
    }
}

impl OxiaClient {
    /// Returns a builder for configuring and creating a client.
    pub fn builder() -> OxiaClientBuilder {
        OxiaClientBuilder::new()
    }

    /// Connects to the given service address (`host:port`) with default options.
    pub async fn connect(service_address: impl Into<String>) -> Result<OxiaClient, OxiaError> {
        OxiaClientBuilder::new()
            .service_address(service_address)
            .build()
            .await
    }

    pub(crate) async fn new(options: OxiaClientOptions) -> Result<OxiaClient, OxiaError> {
        let provider_manager = Arc::new(ProviderManager::new(options.request_timeout));
        let shard_manager = Arc::new(
            ShardManager::new(ShardManagerOptions {
                address: options.service_address.clone(),
                namespace: options.namespace.clone(),
                provider_manager: provider_manager.clone(),
                request_timeout: options.request_timeout,
            })
            .await?,
        );
        let write_stream_manager = Arc::new(WriteStreamManager::new(
            options.namespace.clone(),
            shard_manager.clone(),
            provider_manager.clone(),
            options.request_timeout,
        ));
        let session_manager = Arc::new(SessionManager::new(
            options.identity.clone(),
            options.session_timeout,
            options.session_keep_alive,
            options.request_timeout,
            shard_manager.clone(),
            provider_manager.clone(),
        ));
        Ok(OxiaClient {
            inner: Arc::new(Inner {
                options,
                provider_manager,
                shard_manager,
                session_manager,
                write_stream_manager,
                notification_tasks: Mutex::new(Vec::new()),
                sequence_tasks: Mutex::new(Vec::new()),
                write_batchers: DashMap::new(),
                read_batchers: DashMap::new(),
                closed: AtomicBool::new(false),
            }),
        })
    }

    /// Stores `value` under `key`.
    ///
    /// Returns a [`PutBuilder`]; chain options
    /// ([`ephemeral`](PutBuilder::ephemeral),
    /// [`expected_version_id`](PutBuilder::expected_version_id), …) and
    /// `.await` it.
    pub fn put(&self, key: impl Into<String>, value: impl Into<Bytes>) -> PutBuilder {
        PutBuilder::new(self.clone(), key.into(), value.into())
    }

    /// Reads the record associated with `key`.
    ///
    /// Returns a [`GetBuilder`]; chain options
    /// ([`comparison`](GetBuilder::comparison),
    /// [`use_index`](GetBuilder::use_index), …) and `.await` it.
    pub fn get(&self, key: impl Into<String>) -> GetBuilder {
        GetBuilder::new(self.clone(), key.into())
    }

    /// Deletes the record associated with `key`.
    pub fn delete(&self, key: impl Into<String>) -> DeleteBuilder {
        DeleteBuilder::new(self.clone(), key.into())
    }

    /// Deletes every record with `min_key_inclusive <= key < max_key_exclusive`
    /// (in Oxia's slash-aware key order).
    pub fn delete_range(
        &self,
        min_key_inclusive: impl Into<String>,
        max_key_exclusive: impl Into<String>,
    ) -> DeleteRangeBuilder {
        DeleteRangeBuilder::new(
            self.clone(),
            min_key_inclusive.into(),
            max_key_exclusive.into(),
        )
    }

    /// Lists the keys with `min_key_inclusive <= key < max_key_exclusive`
    /// (in Oxia's slash-aware key order).
    ///
    /// `.await` the builder for all keys at once, or call
    /// [`stream()`](ListBuilder::stream) for an incremental, ordered stream.
    pub fn list(
        &self,
        min_key_inclusive: impl Into<String>,
        max_key_exclusive: impl Into<String>,
    ) -> ListBuilder {
        ListBuilder::new(
            self.clone(),
            min_key_inclusive.into(),
            max_key_exclusive.into(),
        )
    }

    /// Reads every record with `min_key_inclusive <= key < max_key_exclusive`
    /// (in Oxia's slash-aware key order).
    ///
    /// `.await` the builder for all records at once, or call
    /// [`stream()`](RangeScanBuilder::stream) for an incremental, ordered
    /// stream with bounded memory use.
    pub fn range_scan(
        &self,
        min_key_inclusive: impl Into<String>,
        max_key_exclusive: impl Into<String>,
    ) -> RangeScanBuilder {
        RangeScanBuilder::new(
            self.clone(),
            min_key_inclusive.into(),
            max_key_exclusive.into(),
        )
    }

    /// Subscribes to all changes applied to the database, streamed as
    /// [`Notification`](crate::Notification)s.
    pub fn notifications(&self) -> NotificationsBuilder {
        NotificationsBuilder::new(self.clone())
    }

    /// Subscribes to updates of the sequence rooted at `key`, delivering the
    /// highest assigned sequence key as it advances. `partition_key` must be
    /// the partition key the sequential records are written with.
    pub fn sequence_updates(
        &self,
        key: impl Into<String>,
        partition_key: impl Into<String>,
    ) -> SequenceUpdatesBuilder {
        SequenceUpdatesBuilder::new(self.clone(), key.into(), partition_key.into())
    }

    /// Closes the client gracefully: pending batches are flushed, subscriptions
    /// stop, sessions are closed server-side (removing this client's ephemeral
    /// records), and connections are released.
    ///
    /// Idempotent: subsequent calls (from any clone) return `Ok(())`.
    /// Operations submitted after `close` fail with [`OxiaError::Closed`].
    pub async fn close(&self) -> Result<(), OxiaError> {
        if self.inner.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let mut first_err: Option<OxiaError> = None;

        // Flush and stop the batchers (writes first, so queued mutations land).
        for batchers in [&self.inner.write_batchers, &self.inner.read_batchers] {
            let shards: Vec<i64> = batchers.iter().map(|entry| *entry.key()).collect();
            for shard in shards {
                if let Some((_, batcher)) = batchers.remove(&shard) {
                    track(&mut first_err, batcher.close().await, "batcher");
                }
            }
        }
        for manager in self.inner.notification_tasks.lock().await.drain(..) {
            track(&mut first_err, manager.shutdown().await, "notifications");
        }
        for manager in self.inner.sequence_tasks.lock().await.drain(..) {
            track(&mut first_err, manager.shutdown().await, "sequence updates");
        }
        track(
            &mut first_err,
            self.inner.write_stream_manager.close().await,
            "write streams",
        );
        track(
            &mut first_err,
            self.inner.session_manager.close().await,
            "sessions",
        );
        track(
            &mut first_err,
            self.inner.shard_manager.close().await,
            "shard manager",
        );
        self.inner.provider_manager.clear();
        match first_err {
            None => Ok(()),
            Some(err) => Err(err),
        }
    }

    // ---- internals shared by the request builders -------------------------

    pub(crate) fn ensure_open(&self) -> Result<(), OxiaError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(OxiaError::Closed);
        }
        Ok(())
    }

    pub(crate) fn request_timeout(&self) -> Duration {
        self.inner.options.request_timeout
    }

    pub(crate) fn identity(&self) -> &str {
        &self.inner.options.identity
    }

    /// Resolves the shard that owns `routing_key`.
    pub(crate) fn shard_for(&self, routing_key: &str) -> Result<i64, OxiaError> {
        self.inner
            .shard_manager
            .get_shard(routing_key)
            .ok_or_else(|| OxiaError::NoShardForKey {
                key: routing_key.to_string(),
            })
    }

    pub(crate) async fn session_id_for(&self, shard: i64) -> Result<i64, OxiaError> {
        self.inner.session_manager.get_session_id(shard).await
    }

    /// Returns (creating on first use) the batcher of the given kind for a shard.
    pub(crate) fn batcher(&self, kind: Batcher, shard_id: i64) -> Arc<BatchManager> {
        let map = match kind {
            Batcher::Read => &self.inner.read_batchers,
            Batcher::Write => &self.inner.write_batchers,
        };
        map.entry(shard_id)
            .or_insert_with(|| {
                Arc::new(BatchManager::new(
                    kind.clone(),
                    BatcherContext {
                        shard_id,
                        shard_manager: self.inner.shard_manager.clone(),
                        provider_manager: self.inner.provider_manager.clone(),
                        write_stream_manager: self.inner.write_stream_manager.clone(),
                        batch_linger: self.inner.options.batch_linger,
                        max_requests_per_batch: self.inner.options.max_requests_per_batch,
                        request_timeout: self.inner.options.request_timeout,
                    },
                ))
            })
            .clone()
    }

    /// Submits an operation to a shard's batcher and awaits its response,
    /// bounded by the request timeout.
    pub(crate) async fn submit<Req, Resp>(
        &self,
        kind: Batcher,
        shard_id: i64,
        request: Req,
        wrap: fn(Pending<Req, Resp>) -> Operation,
    ) -> Result<Resp, OxiaError> {
        let (pending, rx) = Pending::new(request);
        self.batcher(kind, shard_id).add(wrap(pending))?;
        self.await_response(rx).await
    }

    pub(crate) async fn await_response<Resp>(
        &self,
        rx: oneshot::Receiver<Result<Resp, OxiaError>>,
    ) -> Result<Resp, OxiaError> {
        match tokio::time::timeout(self.request_timeout(), rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                if self.inner.closed.load(Ordering::Acquire) {
                    Err(OxiaError::Closed)
                } else {
                    Err(OxiaError::Disconnected(
                        "operation was dropped without a response".to_string(),
                    ))
                }
            }
            Err(_) => Err(OxiaError::Timeout),
        }
    }

    /// Runs a get against every shard and reduces the per-shard winners
    /// according to the comparison type (used when no partition key pins the
    /// query to one shard).
    pub(crate) async fn broadcast_get(
        &self,
        request: proto::GetRequest,
        comparison: ComparisonType,
    ) -> Result<GetResult, OxiaError> {
        let mut join_set = JoinSet::new();
        for shard in self.inner.shard_manager.get_shard_ids() {
            let client = self.clone();
            let request = request.clone();
            join_set.spawn(async move {
                let requested_key = request.key.clone();
                let response = client
                    .submit(Batcher::Read, shard, request, Operation::Get)
                    .await?;
                check_status(response.status)?;
                GetResult::from_proto(response, Some(requested_key))
            });
        }
        let mut best: Option<GetResult> = None;
        let mut join_error: Option<OxiaError> = None;
        while let Some(joined) = join_set.join_next().await {
            let result = match joined {
                Ok(result) => result,
                Err(err) => {
                    join_error = Some(OxiaError::Disconnected(err.to_string()));
                    continue;
                }
            };
            match result {
                Ok(candidate) => {
                    best = Some(match best.take() {
                        None => candidate,
                        Some(current) => pick_get_winner(current, candidate, comparison),
                    })
                }
                // Not every shard has a matching record.
                Err(OxiaError::KeyNotFound) => {}
                Err(err) => return Err(err),
            }
        }
        if let Some(err) = join_error {
            return Err(err);
        }
        best.ok_or(OxiaError::KeyNotFound)
    }

    /// Resolves the target shards for a list/scan: either the single shard
    /// owning `partition_key`, or all shards.
    fn target_shards(&self, partition_key: Option<&str>) -> Result<Vec<i64>, OxiaError> {
        match partition_key {
            Some(pk) => Ok(vec![self.shard_for(pk)?]),
            None => Ok(self.inner.shard_manager.get_shard_ids()),
        }
    }

    pub(crate) async fn open_list_stream(
        &self,
        request: proto::ListRequest,
        partition_key: Option<&str>,
    ) -> Result<ListStream, OxiaError> {
        self.ensure_open()?;
        let opens = self.target_shards(partition_key)?.into_iter().map(|shard| {
            let client = self.clone();
            let mut request = request.clone();
            async move {
                request.shard = Some(shard);
                let mut provider = client.leader_provider(shard).await?;
                let streaming = provider.list(request).await?.into_inner();
                let stream: ShardStream<String> = Box::pin(
                    streaming
                        .map(|chunk| match chunk {
                            Ok(response) => response.keys.into_iter().map(Ok).collect(),
                            Err(status) => vec![Err(OxiaError::from(status))],
                        })
                        .flat_map(futures::stream::iter),
                );
                Ok::<_, OxiaError>(stream)
            }
        });
        Ok(ListStream::new(merged(try_join_all(opens).await?)))
    }

    pub(crate) async fn open_range_scan_stream(
        &self,
        request: proto::RangeScanRequest,
        partition_key: Option<&str>,
    ) -> Result<RangeScanStream, OxiaError> {
        self.ensure_open()?;
        let opens = self.target_shards(partition_key)?.into_iter().map(|shard| {
            let client = self.clone();
            let mut request = request.clone();
            async move {
                request.shard = Some(shard);
                let mut provider = client.leader_provider(shard).await?;
                let streaming = provider.range_scan(request).await?.into_inner();
                let stream: ShardStream<GetResult> = Box::pin(
                    streaming
                        .map(|chunk| match chunk {
                            Ok(response) => response
                                .records
                                .into_iter()
                                .map(|record| GetResult::from_proto(record, None))
                                .collect(),
                            Err(status) => vec![Err(OxiaError::from(status))],
                        })
                        .flat_map(futures::stream::iter),
                );
                Ok::<_, OxiaError>(stream)
            }
        });
        Ok(RangeScanStream::new(merged(try_join_all(opens).await?)))
    }

    async fn leader_provider(
        &self,
        shard: i64,
    ) -> Result<proto::oxia_client_client::OxiaClientClient<tonic::transport::Channel>, OxiaError>
    {
        let leader = self
            .inner
            .shard_manager
            .get_leader(shard)
            .ok_or(OxiaError::LeaderNotFound { shard })?;
        self.inner
            .provider_manager
            .get_provider(leader.service_address)
            .await
    }

    pub(crate) async fn open_notifications(
        &self,
        buffer_size: usize,
    ) -> Result<Notifications, OxiaError> {
        self.ensure_open()?;
        let (tx, rx) = mpsc::channel(buffer_size);
        let manager = NotificationManager::new(
            self.inner.shard_manager.clone(),
            self.inner.provider_manager.clone(),
            tx,
        );
        self.inner.notification_tasks.lock().await.push(manager);
        Ok(Notifications::new(rx))
    }

    pub(crate) async fn open_sequence_updates(
        &self,
        key: String,
        partition_key: String,
        buffer_size: usize,
    ) -> Result<SequenceUpdates, OxiaError> {
        self.ensure_open()?;
        let (tx, rx) = mpsc::channel(buffer_size);
        let manager = SequenceUpdatesManager::new(
            key,
            partition_key,
            self.inner.shard_manager.clone(),
            self.inner.provider_manager.clone(),
            tx,
        );
        self.inner.sequence_tasks.lock().await.push(manager);
        Ok(SequenceUpdates::new(rx))
    }

    /// Broadcasts a delete-range to every shard.
    pub(crate) async fn broadcast_delete_range(
        &self,
        request: proto::DeleteRangeRequest,
    ) -> Result<(), OxiaError> {
        let mut join_set = JoinSet::new();
        for shard in self.inner.shard_manager.get_shard_ids() {
            let client = self.clone();
            let request = request.clone();
            join_set.spawn(async move {
                let response = client
                    .submit(Batcher::Write, shard, request, Operation::DeleteRange)
                    .await?;
                check_status(response.status)
            });
        }
        while let Some(joined) = join_set.join_next().await {
            joined.map_err(|err| OxiaError::Disconnected(err.to_string()))??;
        }
        Ok(())
    }
}

fn track(first_err: &mut Option<OxiaError>, result: Result<(), OxiaError>, what: &str) {
    if let Err(err) = result {
        warn!("error while closing {what}: {err}");
        first_err.get_or_insert(err);
    }
}

/// Reduces two per-shard get results to the winner for the comparison type.
fn pick_get_winner(a: GetResult, b: GetResult, comparison: ComparisonType) -> GetResult {
    let a_wins = match comparison {
        // The smallest matching key wins.
        ComparisonType::Equal | ComparisonType::Ceiling | ComparisonType::Higher => {
            a.compare_keys(&b).is_le()
        }
        // The largest matching key wins.
        ComparisonType::Floor | ComparisonType::Lower => a.compare_keys(&b).is_ge(),
    };
    if a_wins { a } else { b }
}

/// Maps a wire-level status to the domain error.
pub(crate) fn check_status(status: i32) -> Result<(), OxiaError> {
    match proto::Status::try_from(status) {
        Ok(proto::Status::Ok) => Ok(()),
        Ok(proto::Status::KeyNotFound) => Err(OxiaError::KeyNotFound),
        Ok(proto::Status::UnexpectedVersionId) => Err(OxiaError::UnexpectedVersionId),
        Ok(proto::Status::SessionDoesNotExist) => Err(OxiaError::SessionExpired),
        Err(err) => Err(OxiaError::Decode(format!("invalid status code: {err}"))),
    }
}
