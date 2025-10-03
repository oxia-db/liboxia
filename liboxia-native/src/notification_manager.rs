use crate::client::Notification;
use crate::errors::OxiaError;
use crate::errors::OxiaError::{ShardLeaderNotFound, UnexpectedStatus};
use crate::oxia::NotificationsRequest;
use crate::provider_manager::ProviderManager;
use crate::shard_manager::ShardManager;
use backoff::{Error, ExponentialBackoff};
use dashmap::DashMap;
use futures::TryFutureExt;
use log::{info, warn};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{atomic, Arc};
use task::JoinHandle;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;
use tokio::task;
use tokio_util::sync::CancellationToken;
use tonic::codegen::tokio_stream::StreamExt;

struct NotificationListener {
    shard_id: i64,
    context: CancellationToken,
    start_offset: Arc<AtomicI64>,
    join_handle: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for NotificationListener {
    fn drop(&mut self) {
        self.context.cancel();
    }
}

impl NotificationListener {
    pub fn new(
        shard_id: i64,
        shard_manager: Arc<ShardManager>,
        provider_manager: Arc<ProviderManager>,
        sender: Sender<Notification>,
    ) -> Self {
        let context = CancellationToken::new();
        let start_offset = Arc::new(atomic::AtomicI64::new(-1));

        let passing_context = context.clone();
        let passing_start_offset = start_offset.clone();
        let handle = tokio::spawn(start_notification_listener(
            shard_id,
            shard_manager,
            provider_manager,
            sender,
            passing_context,
            passing_start_offset,
        ));
        NotificationListener {
            shard_id,
            context,
            start_offset,
            join_handle: Mutex::new(Some(handle)),
        }
    }
    pub async fn shutdown(self) -> Result<(), OxiaError> {
        self.context.cancel();
        let mut handle_guard = self.join_handle.lock().await;
        if let Some(handle) = handle_guard.take() {
            handle
                .await
                .map_err(|err| UnexpectedStatus(err.to_string()))?;
        }
        Ok(())
    }
}

async fn start_notification_listener(
    shard_id: i64,
    shard_manager: Arc<ShardManager>,
    provider_manager: Arc<ProviderManager>,
    sender: Sender<Notification>,
    passing_context: CancellationToken,
    passing_start_offset: Arc<AtomicI64>,
) {
    let defer = || {
        let context = passing_context.clone();
        let offset_ref = passing_start_offset.clone();
        let shard_manager = shard_manager.clone();
        let provider_manager = provider_manager.clone();
        let sender = sender.clone();
        async move {
            let provider = match shard_manager.get_leader(shard_id) {
                None => Err(ShardLeaderNotFound(shard_id)),
                Some(leader) => Ok(provider_manager
                    .get_provider(leader.service_address)
                    .await?),
            }?;
            let mut provider_guard = provider.lock().await;
            let mut streaming = provider_guard
                .get_notifications(NotificationsRequest {
                    shard: shard_id,
                    start_offset_exclusive: Some(offset_ref.load(Ordering::Acquire)),
                })
                .map_err(|err| UnexpectedStatus(err.to_string()))
                .await?
                .into_inner();
            drop(provider_guard);
            loop {
                tokio::select! {
                     _ = context.cancelled() => {
                    info!("Close notification listener due to context canceled. shard_id={:?}", shard_id);
                },
                message = streaming.next() => match message {
                        None => {
                            return Err(Error::transient(UnexpectedStatus(String::from( "streaming has closed by server", )))); }
                        Some(notification) => {
                            let batch = notification.map_err(|err| UnexpectedStatus(err.to_string()))?;
                            offset_ref.store(batch.offset, Ordering::Release);
                            for notification_tuple in batch.notifications {
                                    sender
                                        .send(notification_tuple.into())
                                        .await
                                        .map_err(|err| UnexpectedStatus(err.to_string()))?;
                            }
                    }
                }
                }
            }
            Ok(())
        }
    };
    let notify = |err, duration| {
        warn!(
            "Transient failure when listen notification. error: {:?} retry-after: {:?}.",
            err, duration
        );
    };
    let backoff = ExponentialBackoff::default();
    let _ = backoff::future::retry_notify(backoff, defer, notify).await;
}

pub struct NotificationManager {
    shard_manager: Arc<ShardManager>,
    provider_manager: Arc<ProviderManager>,

    listener: DashMap<i64, NotificationListener>,
}

impl NotificationManager {
    pub fn new(
        shard_manager: Arc<ShardManager>,
        provider_manager: Arc<ProviderManager>,
        sender: Sender<Notification>,
    ) -> Self {
        let manager = Self {
            shard_manager: shard_manager.clone(),
            provider_manager: provider_manager.clone(),
            listener: DashMap::new(),
        };
        for (shard_id, node) in shard_manager.get_shards_leader() {
            let shard_sender = sender.clone();
            let listener = NotificationListener::new(
                shard_id,
                shard_manager.clone(),
                provider_manager.clone(),
                shard_sender,
            );
            manager.listener.insert(shard_id, listener);
        }
        manager
    }

    pub async fn shutdown(self) -> Result<(), OxiaError> {
        for (_, listener) in self.listener.into_iter() {
            listener.shutdown().await?
        }
        Ok(())
    }
}
