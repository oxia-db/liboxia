//! Per-shard notification subscriptions for one [`Notifications`] handle.
//!
//! One listener task per shard streams that shard's change notifications into a
//! shared channel, reconnecting (resuming from the last delivered offset) after
//! connection loss. All listeners share one [`CancellationToken`], so dropping
//! the manager — which happens when the [`Notifications`] handle is dropped or
//! the client is closed — stops every listener promptly.
//!
//! Notifications from one shard are delivered in the order the server applied
//! them (the batch is a server-sorted `repeated NotificationEntry`); ordering
//! across shards is unspecified.
//!
//! [`Notifications`]: crate::Notifications

use crate::errors::OxiaError;
use crate::proto::NotificationsRequest;
use crate::provider_manager::ProviderManager;
use crate::retry::{RetryError, retry_until_cancelled};
use crate::shard_manager::ShardManager;
use crate::types::Notification;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use tokio::sync::Mutex;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::codegen::tokio_stream::StreamExt;
use tracing::{debug, warn};

pub(crate) struct NotificationManager {
    context: CancellationToken,
    handles: Mutex<Vec<JoinHandle<()>>>,
}

impl Drop for NotificationManager {
    fn drop(&mut self) {
        self.context.cancel();
    }
}

impl NotificationManager {
    /// Spawns a listener per shard. Each reports once, via `init_tx`, whether it
    /// established its first subscription (`Ok`) or hit a permanent error
    /// (`Err`); the caller waits on these to make the subscription live before
    /// returning to the user.
    pub(crate) fn new(
        shards: Vec<i64>,
        shard_manager: Arc<ShardManager>,
        provider_manager: Arc<ProviderManager>,
        sender: Sender<Notification>,
        init_tx: tokio::sync::mpsc::Sender<Result<(), OxiaError>>,
    ) -> Self {
        let context = CancellationToken::new();
        let handles = shards
            .into_iter()
            .map(|shard_id| {
                tokio::spawn(start_notification_listener(
                    shard_id,
                    shard_manager.clone(),
                    provider_manager.clone(),
                    sender.clone(),
                    context.clone(),
                    init_tx.clone(),
                ))
            })
            .collect();
        NotificationManager {
            context,
            handles: Mutex::new(handles),
        }
    }

    pub(crate) async fn shutdown(self) -> Result<(), OxiaError> {
        self.context.cancel();
        for handle in self.handles.lock().await.drain(..) {
            handle
                .await
                .map_err(|err| OxiaError::Disconnected(err.to_string()))?;
        }
        Ok(())
    }
}

async fn start_notification_listener(
    shard_id: i64,
    shard_manager: Arc<ShardManager>,
    provider_manager: Arc<ProviderManager>,
    sender: Sender<Notification>,
    context: CancellationToken,
    init_tx: tokio::sync::mpsc::Sender<Result<(), OxiaError>>,
) {
    // The last offset delivered by the server, tracked across reconnects. A
    // negative value means "not yet positioned": the first request omits the
    // offset (`None`), which the server answers with an initial empty batch
    // that primes the notification cursor. Reconnects resume from the last
    // offset so nothing in the gap is missed.
    let last_offset = Arc::new(AtomicI64::new(-1));
    // Readiness is reported exactly once per listener; a reconnect must not
    // signal it again, or the init wait could over-count and return before a
    // slower shard is live.
    let reported = Arc::new(AtomicBool::new(false));

    let defer = || {
        let context = context.clone();
        let offset_ref = last_offset.clone();
        let reported = reported.clone();
        let shard_manager = shard_manager.clone();
        let provider_manager = provider_manager.clone();
        let sender = sender.clone();
        let init_tx = init_tx.clone();
        async move {
            let mut provider = match shard_manager.get_leader(shard_id) {
                None => {
                    return Err(RetryError::transient(OxiaError::LeaderNotFound {
                        shard: shard_id,
                    }));
                }
                Some(leader) => provider_manager
                    .get_provider(leader.service_address)
                    .await
                    .map_err(RetryError::transient)?,
            };
            let offset = offset_ref.load(Ordering::Acquire);
            let mut streaming = match provider
                .get_notifications(NotificationsRequest {
                    shard: shard_id,
                    start_offset_exclusive: (offset >= 0).then_some(offset),
                })
                .await
            {
                Ok(response) => response.into_inner(),
                Err(status) => {
                    let err = OxiaError::from(status);
                    // A permanent failure (e.g. notifications disabled server-
                    // side) is reported to the caller so the subscription call
                    // fails fast instead of retrying forever.
                    if err.is_retryable() {
                        return Err(RetryError::transient(err));
                    }
                    if !reported.swap(true, Ordering::AcqRel) {
                        let _ = init_tx.send(Err(err.clone())).await;
                    }
                    return Err(RetryError::fatal(err));
                }
            };
            loop {
                tokio::select! {
                    _ = context.cancelled() => {
                        debug!(shard = shard_id, "notification listener stopped: cancelled");
                        return Ok(());
                    }
                    message = streaming.next() => match message {
                        None => {
                            return Err(RetryError::transient(OxiaError::Disconnected(
                                "notification stream closed by server".to_string(),
                            )));
                        }
                        Some(notification) => {
                            let batch = notification
                                .map_err(|err| RetryError::transient(OxiaError::from(err)))?;
                            offset_ref.store(batch.offset, Ordering::Release);
                            // The first batch received confirms the subscription
                            // is live: it is the server's empty cursor-priming
                            // batch, so the loop below emits nothing for it.
                            if !reported.swap(true, Ordering::AcqRel) {
                                let _ = init_tx.send(Ok(())).await;
                            }
                            for entry in batch.notifications {
                                let Some(notification) = Notification::from_proto(entry) else {
                                    warn!("skipping unrecognized notification type");
                                    continue;
                                };
                                if sender.send(notification).await.is_err() {
                                    // The Notifications handle was dropped.
                                    return Ok(());
                                }
                            }
                        }
                    }
                }
            }
        }
    };
    retry_until_cancelled(&context, "notifications", defer).await;
}
