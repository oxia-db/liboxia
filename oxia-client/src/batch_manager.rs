//! One batcher task per shard: accumulates operations and flushes them when the
//! batch fills up or the linger timer fires.

use crate::batch::{Batch, ReadBatch, WriteBatch};
use crate::errors::OxiaError;
use crate::operations::Operation;
use crate::provider_manager::ProviderManager;
use crate::shard_manager::ShardManager;
use crate::write_stream_manager::WriteStreamManager;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::debug;

#[derive(Clone)]
pub(crate) enum Batcher {
    Read,
    Write,
}

impl Batcher {
    fn create_batch(&self, ctx: &BatcherContext) -> Batch {
        match self {
            Batcher::Read => Batch::Read(ReadBatch::new(
                ctx.shard_id,
                ctx.shard_manager.clone(),
                ctx.provider_manager.clone(),
                ctx.max_requests_per_batch,
                ctx.request_timeout,
            )),
            Batcher::Write => Batch::Write(WriteBatch::new(
                ctx.shard_id,
                ctx.write_stream_manager.clone(),
                ctx.max_requests_per_batch,
            )),
        }
    }
}

pub(crate) struct BatcherContext {
    pub(crate) shard_id: i64,
    pub(crate) shard_manager: Arc<ShardManager>,
    pub(crate) provider_manager: Arc<ProviderManager>,
    pub(crate) write_stream_manager: Arc<WriteStreamManager>,
    pub(crate) batch_linger: Duration,
    pub(crate) max_requests_per_batch: u32,
    pub(crate) request_timeout: Duration,
}

pub(crate) struct BatchManager {
    context: CancellationToken,
    tx: UnboundedSender<Operation>,
    batch_handle: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for BatchManager {
    fn drop(&mut self) {
        self.context.cancel();
    }
}

impl BatchManager {
    pub(crate) fn new(batcher: Batcher, ctx: BatcherContext) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let context = CancellationToken::new();
        let handle = tokio::spawn(start_batcher(context.clone(), rx, batcher, ctx));
        BatchManager {
            context,
            tx,
            batch_handle: Mutex::new(Some(handle)),
        }
    }

    pub(crate) fn add(&self, operation: Operation) -> Result<(), OxiaError> {
        // The batcher's receiver is only dropped when the batcher has been shut
        // down, so a send failure means the client is closed.
        self.tx.send(operation).map_err(|_| OxiaError::Closed)?;
        Ok(())
    }

    /// Gracefully stops the batcher: everything already enqueued is flushed
    /// before the task exits.
    pub(crate) async fn close(&self) -> Result<(), OxiaError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        if self.tx.send(Operation::Flush(ack_tx)).is_ok() {
            // Wait for the batcher to drain everything queued before the marker.
            let _ = ack_rx.await;
        }
        self.context.cancel();
        let mut handle_guard = self.batch_handle.lock().await;
        if let Some(handle) = handle_guard.take() {
            handle
                .await
                .map_err(|err| OxiaError::Disconnected(err.to_string()))?;
        }
        Ok(())
    }
}

async fn start_batcher(
    context: CancellationToken,
    mut rx: UnboundedReceiver<Operation>,
    batcher: Batcher,
    ctx: BatcherContext,
) {
    let mut buffer = Vec::new();
    let mut batch = batcher.create_batch(&ctx);
    let mut interval = interval(ctx.batch_linger);
    loop {
        tokio::select! {
             _ = context.cancelled() => {
                debug!(shard = ctx.shard_id, "batcher stopped: cancelled");
                return
            },
            _ = interval.tick() => {
                if batch.is_empty() {
                   continue
                }
                batch.flush().await;
                batch = batcher.create_batch(&ctx);
            }
            size = rx.recv_many(&mut buffer, usize::MAX) => {
                if size == 0 {
                    // Channel closed: flush what we have, then exit.
                    if !batch.is_empty() {
                        batch.flush().await;
                    }
                    debug!(shard = ctx.shard_id, "batcher stopped: channel closed");
                    return
                }
                for operation in buffer.drain(..) {
                    if let Operation::Flush(ack) = operation {
                        if !batch.is_empty() {
                            batch.flush().await;
                            batch = batcher.create_batch(&ctx);
                        }
                        let _ = ack.send(());
                        continue;
                    }
                    if !batch.can_add(&operation) {
                        batch.flush().await;
                        batch = batcher.create_batch(&ctx);
                        interval.reset();
                    }
                    batch.add(operation);
                }
            }
        }
    }
}
