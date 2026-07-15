use crate::batch::{Batch, ReadBatch, WriteBatch};
use crate::errors::OxiaError;
use crate::operations::Operation;
use crate::provider_manager::ProviderManager;
use crate::shard_manager::ShardManager;
use crate::write_stream_manager::WriteStreamManager;
use log::info;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub enum Batcher {
    Read,
    Write,
}

impl Batcher {
    fn create_batch(
        &self,
        shard_id: i64,
        shard_manager: Arc<ShardManager>,
        provider_manager: Arc<ProviderManager>,
        write_stream_manager: Arc<WriteStreamManager>,
        max_requests_per_batch: u32,
    ) -> Batch {
        match self {
            Batcher::Read => Batch::Read(ReadBatch::new(
                shard_id,
                shard_manager,
                provider_manager,
                max_requests_per_batch,
            )),
            Batcher::Write => Batch::Write(WriteBatch::new(
                shard_id,
                write_stream_manager,
                max_requests_per_batch,
            )),
        }
    }
}

pub struct BatchManager {
    context: CancellationToken,
    tx: UnboundedSender<Operation>,
    batch_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl Drop for BatchManager {
    fn drop(&mut self) {
        self.context.cancel();
    }
}

impl BatchManager {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        shard_id: i64,
        batcher: Batcher,
        shard_manager: Arc<ShardManager>,
        provider_manager: Arc<ProviderManager>,
        write_stream_manager: Arc<WriteStreamManager>,
        batch_linger: Duration,
        _batch_max_size: u32,
        max_requests_per_batch: u32,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let context = CancellationToken::new();
        let handle = tokio::spawn(start_batcher(
            context.clone(),
            rx,
            shard_id,
            batcher,
            shard_manager,
            provider_manager,
            write_stream_manager,
            batch_linger,
            max_requests_per_batch,
        ));
        BatchManager {
            context,
            tx,
            batch_handle: Arc::new(Mutex::new(Some(handle))),
        }
    }

    pub fn add(&self, operation: Operation) -> Result<(), OxiaError> {
        // The batcher's receiver is only dropped when the batcher has been shut
        // down, so a send failure means the client (or this shard's batcher) is
        // gone.
        self.tx.send(operation).map_err(|_| OxiaError::Closed)?;
        Ok(())
    }

    pub async fn shutdown(self) -> Result<(), OxiaError> {
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

#[allow(clippy::too_many_arguments)]
async fn start_batcher(
    context: CancellationToken,
    mut rx: UnboundedReceiver<Operation>,
    shard_id: i64,
    batcher: Batcher,
    shard_manager: Arc<ShardManager>,
    provider_manager: Arc<ProviderManager>,
    write_stream_manager: Arc<WriteStreamManager>,
    batch_linger: Duration,
    max_requests_per_batch: u32,
) {
    let mut buffer = Vec::new();
    let mut batch = batcher.create_batch(
        shard_id,
        shard_manager.clone(),
        provider_manager.clone(),
        write_stream_manager.clone(),
        max_requests_per_batch,
    );
    let mut interval = interval(batch_linger);
    loop {
        tokio::select! {
             _ = context.cancelled() => {
                info!("Exit batcher due to context canceled.");
                return
            },
            _ = interval.tick() => {
                if batch.is_empty() {
                   continue
                }
                batch.flush().await;
                batch = batcher.create_batch( shard_id, shard_manager.clone(), provider_manager.clone(), write_stream_manager.clone(), max_requests_per_batch);
            }
            size = rx.recv_many(&mut buffer, usize::MAX) => {
                if size == 0 {
                    info!("Exit batcher due to channel closed.");
                    return
                }
                for operation  in buffer.drain(..) {
                    if !batch.can_add(&operation) {
                        batch.flush().await;
                        batch = batcher.create_batch( shard_id, shard_manager.clone(), provider_manager.clone(), write_stream_manager.clone(), max_requests_per_batch);
                        interval.reset();
                    }
                    batch.add(operation);
                }
            }
        }
    }
}
