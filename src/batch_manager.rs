use crate::batch::{Batch, ReadBatch, WriteBatch};
use crate::errors::OxiaError;
use crate::errors::OxiaError::UnexpectedStatus;
use crate::operations::Operation;
use crate::provider_manager::ProviderManager;
use crate::shard_manager::ShardManager;
use crate::write_stream_manager::WriteStreamManager;
use log::info;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;

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
    ) -> Batch {
        match self {
            Batcher::Read => Batch::Read(ReadBatch::new(shard_id, shard_manager, provider_manager)),
            Batcher::Write => Batch::Write(WriteBatch::new(shard_id, write_stream_manager)),
        }
    }
}

pub struct BatchManager {
    context: CancellationToken,
    tx: UnboundedSender<Operation>,
    batch_handle: JoinHandle<()>,
}

impl BatchManager {
    pub fn new(
        shard_id: i64,
        batcher: Batcher,
        shard_manager: Arc<ShardManager>,
        provider_manager: Arc<ProviderManager>,
        write_stream_manager: Arc<WriteStreamManager>,
        batch_linger: Duration,
        batch_max_size: u32,
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
            batch_max_size,
        ));
        BatchManager {
            context,
            tx,
            batch_handle: handle,
        }
    }

    pub fn add(&self, operation: Operation) -> Result<(), OxiaError> {
        self.tx
            .send(operation)
            .map_err(|err| UnexpectedStatus(err.to_string()))?;
        Ok(())
    }

    pub async fn shutdown(self) -> Result<(), OxiaError> {
        self.context.cancel();
        self.batch_handle
            .await
            .map_err(|err| UnexpectedStatus(err.to_string()))?;
        Ok(())
    }
}

async fn start_batcher(
    context: CancellationToken,
    mut rx: UnboundedReceiver<Operation>,
    shard_id: i64,
    batcher: Batcher,
    shard_manager: Arc<ShardManager>,
    provider_manager: Arc<ProviderManager>,
    write_stream_manager: Arc<WriteStreamManager>,
    batch_linger: Duration,
    _batch_max_size: u32,
) {
    let mut buffer = Vec::new();
    let mut batch = batcher.create_batch(
        shard_id,
        shard_manager.clone(),
        provider_manager.clone(),
        write_stream_manager.clone(),
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
                batch = batcher.create_batch( shard_id, shard_manager.clone(), provider_manager.clone(), write_stream_manager.clone());
            }
            size = rx.recv_many(&mut buffer, usize::MAX) => {
                if size == 0 {
                    info!("Exit batcher due to channel closed.");
                    return
                }
                for operation  in buffer.drain(..) {
                    if !batch.can_add(&operation) {
                        batch.flush().await;
                        batch = batcher.create_batch( shard_id, shard_manager.clone(), provider_manager.clone(), write_stream_manager.clone());
                        interval.reset();
                    }
                    batch.add(operation);
                }
            }
        }
    }
}
