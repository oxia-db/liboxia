use crate::errors::OxiaError;
use crate::oxia::{WriteRequest, WriteResponse};
use crate::provider_manager::ProviderManager;
use crate::shard_manager::ShardManager;
use crate::write_stream::WriteStream;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tonic::codegen::tokio_stream::wrappers::UnboundedReceiverStream;

pub struct WriteStreamManager {
    shard_manager: Arc<ShardManager>,
    provider_manager: Arc<ProviderManager>,

    streams: Arc<DashMap<i64, WriteStream>>,
    stream_creation_lock: Arc<Mutex<()>>,
}

impl WriteStreamManager {
    pub async fn write(&self, request: WriteRequest) -> Result<WriteResponse, OxiaError> {
        let shard_id = request.shard.unwrap();
        let option = self.shard_manager.get_leader(shard_id);
        if option.is_none() {
            return Err(OxiaError::ShardLeaderNotFound(shard_id));
        }
        let client = self
            .provider_manager
            .get_provider(&option.unwrap().service_address)
            .await?;
        let (tx, rx) = mpsc::unbounded_channel();
        let mut client_guard = client.lock().await;
        let stream = WriteStream::wrap(
            tx,
            client_guard
                .write_stream(UnboundedReceiverStream::new(rx))
                .await?
                .into_inner(),
        );
        stream.send(request).await
    }

    pub fn new(shard_manager: Arc<ShardManager>, provider_manager: Arc<ProviderManager>) -> Self {
        WriteStreamManager {
            shard_manager,
            provider_manager,
            streams: Arc::new(DashMap::new()),
            stream_creation_lock: Arc::new(Mutex::new(())),
        }
    }
}
