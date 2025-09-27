use crate::errors::OxiaError;
use crate::oxia::{WriteRequest, WriteResponse};
use crate::provider_manager::ProviderManager;
use crate::shard_manager::ShardManager;
use crate::write_stream::WriteStream;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tonic::codegen::tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::Request;

const WRITE_STREAM_HEADER_NAMESPACE: &str = "namespace";
const WRITE_STREAM_HEADER_SHARD_ID: &str = "shard-id";

pub struct WriteStreamManager {
    namespace: String,
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
        let mut write_stream_request = Request::new(UnboundedReceiverStream::new(rx));
        let write_stream_request_metadata = write_stream_request.metadata_mut();
        write_stream_request_metadata.insert(
            WRITE_STREAM_HEADER_NAMESPACE,
            self.namespace.parse().unwrap(),
        );
        write_stream_request_metadata.insert(
            WRITE_STREAM_HEADER_SHARD_ID,
            shard_id.to_string().parse().unwrap(),
        );
        tx.send(request.clone()).unwrap();
        let streaming = client_guard
            .write_stream(write_stream_request)
            .await?
            .into_inner();
        let stream = WriteStream::wrap(tx, streaming);
        stream.send(request).await
    }

    pub fn new(
        namespace: String,
        shard_manager: Arc<ShardManager>,
        provider_manager: Arc<ProviderManager>,
    ) -> Self {
        WriteStreamManager {
            namespace,
            shard_manager,
            provider_manager,
            streams: Arc::new(DashMap::new()),
            stream_creation_lock: Arc::new(Mutex::new(())),
        }
    }
}
