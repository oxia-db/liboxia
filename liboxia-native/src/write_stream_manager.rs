use crate::errors::OxiaError;
use crate::errors::OxiaError::{InternalRetryable, UnexpectedStatus};
use crate::oxia::{WriteRequest, WriteResponse};
use crate::provider_manager::ProviderManager;
use crate::shard_manager::ShardManager;
use crate::write_stream::WriteStream;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, OnceCell};
use tokio::task::JoinSet;
use tonic::codegen::tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::Request;

const WRITE_STREAM_HEADER_NAMESPACE: &str = "namespace";
const WRITE_STREAM_HEADER_SHARD_ID: &str = "shard-id";

pub struct WriteStreamManager {
    namespace: String,
    shard_manager: Arc<ShardManager>,
    provider_manager: Arc<ProviderManager>,

    streams: DashMap<i64, OnceCell<WriteStream>>,
}

impl WriteStreamManager {
    pub async fn write(&self, request: WriteRequest) -> Result<WriteResponse, OxiaError> {
        // todo: make request Rc
        loop {
            let result = self.write0(request.clone()).await;
            if result.is_ok() {
                return result;
            }
            if result.is_err() {
                let error = result.unwrap_err();
                match error {
                    InternalRetryable() => continue,
                    _ => return Err(error),
                }
            }
        }
    }

    async fn write0(&self, request: WriteRequest) -> Result<WriteResponse, OxiaError> {
        let shard_id = request.shard.unwrap();
        let option = self.shard_manager.get_leader(shard_id);
        if option.is_none() {
            return Err(OxiaError::ShardLeaderNotFound(shard_id));
        }
        let defer_init = || async {
            let mut provider = self
                .provider_manager
                .get_provider(option.unwrap().service_address)
                .await?;
            let (tx, rx) = mpsc::unbounded_channel();
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
            // https://github.com/hyperium/hyper/issues/3737
            // We should put the first message into the stream to trigger it to actually send.
            // otherwise, write_stream will hang forever.
            let w_stream = WriteStream::new(tx);
            w_stream.send_defer(request.clone()).await?;
            let streaming = provider
                .write_stream(write_stream_request)
                .await?
                .into_inner();
            w_stream.listen(streaming).await;
            Ok::<WriteStream, OxiaError>(w_stream)
        };
        let mut cell = self
            .streams
            .entry(shard_id)
            .or_insert_with(|| OnceCell::new());
        let initialized = cell.initialized();
        let w_stream = cell.get_or_try_init(defer_init).await?;
        if !w_stream.is_alive().await {
            if let Some(stream) = cell.take() {
                stream.shutdown().await?;
            }
            return Err(InternalRetryable());
        }
        if initialized {
            return w_stream.send(request).await;
        }
        w_stream.get_defer_response().await.unwrap()
    }

    pub async fn shutdown(self) -> Result<(), OxiaError> {
        let streams = self.streams;

        let mut joiner = JoinSet::new();
        for (_, stream_cell) in streams.into_iter() {
            if let Some(stream) = stream_cell.into_inner() {
                joiner.spawn(stream.shutdown());
            }
        }
        while let Some(result) = joiner.join_next().await {
            result.map_err(|err| {
                UnexpectedStatus(format!(
                    "write stream nanager background task failed to join: {}",
                    err
                ))
            })??;
        }
        Ok(())
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
            streams: DashMap::new(),
        }
    }
}
