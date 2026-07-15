//! Accumulates pending operations for one shard and flushes them as a single
//! batched request. Requests are moved (not cloned) into the outgoing batch;
//! callbacks are kept and completed in order from the response.

use crate::errors::OxiaError;
use crate::operations::{Operation, PendingDelete, PendingDeleteRange, PendingGet, PendingPut};
use crate::proto::{ReadRequest, ReadResponse, WriteRequest};
use crate::provider_manager::ProviderManager;
use crate::shard_manager::ShardManager;
use crate::write_stream_manager::WriteStreamManager;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tonic::codegen::tokio_stream::StreamExt;
use tonic::{Request, Streaming};
use tracing::warn;

pub(crate) enum Batch {
    Read(ReadBatch),
    Write(WriteBatch),
}

impl Batch {
    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Batch::Read(read_batch) => read_batch.gets.is_empty(),
            Batch::Write(write_batch) => write_batch.is_empty(),
        }
    }

    pub(crate) fn can_add(&self, operation: &Operation) -> bool {
        let _ = operation;
        match self {
            Batch::Read(read_batch) => (read_batch.gets.len() as u32) < read_batch.max_requests,
            Batch::Write(write_batch) => write_batch.total_count() < write_batch.max_requests,
        }
    }

    /// Adds an operation. `Flush` markers are handled by the batcher loop and
    /// must not reach here.
    pub(crate) fn add(&mut self, operation: Operation) {
        match (self, operation) {
            (Batch::Read(read_batch), Operation::Get(get)) => read_batch.gets.push(get),
            (Batch::Write(write_batch), Operation::Put(put)) => write_batch.puts.push(put),
            (Batch::Write(write_batch), Operation::Delete(delete)) => {
                write_batch.deletes.push(delete)
            }
            (Batch::Write(write_batch), Operation::DeleteRange(dr)) => {
                write_batch.delete_ranges.push(dr)
            }
            _ => warn!("operation routed to the wrong batcher; dropping"),
        }
    }

    pub(crate) async fn flush(&mut self) {
        match self {
            Batch::Read(read_batch) => read_batch.flush().await,
            Batch::Write(write_batch) => write_batch.flush().await,
        }
    }
}

fn fail_all<Resp>(
    callbacks: impl IntoIterator<Item = oneshot::Sender<Result<Resp, OxiaError>>>,
    err: &OxiaError,
) {
    for callback in callbacks {
        let _ = callback.send(Err(err.clone()));
    }
}

/// Completes `callbacks` in order from `responses`; leftovers on either side
/// indicate a server bug and fail with a decode error.
fn complete_all<Resp>(
    callbacks: impl IntoIterator<Item = oneshot::Sender<Result<Resp, OxiaError>>>,
    responses: impl IntoIterator<Item = Resp>,
    what: &str,
) {
    let mut responses = responses.into_iter();
    for callback in callbacks {
        match responses.next() {
            Some(response) => {
                let _ = callback.send(Ok(response));
            }
            None => {
                let _ = callback.send(Err(OxiaError::Decode(format!(
                    "missing {what} response from server"
                ))));
            }
        }
    }
    if responses.next().is_some() {
        warn!("server returned more {what} responses than requested");
    }
}

pub(crate) struct ReadBatch {
    gets: Vec<PendingGet>,
    shard_id: i64,
    shard_manager: Arc<ShardManager>,
    provider_manager: Arc<ProviderManager>,
    max_requests: u32,
    request_timeout: Duration,
}

impl ReadBatch {
    pub(crate) fn new(
        shard_id: i64,
        shard_manager: Arc<ShardManager>,
        provider_manager: Arc<ProviderManager>,
        max_requests: u32,
        request_timeout: Duration,
    ) -> Self {
        ReadBatch {
            gets: Vec::new(),
            shard_id,
            shard_manager,
            provider_manager,
            max_requests,
            request_timeout,
        }
    }

    async fn flush(&mut self) {
        let (requests, callbacks): (Vec<_>, Vec<_>) =
            self.gets.drain(..).map(|p| (p.request, p.callback)).unzip();

        let node = match self.shard_manager.get_leader(self.shard_id) {
            Some(node) => node,
            None => {
                fail_all(
                    callbacks,
                    &OxiaError::LeaderNotFound {
                        shard: self.shard_id,
                    },
                );
                return;
            }
        };
        let mut provider = match self
            .provider_manager
            .get_provider(node.service_address)
            .await
        {
            Ok(provider) => provider,
            Err(err) => {
                fail_all(callbacks, &err);
                return;
            }
        };

        let mut request = Request::new(ReadRequest {
            shard: Some(self.shard_id),
            gets: requests,
        });
        request.set_timeout(self.request_timeout);
        match provider.read(request).await {
            Ok(response) => receive_all(response.into_inner(), callbacks).await,
            Err(status) => fail_all(callbacks, &OxiaError::from(status)),
        }
    }
}

async fn receive_all(
    mut streaming: Streaming<ReadResponse>,
    callbacks: Vec<oneshot::Sender<Result<crate::proto::GetResponse, OxiaError>>>,
) {
    let mut callbacks = callbacks.into_iter();
    while let Some(next) = streaming.next().await {
        match next {
            Ok(read_response) => {
                for get_response in read_response.gets {
                    match callbacks.next() {
                        Some(callback) => {
                            let _ = callback.send(Ok(get_response));
                        }
                        None => {
                            warn!("server returned more get responses than requested");
                            return;
                        }
                    }
                }
            }
            Err(status) => {
                fail_all(callbacks, &OxiaError::from(status));
                return;
            }
        }
    }
    // The stream ended before every get was answered.
    fail_all(
        callbacks,
        &OxiaError::Decode("missing get response from server".to_string()),
    );
}

pub(crate) struct WriteBatch {
    shard_id: i64,
    puts: Vec<PendingPut>,
    deletes: Vec<PendingDelete>,
    delete_ranges: Vec<PendingDeleteRange>,
    write_stream_manager: Arc<WriteStreamManager>,
    max_requests: u32,
}

impl WriteBatch {
    pub(crate) fn new(
        shard_id: i64,
        write_stream_manager: Arc<WriteStreamManager>,
        max_requests: u32,
    ) -> Self {
        WriteBatch {
            shard_id,
            puts: Vec::new(),
            deletes: Vec::new(),
            delete_ranges: Vec::new(),
            write_stream_manager,
            max_requests,
        }
    }

    fn is_empty(&self) -> bool {
        self.puts.is_empty() && self.deletes.is_empty() && self.delete_ranges.is_empty()
    }

    fn total_count(&self) -> u32 {
        (self.puts.len() + self.deletes.len() + self.delete_ranges.len()) as u32
    }

    async fn flush(&mut self) {
        let (put_reqs, put_cbs): (Vec<_>, Vec<_>) =
            self.puts.drain(..).map(|p| (p.request, p.callback)).unzip();
        let (delete_reqs, delete_cbs): (Vec<_>, Vec<_>) = self
            .deletes
            .drain(..)
            .map(|p| (p.request, p.callback))
            .unzip();
        let (dr_reqs, dr_cbs): (Vec<_>, Vec<_>) = self
            .delete_ranges
            .drain(..)
            .map(|p| (p.request, p.callback))
            .unzip();

        let request = WriteRequest {
            shard: Some(self.shard_id),
            puts: put_reqs,
            deletes: delete_reqs,
            delete_ranges: dr_reqs,
        };
        match self.write_stream_manager.write(request).await {
            Ok(response) => {
                complete_all(put_cbs, response.puts, "put");
                complete_all(delete_cbs, response.deletes, "delete");
                complete_all(dr_cbs, response.delete_ranges, "delete_range");
            }
            Err(err) => {
                fail_all(put_cbs, &err);
                fail_all(delete_cbs, &err);
                fail_all(dr_cbs, &err);
            }
        }
    }
}
