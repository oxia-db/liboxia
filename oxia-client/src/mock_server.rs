//! A programmable in-process Oxia server for hermetic client tests.
//!
//! Serves the real gRPC service on an ephemeral local port (no Docker, no
//! containers) and advertises itself as the single shard leader, so an
//! unmodified [`OxiaClient`](crate::OxiaClient) connects to it exactly as it
//! would to a real cluster. Tests script server behavior — delayed, missing,
//! erroneous, or absent responses; torn streams — and assert on what the
//! server observed (batch sizes, connection counts) and on what the client
//! surfaced.

use crate::proto;
use crate::proto::oxia_client_server::{OxiaClient as OxiaService, OxiaClientServer};
use futures::StreamExt;
use futures::channel::mpsc;
use futures::stream::BoxStream;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tonic::{Request, Response, Status, Streaming};

/// Scripted behavior of the write stream.
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum WriteMode {
    /// Acknowledge every operation successfully.
    Ok,
    /// Acknowledge successfully after the delay (holds the in-flight window
    /// open so later operations accumulate into sealed batches).
    DelayResponses(Duration),
    /// Receive requests but never respond.
    Hang,
    /// Respond to every request with an empty `WriteResponse`.
    OmitResponses,
    /// Respond to every put with this proto `Status` code.
    PutStatus(i32),
    /// Respond to every delete with this proto `Status` code.
    DeleteStatus(i32),
}

/// Scripted behavior of read RPCs.
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum ReadMode {
    /// Every get succeeds with value `b"mock-value"`.
    Ok,
    /// Accept the RPC but never produce a response.
    Hang,
    /// Respond to every get with this proto `Status` code.
    GetStatus(i32),
}

pub(crate) struct Behavior {
    pub(crate) write: WriteMode,
    /// Write-stream connections with index below this read one request and
    /// then die, leaving the batch's fate unknown (the client must requeue).
    pub(crate) torn_write_connections: u32,
    pub(crate) read: ReadMode,
    /// When set, the assignments stream fails with this gRPC status.
    pub(crate) assignments_error: Option<tonic::Code>,
}

impl Default for Behavior {
    fn default() -> Self {
        Behavior {
            write: WriteMode::Ok,
            torn_write_connections: 0,
            read: ReadMode::Ok,
            assignments_error: None,
        }
    }
}

struct State {
    behavior: Mutex<Behavior>,
    /// Operations per received `WriteRequest` (puts + deletes + delete-ranges).
    write_batch_sizes: Mutex<Vec<usize>>,
    write_connections: AtomicU32,
    /// Keeps assignment streams open for the lifetime of the mock.
    assignment_senders: Mutex<Vec<mpsc::UnboundedSender<Result<proto::ShardAssignments, Status>>>>,
}

/// A running mock server. Dropping it stops the server.
pub(crate) struct MockOxia {
    pub(crate) address: String,
    state: Arc<State>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for MockOxia {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl MockOxia {
    pub(crate) async fn start(behavior: Behavior) -> MockOxia {
        let state = Arc::new(State {
            behavior: Mutex::new(behavior),
            write_batch_sizes: Mutex::new(Vec::new()),
            write_connections: AtomicU32::new(0),
            assignment_senders: Mutex::new(Vec::new()),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock listener");
        let port = listener.local_addr().expect("mock local addr").port();
        let address = format!("127.0.0.1:{port}");

        let service = Service {
            state: state.clone(),
            advertised: address.clone(),
        };
        let incoming = futures::stream::unfold(listener, |listener| async move {
            let accepted = listener.accept().await.map(|(stream, _)| stream);
            Some((accepted, listener))
        });
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(OxiaClientServer::new(service))
                .serve_with_incoming(incoming)
                .await
                .expect("mock server failed");
        });

        MockOxia {
            address,
            state,
            server,
        }
    }

    pub(crate) fn set_read_mode(&self, read: ReadMode) {
        self.state.behavior.lock().expect("behavior lock").read = read;
    }

    /// Operations per `WriteRequest` the server has received, in order.
    pub(crate) fn write_batch_sizes(&self) -> Vec<usize> {
        self.state
            .write_batch_sizes
            .lock()
            .expect("sizes lock")
            .clone()
    }

    pub(crate) fn write_connections(&self) -> u32 {
        self.state.write_connections.load(Ordering::SeqCst)
    }
}

fn ok_version() -> proto::Version {
    proto::Version {
        version_id: 1,
        modifications_count: 0,
        created_timestamp: 1,
        modified_timestamp: 1,
        session_id: None,
        client_identity: None,
    }
}

fn respond_to(request: &proto::WriteRequest, mode: WriteMode) -> proto::WriteResponse {
    if mode == WriteMode::OmitResponses {
        return proto::WriteResponse::default();
    }
    let put_status = match mode {
        WriteMode::PutStatus(code) => code,
        _ => proto::Status::Ok as i32,
    };
    let delete_status = match mode {
        WriteMode::DeleteStatus(code) => code,
        _ => proto::Status::Ok as i32,
    };
    proto::WriteResponse {
        puts: request
            .puts
            .iter()
            .map(|_| proto::PutResponse {
                status: put_status,
                version: (put_status == proto::Status::Ok as i32).then(ok_version),
                key: None,
            })
            .collect(),
        deletes: request
            .deletes
            .iter()
            .map(|_| proto::DeleteResponse {
                status: delete_status,
            })
            .collect(),
        delete_ranges: request
            .delete_ranges
            .iter()
            .map(|_| proto::DeleteRangeResponse {
                status: proto::Status::Ok as i32,
            })
            .collect(),
    }
}

struct Service {
    state: Arc<State>,
    advertised: String,
}

type Rpc<T> = Result<Response<T>, Status>;

#[tonic::async_trait]
impl OxiaService for Service {
    type GetShardAssignmentsStream = BoxStream<'static, Result<proto::ShardAssignments, Status>>;

    async fn get_shard_assignments(
        &self,
        request: Request<proto::ShardAssignmentsRequest>,
    ) -> Rpc<Self::GetShardAssignmentsStream> {
        let scripted_error = self
            .state
            .behavior
            .lock()
            .expect("behavior lock")
            .assignments_error;
        if let Some(code) = scripted_error {
            return Err(Status::new(code, "scripted assignments failure"));
        }
        let namespace = request.into_inner().namespace;
        let assignments = proto::ShardAssignments {
            namespaces: [(
                namespace,
                proto::NamespaceShardsAssignment {
                    assignments: vec![proto::ShardAssignment {
                        shard: 0,
                        leader: self.advertised.clone(),
                        shard_boundaries: Some(
                            proto::shard_assignment::ShardBoundaries::Int32HashRange(
                                proto::Int32HashRange {
                                    min_hash_inclusive: 0,
                                    max_hash_inclusive: u32::MAX,
                                },
                            ),
                        ),
                    }],
                    shard_key_router: proto::ShardKeyRouter::Xxhash3 as i32,
                },
            )]
            .into(),
            allowed_authorities: vec![],
        };
        let (tx, rx) = mpsc::unbounded();
        tx.unbounded_send(Ok(assignments))
            .expect("first assignment");
        // Park the sender so the stream stays open, like a real server.
        self.state
            .assignment_senders
            .lock()
            .expect("senders lock")
            .push(tx);
        Ok(Response::new(rx.boxed()))
    }

    async fn write(&self, _: Request<proto::WriteRequest>) -> Rpc<proto::WriteResponse> {
        Err(Status::unimplemented("deprecated unary write"))
    }

    type WriteStreamStream = BoxStream<'static, Result<proto::WriteResponse, Status>>;

    async fn write_stream(
        &self,
        request: Request<Streaming<proto::WriteRequest>>,
    ) -> Rpc<Self::WriteStreamStream> {
        let connection = self.state.write_connections.fetch_add(1, Ordering::SeqCst);
        let state = self.state.clone();
        let (mode, torn) = {
            let behavior = state.behavior.lock().expect("behavior lock");
            (behavior.write, behavior.torn_write_connections)
        };
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::unbounded();
        tokio::spawn(async move {
            while let Some(Ok(request)) = inbound.next().await {
                let ops = request.puts.len() + request.deletes.len() + request.delete_ranges.len();
                state
                    .write_batch_sizes
                    .lock()
                    .expect("sizes lock")
                    .push(ops);
                if connection < torn {
                    // Received but unacknowledged: the batch's fate is
                    // unknown, so the client must requeue it on a new stream.
                    let _ = tx.unbounded_send(Err(Status::unavailable("scripted torn stream")));
                    return;
                }
                match mode {
                    WriteMode::Hang => continue,
                    WriteMode::DelayResponses(delay) => {
                        tokio::time::sleep(delay).await;
                    }
                    _ => {}
                }
                if tx.unbounded_send(Ok(respond_to(&request, mode))).is_err() {
                    return;
                }
            }
        });
        Ok(Response::new(rx.boxed()))
    }

    type ReadStream = BoxStream<'static, Result<proto::ReadResponse, Status>>;

    async fn read(&self, request: Request<proto::ReadRequest>) -> Rpc<Self::ReadStream> {
        let mode = self.state.behavior.lock().expect("behavior lock").read;
        let gets = request.into_inner().gets;
        match mode {
            ReadMode::Hang => {
                let (tx, rx) = mpsc::unbounded();
                // Keep the RPC open without ever responding.
                std::mem::forget(tx);
                Ok(Response::new(rx.boxed()))
            }
            ReadMode::Ok | ReadMode::GetStatus(_) => {
                let status = match mode {
                    ReadMode::GetStatus(code) => code,
                    _ => proto::Status::Ok as i32,
                };
                let response = proto::ReadResponse {
                    gets: gets
                        .iter()
                        .map(|_| proto::GetResponse {
                            status,
                            version: (status == proto::Status::Ok as i32).then(ok_version),
                            value: (status == proto::Status::Ok as i32)
                                .then(|| bytes::Bytes::from_static(b"mock-value")),
                            key: None,
                            secondary_index_key: None,
                        })
                        .collect(),
                };
                Ok(Response::new(futures::stream::iter([Ok(response)]).boxed()))
            }
        }
    }

    type ListStream = BoxStream<'static, Result<proto::ListResponse, Status>>;

    async fn list(&self, _: Request<proto::ListRequest>) -> Rpc<Self::ListStream> {
        Err(Status::unimplemented("not scripted"))
    }

    type RangeScanStream = BoxStream<'static, Result<proto::RangeScanResponse, Status>>;

    async fn range_scan(&self, _: Request<proto::RangeScanRequest>) -> Rpc<Self::RangeScanStream> {
        Err(Status::unimplemented("not scripted"))
    }

    type GetSequenceUpdatesStream =
        BoxStream<'static, Result<proto::GetSequenceUpdatesResponse, Status>>;

    async fn get_sequence_updates(
        &self,
        _: Request<proto::GetSequenceUpdatesRequest>,
    ) -> Rpc<Self::GetSequenceUpdatesStream> {
        Err(Status::unimplemented("not scripted"))
    }

    type GetNotificationsStream = BoxStream<'static, Result<proto::NotificationBatch, Status>>;

    async fn get_notifications(
        &self,
        _: Request<proto::NotificationsRequest>,
    ) -> Rpc<Self::GetNotificationsStream> {
        Err(Status::unimplemented("not scripted"))
    }

    async fn create_session(
        &self,
        _: Request<proto::CreateSessionRequest>,
    ) -> Rpc<proto::CreateSessionResponse> {
        Err(Status::unimplemented("not scripted"))
    }

    async fn keep_alive(
        &self,
        _: Request<proto::SessionHeartbeat>,
    ) -> Rpc<proto::KeepAliveResponse> {
        Err(Status::unimplemented("not scripted"))
    }

    async fn close_session(
        &self,
        _: Request<proto::CloseSessionRequest>,
    ) -> Rpc<proto::CloseSessionResponse> {
        Err(Status::unimplemented("not scripted"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{OxiaClient, OxiaClientBuilder, OxiaError};
    use std::time::Instant;

    async fn connect(mock: &MockOxia, request_timeout: Duration) -> OxiaClient {
        OxiaClientBuilder::new()
            .service_address(mock.address.clone())
            .request_timeout(request_timeout)
            .max_write_batches_in_flight(1)
            .build()
            .await
            .expect("client connects to mock")
    }

    /// With the in-flight window saturated, accumulating batches seal at
    /// `max_requests_per_batch`: 1 immediate dispatch, then 3 + 3.
    #[tokio::test]
    async fn batches_seal_at_max_requests() {
        let mock = MockOxia::start(Behavior {
            write: WriteMode::DelayResponses(Duration::from_millis(200)),
            ..Behavior::default()
        })
        .await;
        let client = OxiaClientBuilder::new()
            .service_address(mock.address.clone())
            .request_timeout(Duration::from_secs(10))
            .max_write_batches_in_flight(1)
            .max_requests_per_batch(3)
            .build()
            .await
            .expect("connect");

        let first = tokio::spawn({
            let client = client.clone();
            async move { client.put("count/first", "x").await }
        });
        // Let the first put dispatch alone, then queue six more behind it.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut rest = tokio::task::JoinSet::new();
        for i in 0..6 {
            let client = client.clone();
            rest.spawn(async move { client.put(format!("count/{i}"), "x").await });
        }
        first.await.expect("join").expect("first put");
        while let Some(result) = rest.join_next().await {
            result.expect("join").expect("put");
        }

        assert_eq!(mock.write_batch_sizes(), vec![1, 3, 3]);
    }

    /// With the window saturated, accumulating batches seal when the next
    /// operation would exceed `batch_max_size`.
    #[tokio::test]
    async fn batches_seal_at_max_bytes() {
        let mock = MockOxia::start(Behavior {
            write: WriteMode::DelayResponses(Duration::from_millis(200)),
            ..Behavior::default()
        })
        .await;
        let client = OxiaClientBuilder::new()
            .service_address(mock.address.clone())
            .request_timeout(Duration::from_secs(10))
            .max_write_batches_in_flight(1)
            // Two ~110-byte puts fit; a third (~330 bytes total) does not.
            .batch_max_size(300)
            .build()
            .await
            .expect("connect");

        let first = tokio::spawn({
            let client = client.clone();
            async move { client.put("bytes/first", vec![0u8; 100]).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut rest = tokio::task::JoinSet::new();
        for i in 0..6 {
            let client = client.clone();
            rest.spawn(async move { client.put(format!("bytes/{i}"), vec![0u8; 100]).await });
        }
        first.await.expect("join").expect("first put");
        while let Some(result) = rest.join_next().await {
            result.expect("join").expect("put");
        }

        let sizes = mock.write_batch_sizes();
        assert_eq!(sizes.iter().sum::<usize>(), 7, "sizes: {sizes:?}");
        assert!(
            sizes.iter().all(|&s| s <= 2),
            "byte limit must cap batches at 2 puts, got {sizes:?}"
        );
        assert!(
            sizes.contains(&2),
            "accumulation must pack 2 puts per batch, got {sizes:?}"
        );
    }

    /// No more write batches than `max_write_batches_in_flight` are ever
    /// outstanding on a shard.
    #[tokio::test]
    async fn write_window_is_bounded() {
        let mock = MockOxia::start(Behavior {
            write: WriteMode::Hang,
            ..Behavior::default()
        })
        .await;
        let client = OxiaClientBuilder::new()
            .service_address(mock.address.clone())
            .request_timeout(Duration::from_millis(800))
            .max_write_batches_in_flight(2)
            .build()
            .await
            .expect("connect");

        // Stagger the first two puts so each dispatches as its own batch and
        // fills one window slot; the remaining four must then queue.
        let mut puts = tokio::task::JoinSet::new();
        for i in 0..6 {
            let client = client.clone();
            puts.spawn(async move { client.put(format!("window/{i}"), "x").await });
            if i < 2 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        // Give the batcher ample time to dispatch everything it is willing to.
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(
            mock.write_batch_sizes().len(),
            2,
            "only the window's worth of batches may be dispatched"
        );

        // The server never answers, so every operation times out.
        while let Some(result) = puts.join_next().await {
            assert!(matches!(result.expect("join"), Err(OxiaError::Timeout)));
        }
    }

    /// A server that acknowledges a batch with missing per-operation
    /// responses produces a decode error, not a hang.
    #[tokio::test]
    async fn missing_responses_fail_the_batch() {
        let mock = MockOxia::start(Behavior {
            write: WriteMode::OmitResponses,
            ..Behavior::default()
        })
        .await;
        let client = connect(&mock, Duration::from_secs(5)).await;

        let result = client.put("missing/key", "x").await;
        match result {
            Err(OxiaError::Decode(message)) => {
                assert!(message.contains("missing put response"), "got: {message}")
            }
            other => panic!("expected a decode error, got {other:?}"),
        }
    }

    /// A stream torn after the server received a batch (fate unknown) makes
    /// the client requeue and re-send it on a fresh stream.
    #[tokio::test]
    async fn torn_stream_requeues_and_retries() {
        let mock = MockOxia::start(Behavior {
            torn_write_connections: 1,
            ..Behavior::default()
        })
        .await;
        let client = connect(&mock, Duration::from_secs(10)).await;

        client
            .put("torn/key", "x")
            .await
            .expect("put succeeds after retry");
        assert!(
            mock.write_connections() >= 2,
            "the batch must have been re-sent on a fresh stream"
        );
        assert!(
            mock.write_batch_sizes().len() >= 2,
            "the batch must have been received twice (at-least-once)"
        );
    }

    /// A write the server never acknowledges fails with `Timeout` once the
    /// request timeout elapses — it does not hang.
    #[tokio::test]
    async fn hung_write_times_out() {
        let mock = MockOxia::start(Behavior {
            write: WriteMode::Hang,
            ..Behavior::default()
        })
        .await;
        let client = connect(&mock, Duration::from_millis(500)).await;

        let start = Instant::now();
        let result = client.put("hang/key", "x").await;
        assert!(matches!(result, Err(OxiaError::Timeout)), "got {result:?}");
        assert!(start.elapsed() < Duration::from_secs(5));
    }

    /// A read the server never answers fails with `Timeout` as well.
    #[tokio::test]
    async fn hung_read_times_out() {
        let mock = MockOxia::start(Behavior {
            read: ReadMode::Hang,
            ..Behavior::default()
        })
        .await;
        let client = connect(&mock, Duration::from_millis(500)).await;

        let start = Instant::now();
        let result = client.get("hang/key").await;
        assert!(matches!(result, Err(OxiaError::Timeout)), "got {result:?}");
        assert!(start.elapsed() < Duration::from_secs(5));
    }

    /// Per-operation proto statuses in write responses map to the typed
    /// errors.
    #[tokio::test]
    async fn write_statuses_map_to_typed_errors() {
        let mock = MockOxia::start(Behavior {
            write: WriteMode::PutStatus(proto::Status::UnexpectedVersionId as i32),
            ..Behavior::default()
        })
        .await;
        let client = connect(&mock, Duration::from_secs(5)).await;
        let result = client.put("map/key", "x").expected_version_id(7).await;
        assert!(
            matches!(result, Err(OxiaError::UnexpectedVersionId)),
            "got {result:?}"
        );
        drop(client);

        let mock = MockOxia::start(Behavior {
            write: WriteMode::DeleteStatus(proto::Status::KeyNotFound as i32),
            ..Behavior::default()
        })
        .await;
        let client = connect(&mock, Duration::from_secs(5)).await;
        let result = client.delete("map/other").await;
        assert!(
            matches!(result, Err(OxiaError::KeyNotFound)),
            "got {result:?}"
        );
    }

    /// Per-operation proto statuses in read responses map to the typed
    /// errors, and a healthy read produces the served value.
    #[tokio::test]
    async fn read_statuses_map_to_typed_errors() {
        let mock = MockOxia::start(Behavior::default()).await;
        let client = connect(&mock, Duration::from_secs(5)).await;

        let record = client.get("read/key").await.expect("scripted get succeeds");
        assert_eq!(record.value.as_deref(), Some(b"mock-value".as_ref()));

        mock.set_read_mode(ReadMode::GetStatus(proto::Status::KeyNotFound as i32));
        let result = client.get("read/key").await;
        assert!(
            matches!(result, Err(OxiaError::KeyNotFound)),
            "got {result:?}"
        );
    }

    /// A permanent gRPC error on the assignments stream fails `build()` fast
    /// instead of retrying until the deadline.
    #[tokio::test]
    async fn permanent_error_fails_build_fast() {
        let mock = MockOxia::start(Behavior {
            assignments_error: Some(tonic::Code::Unauthenticated),
            ..Behavior::default()
        })
        .await;

        let start = Instant::now();
        let result = OxiaClientBuilder::new()
            .service_address(mock.address.clone())
            .request_timeout(Duration::from_secs(30))
            .build()
            .await;
        assert!(result.is_err());
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "a permanent error must fail fast, took {:?}",
            start.elapsed()
        );
    }
}
