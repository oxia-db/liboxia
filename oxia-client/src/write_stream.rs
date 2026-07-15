use crate::errors::OxiaError;
use crate::proto::{WriteRequest, WriteResponse};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot::Receiver;
use tokio::sync::{Mutex, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::Streaming;
use tonic::codegen::tokio_stream::StreamExt;
use tracing::{info, warn};

pub(crate) struct Inflight {
    pub(crate) future: oneshot::Sender<WriteResponse>,
}

pub(crate) struct Inner {
    pub(crate) alive: bool,
    pub(crate) tx: UnboundedSender<WriteRequest>,
    pub(crate) inflight_deque: VecDeque<Inflight>,
}

impl Inner {
    fn fail_inflight(&mut self) {
        for _inflight in self.inflight_deque.drain(..) {}
    }
}

pub(crate) struct WriteStream {
    context: CancellationToken,
    inner: Arc<Mutex<Inner>>,
    defer_response: Mutex<Option<Receiver<WriteResponse>>>,
    handle: Mutex<Option<JoinHandle<()>>>,
    request_timeout: Duration,
}

impl Drop for WriteStream {
    fn drop(&mut self) {
        self.context.cancel();
    }
}

impl WriteStream {
    pub(crate) async fn send_defer(&self, request: WriteRequest) -> Result<(), OxiaError> {
        let mut inner_guard = self.inner.lock().await;
        if !inner_guard.alive {
            return Err(OxiaError::Disconnected(
                "write stream is closed".to_string(),
            ));
        }
        let (tx, rx) = oneshot::channel();
        let inflight = Inflight { future: tx };
        inner_guard.inflight_deque.push_back(inflight);
        if let Err(err) = inner_guard.tx.send(request) {
            inner_guard.alive = false;
            inner_guard.fail_inflight();
            return Err(OxiaError::Disconnected(err.to_string()));
        }
        drop(inner_guard);
        let mut guard = self.defer_response.lock().await;
        *guard = Some(rx);
        Ok(())
    }

    pub(crate) async fn is_alive(&self) -> bool {
        self.inner.lock().await.alive
    }

    pub(crate) async fn send(&self, request: WriteRequest) -> Result<WriteResponse, OxiaError> {
        let mut inner_guard = self.inner.lock().await;
        if !inner_guard.alive {
            return Err(OxiaError::Disconnected(
                "write stream is closed".to_string(),
            ));
        }
        let (tx, rx) = oneshot::channel();
        let inflight = Inflight { future: tx };
        inner_guard.inflight_deque.push_back(inflight);
        if let Err(err) = inner_guard.tx.send(request) {
            inner_guard.alive = false;
            inner_guard.fail_inflight();
            return Err(OxiaError::Disconnected(err.to_string()));
        }
        drop(inner_guard);
        self.await_response(rx).await
    }

    pub(crate) async fn get_defer_response(&self) -> Option<Result<WriteResponse, OxiaError>> {
        let mut guard = self.defer_response.lock().await;
        let rx = guard.take()?;
        drop(guard);
        Some(self.await_response(rx).await)
    }

    /// Waits for a batch's response, bounded by the request timeout. A timeout
    /// means the stream is wedged (the server stopped responding): mark it dead
    /// and fail its in-flight requests so the caller sees a `Timeout` and the
    /// next write tears the stream down and re-creates it.
    async fn await_response(
        &self,
        rx: Receiver<WriteResponse>,
    ) -> Result<WriteResponse, OxiaError> {
        match tokio::time::timeout(self.request_timeout, rx).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(err)) => Err(OxiaError::Disconnected(err.to_string())),
            Err(_) => {
                let mut inner_guard = self.inner.lock().await;
                inner_guard.alive = false;
                inner_guard.fail_inflight();
                Err(OxiaError::Timeout)
            }
        }
    }

    pub(crate) fn new(tx: UnboundedSender<WriteRequest>, request_timeout: Duration) -> Self {
        let inner = Arc::new(Mutex::new(Inner {
            alive: true,
            tx,
            inflight_deque: VecDeque::new(),
        }));
        let context = CancellationToken::new();
        WriteStream {
            context,
            inner,
            defer_response: Mutex::new(None),
            handle: Mutex::new(None),
            request_timeout,
        }
    }

    pub(crate) async fn listen(&self, streaming: Streaming<WriteResponse>) {
        let mut handle_guard = self.handle.lock().await;
        *handle_guard = Some(tokio::spawn(handle_response(
            self.context.clone(),
            self.inner.clone(),
            streaming,
        )));
    }

    pub async fn shutdown(self) -> Result<(), OxiaError> {
        self.context.cancel();
        let mut guard = self.handle.lock().await;
        if let Some(handle) = guard.take() {
            handle
                .await
                .map_err(|err| OxiaError::Disconnected(err.to_string()))?
        }
        Ok(())
    }
}

async fn handle_response(
    context: CancellationToken,
    inner: Arc<Mutex<Inner>>,
    mut rx: Streaming<WriteResponse>,
) {
    loop {
        tokio::select! {
            _ = context.cancelled() => {
                info!("Close write stream due to context canceled.");
                return
            },
            response = rx.next() => {
                let Some(response) = response else {
                    let mut inner_guard = inner.lock().await;
                    inner_guard.alive = false;
                    inner_guard.fail_inflight();
                    return;
                };
                match response {
                    Ok(write_response) => {
                        let mut inner_guard = inner.lock().await;
                        if !inner_guard.alive {
                            // stop loop and exit
                            return
                        }
                        let Some(inflight) = inner_guard.inflight_deque.pop_front() else {
                            warn!("Receive an empty write inflight, discard it.");
                            continue
                        };
                        if let Err(err) = inflight.future.send(write_response) {
                            warn!("Send callback response failed. error: {:?}", err);
                        }
                    }
                    Err(_status) => {
                        let mut inner_guard = inner.lock().await;
                        inner_guard.alive = false;
                        inner_guard.fail_inflight();
                        return;
                    }
                }
            }
        }
    }
}
