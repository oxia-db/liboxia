use crate::errors::OxiaError;
use crate::errors::OxiaError::UnexpectedStatus;
use crate::oxia::{WriteRequest, WriteResponse};
use log::{info, warn};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot::Receiver;
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::codegen::tokio_stream::StreamExt;
use tonic::Streaming;

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
            return Err(UnexpectedStatus("write stream is closed".to_string()));
        }
        let (tx, rx) = oneshot::channel();
        let inflight = Inflight { future: tx };
        inner_guard.inflight_deque.push_back(inflight);
        let send_result = inner_guard.tx.send(request);
        if send_result.is_err() {
            inner_guard.alive = false;
            inner_guard.fail_inflight();
            return Err(UnexpectedStatus(send_result.unwrap_err().to_string()));
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
            return Err(UnexpectedStatus("write stream is closed".to_string()));
        }
        let (tx, rx) = oneshot::channel();
        let inflight = Inflight { future: tx };
        inner_guard.inflight_deque.push_back(inflight);
        let send_result = inner_guard.tx.send(request);
        if send_result.is_err() {
            inner_guard.alive = false;
            inner_guard.fail_inflight();
            return Err(UnexpectedStatus(send_result.unwrap_err().to_string()));
        }
        drop(inner_guard);
        rx.await.map_err(|err| UnexpectedStatus(err.to_string()))
    }

    pub(crate) async fn get_defer_response(&self) -> Option<Result<WriteResponse, OxiaError>> {
        let mut guard = self.defer_response.lock().await;
        let option = guard.take();
        if option.is_none() {
            return None;
        }
        Some(
            option
                .unwrap()
                .await
                .map_err(|err| UnexpectedStatus(err.to_string())),
        )
    }

    pub(crate) fn new(tx: UnboundedSender<WriteRequest>) -> Self {
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
                .map_err(|err| UnexpectedStatus(err.to_string()))?
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
                if response.is_none() {
                    let mut inner_guard = inner.lock().await;
                    inner_guard.alive = false;
                    inner_guard.fail_inflight();
                    return;
                }
                match response.unwrap() {
                    Ok(write_response) => {
                        let mut inner_guard = inner.lock().await;
                        if !inner_guard.alive {
                            // stop loop and exit
                            return
                        }
                        let inflight = inner_guard.inflight_deque.pop_front();
                        if inflight.is_none() {
                            warn!("Receive an empty write inflight, discard it.");
                            continue
                        }
                        let callback_result = inflight.unwrap().future.send(write_response); // it shouldn't happen
                        if callback_result.is_err() {
                            warn!("Send callback response failed. error: {:?}", callback_result.unwrap_err());
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
