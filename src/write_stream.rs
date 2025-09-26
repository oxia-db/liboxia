use crate::errors::OxiaError;
use crate::errors::OxiaError::UnexpectedStatus;
use crate::oxia::{WriteRequest, WriteResponse};
use log::{info, warn};
use std::collections::VecDeque;
use std::sync::Arc;
use task::JoinHandle;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{oneshot, Mutex};
use tokio::task;
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
    loop_handle: JoinHandle<()>,
}

impl WriteStream {
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

    pub(crate) fn wrap(tx: UnboundedSender<WriteRequest>, rx: Streaming<WriteResponse>) -> Self {
        let inner = Arc::new(Mutex::new(Inner {
            alive: true,
            tx,
            inflight_deque: VecDeque::new(),
        }));
        let context = CancellationToken::new();
        let handle = tokio::spawn(handle_response(context.clone(), inner.clone(), rx));
        WriteStream {
            context,
            inner,
            loop_handle: handle,
        }
    }
}

impl Drop for WriteStream {
    fn drop(&mut self) {
        self.context.cancel();
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
