use crate::errors::OxiaError;
use crate::errors::OxiaError::{SessionDoesNotExist, ShardLeaderNotFound, UnexpectedStatus};
use crate::oxia::{CreateSessionRequest, SessionHeartbeat};
use crate::provider_manager::ProviderManager;
use crate::shard_manager::ShardManager;
use crate::status::CODE_SESSION_NOT_FOUND;
use backoff::{Error, ExponentialBackoff};
use dashmap::DashMap;
use log::{info, warn};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, OnceCell};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tonic::Request;

struct Inner {
    shard_id: i64,
    id: i64,
    shard_manager: Arc<ShardManager>,
    provider_manager: Arc<ProviderManager>,
}

struct Session {
    context: CancellationToken,
    handle: Mutex<Option<JoinHandle<()>>>,
    inner: Arc<Inner>,
}

impl Session {
    pub fn new(
        shard_id: i64,
        session_id: i64,
        shard_manager: Arc<ShardManager>,
        provider_manager: Arc<ProviderManager>,
    ) -> Self {
        let context = CancellationToken::new();
        let handle = Mutex::new(Some(tokio::spawn(start_keep_alive(
            context.clone(),
            shard_manager.clone(),
            provider_manager.clone(),
            shard_id,
            session_id,
        ))));
        let session = Self {
            handle,
            context,
            inner: Arc::new(Inner {
                id: session_id,
                shard_id,
                shard_manager,
                provider_manager,
            }),
        };
        info!(
            "Created a new session. shard_id={:?} session_id={:?}",
            shard_id, session_id
        );
        session
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.context.cancel();
    }
}

async fn start_keep_alive(
    context: CancellationToken,
    shard_manager: Arc<ShardManager>,
    provider_manager: Arc<ProviderManager>,
    shard_id: i64,
    session_id: i64,
) {
    let op_defer = || {
        let local_shard_manager = shard_manager.clone();
        let local_provider_manager = provider_manager.clone();
        let local_context = context.clone();
        async move {
            match local_shard_manager.get_leader(shard_id) {
                None => Err(Error::transient(ShardLeaderNotFound(shard_id))),
                Some(leader) => {
                    let provider = local_provider_manager
                        .get_provider(leader.service_address)
                        .await?;
                    loop {
                        tokio::select! {
                        _ = local_context.cancelled() => {
                            info!("Session keep-alive exit due to cancellation.");
                            return Ok(());
                        },
                        else => {
                            let mut provider_guard = provider.lock().await;
                            let _ = provider_guard
                                .keep_alive(Request::new(SessionHeartbeat { shard: shard_id, session_id, }))
                                .await
                                .map_err(|err| {
                                    if err.code() as i32 == CODE_SESSION_NOT_FOUND{
                                         info!("Session keep-alive exit due to session not found.");
                                        return Error::permanent(SessionDoesNotExist());
                                    }
                                    return Error::transient(UnexpectedStatus(err.to_string()));
                                });
                            }
                        }
                    }
                }
            }
        }
    };
    let backoff = ExponentialBackoff::default();
    let _ = backoff::future::retry_notify(backoff, op_defer, |err, duration| {
        warn!(
            "Transient failure when session keep-alive. error: {:?} retry-after: {:?}.",
            err, duration
        )
    })
    .await;
}

impl Session {
    pub(crate) async fn shutdown(self) -> Result<(), OxiaError> {
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

pub(crate) struct SessionManager {
    identity: String,
    session_timeout: Duration,
    sessions: DashMap<i64, OnceCell<Session>>,
    shard_manager: Arc<ShardManager>,
    provider_manager: Arc<ProviderManager>,
}

impl SessionManager {
    pub(crate) fn new(
        identity: String,
        session_timeout: Duration,
        shard_manager: Arc<ShardManager>,
        provider_manager: Arc<ProviderManager>,
    ) -> Self {
        SessionManager {
            identity,
            session_timeout,
            sessions: DashMap::new(),
            shard_manager,
            provider_manager,
        }
    }

    pub(crate) async fn get_session_id(&self, shard_id: i64) -> Result<i64, OxiaError> {
        let session_cell = self
            .sessions
            .entry(shard_id)
            .or_insert_with(|| OnceCell::new());
        let session = session_cell
            .get_or_try_init(|| async {
                match self.shard_manager.get_leader(shard_id) {
                    None => Err(ShardLeaderNotFound(shard_id)),
                    Some(node) => {
                        let client = self
                            .provider_manager
                            .get_provider(node.service_address)
                            .await?;
                        let mut client_guard = client.lock().await;
                        let response = client_guard
                            .create_session(Request::new(CreateSessionRequest {
                                shard: shard_id,
                                session_timeout_ms: self.session_timeout.clone().as_millis() as u32,
                                client_identity: self.identity.clone(),
                            }))
                            .await
                            .map_err(|err| UnexpectedStatus(err.to_string()))?;
                        let session_id = response.into_inner().session_id;
                        Ok(Session::new(
                            shard_id,
                            session_id,
                            self.shard_manager.clone(),
                            self.provider_manager.clone(),
                        ))
                    }
                }
            })
            .await?;
        Ok(session.inner.id)
    }

    pub(crate) async fn shutdown(self) -> Result<(), OxiaError> {
        let mut joiner = JoinSet::new();
        for (_, session_cell) in self.sessions.into_iter() {
            if let Some(session) = session_cell.into_inner() {
                joiner.spawn(session.shutdown());
            }
        }
        while let Some(result) = joiner.join_next().await {
            result.map_err(|err| {
                UnexpectedStatus(format!("Session task failed to join: {}", err))
            })??;
        }
        Ok(())
    }
}
