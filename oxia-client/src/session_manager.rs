use crate::errors::OxiaError;
use crate::oxia::{CloseSessionRequest, CreateSessionRequest, SessionHeartbeat};
use crate::provider_manager::ProviderManager;
use crate::retry::{retry_until_cancelled, RetryError};
use crate::shard_manager::ShardManager;
use crate::status::CODE_SESSION_NOT_FOUND;
use dashmap::DashMap;
use log::{info, warn};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, OnceCell};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::interval;
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
    /// Set to `false` by the keep-alive task once the server reports the session
    /// no longer exists, so [`SessionManager::get_session_id`] re-creates it.
    alive: Arc<AtomicBool>,
    inner: Arc<Inner>,
}

impl Session {
    pub fn new(
        shard_id: i64,
        session_id: i64,
        keep_alive_interval: Duration,
        shard_manager: Arc<ShardManager>,
        provider_manager: Arc<ProviderManager>,
    ) -> Self {
        let context = CancellationToken::new();
        let alive = Arc::new(AtomicBool::new(true));
        let handle = Mutex::new(Some(tokio::spawn(start_keep_alive(
            context.clone(),
            shard_manager.clone(),
            provider_manager.clone(),
            shard_id,
            session_id,
            keep_alive_interval,
            alive.clone(),
        ))));
        let session = Self {
            handle,
            context,
            alive,
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

    /// Whether the session is still believed valid. Turns `false` once the
    /// keep-alive observes that the server has expired it.
    fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.context.cancel();
    }
}

#[allow(clippy::too_many_arguments)]
async fn start_keep_alive(
    context: CancellationToken,
    shard_manager: Arc<ShardManager>,
    provider_manager: Arc<ProviderManager>,
    shard_id: i64,
    session_id: i64,
    keep_alive_interval: Duration,
    alive: Arc<AtomicBool>,
) {
    let op_defer = || {
        let local_shard_manager = shard_manager.clone();
        let local_provider_manager = provider_manager.clone();
        let local_context = context.clone();
        let local_alive = alive.clone();
        async move {
            match local_shard_manager.get_leader(shard_id) {
                None => Err(RetryError::transient(OxiaError::LeaderNotFound {
                    shard: shard_id,
                })),
                Some(leader) => {
                    let mut provider = local_provider_manager
                        .get_provider(leader.service_address)
                        .await
                        .map_err(RetryError::transient)?;
                    let mut ticker = interval(keep_alive_interval);
                    loop {
                        tokio::select! {
                            _ = local_context.cancelled() => {
                                info!("Session keep-alive exit due to cancellation.");
                                return Ok(());
                            },
                            _ = ticker.tick() => {
                                if let Err(err) = provider
                                    .keep_alive(Request::new(SessionHeartbeat { shard: shard_id, session_id }))
                                    .await
                                {
                                    if err.code() as i32 == CODE_SESSION_NOT_FOUND {
                                        info!("Session {session_id} on shard {shard_id} expired; it will be re-created on next use.");
                                        // Mark dead so get_session_id re-creates it, then stop.
                                        local_alive.store(false, Ordering::Release);
                                        return Err(RetryError::fatal(OxiaError::SessionExpired));
                                    }
                                    return Err(RetryError::transient(OxiaError::from(err)));
                                }
                            }
                        }
                    }
                }
            }
        }
    };
    retry_until_cancelled(&context, "session-keep-alive", op_defer).await;
}

impl Session {
    pub(crate) async fn shutdown(self) -> Result<(), OxiaError> {
        let shard_manager = self.inner.shard_manager.clone();
        let provider_manager = self.inner.provider_manager.clone();
        if let Some(node) = shard_manager.get_leader(self.inner.shard_id) {
            let mut provider = provider_manager.get_provider(node.service_address).await?;
            let result = provider
                .close_session(Request::new(CloseSessionRequest {
                    shard: self.inner.shard_id,
                    session_id: self.inner.id,
                }))
                .await
                .map_err(OxiaError::from);
            if let Err(err) = result {
                warn!(
                    "Failed to close session. shard_id={:?} session_id={:?} error={:?}",
                    self.inner.shard_id, self.inner.id, err
                );
            }
        } else {
            warn!("Shard leader not found. shard_id={:?}", self.inner.shard_id);
        }

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

pub(crate) struct SessionManager {
    identity: String,
    session_timeout: Duration,
    session_keep_alive: Duration,
    sessions: DashMap<i64, OnceCell<Session>>,
    shard_manager: Arc<ShardManager>,
    provider_manager: Arc<ProviderManager>,
}

impl SessionManager {
    pub(crate) fn new(
        identity: String,
        session_timeout: Duration,
        session_keep_alive: Duration,
        shard_manager: Arc<ShardManager>,
        provider_manager: Arc<ProviderManager>,
    ) -> Self {
        SessionManager {
            identity,
            session_timeout,
            session_keep_alive,
            sessions: DashMap::new(),
            shard_manager,
            provider_manager,
        }
    }

    pub(crate) async fn get_session_id(&self, shard_id: i64) -> Result<i64, OxiaError> {
        let mut session_cell = self.sessions.entry(shard_id).or_default();
        // If a previously created session has expired (its keep-alive saw
        // SESSION_NOT_FOUND), discard it so a fresh one is created below — the
        // caller then transparently gets a working session.
        if session_cell.get().is_some_and(|s| !s.is_alive()) {
            *session_cell = OnceCell::new();
        }
        let session = session_cell
            .get_or_try_init(|| async {
                match self.shard_manager.get_leader(shard_id) {
                    None => Err(OxiaError::LeaderNotFound { shard: shard_id }),
                    Some(node) => {
                        let mut provider = self
                            .provider_manager
                            .get_provider(node.service_address)
                            .await?;
                        let response = provider
                            .create_session(Request::new(CreateSessionRequest {
                                shard: shard_id,
                                session_timeout_ms: self.session_timeout.as_millis() as u32,
                                client_identity: self.identity.clone(),
                            }))
                            .await
                            .map_err(OxiaError::from)?;
                        let session_id = response.into_inner().session_id;
                        Ok(Session::new(
                            shard_id,
                            session_id,
                            self.session_keep_alive,
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
                OxiaError::Disconnected(format!("Session task failed to join: {}", err))
            })??;
        }
        Ok(())
    }
}
