use crate::auth::{AuthInterceptor, TokenAuth};
use crate::errors::OxiaError;
use crate::proto::oxia_client_client::OxiaClientClient;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::OnceCell;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint};

/// The transport every RPC flows through: a tonic channel wrapped with the
/// auth interceptor (a no-op when no token provider is configured).
pub(crate) type AuthChannel = InterceptedService<Channel, AuthInterceptor>;

/// HTTP/2 keep-alive settings, matching the reference client. The server enforces
/// a minimum client ping interval of 5s and permits pings without an active
/// stream, so a 10s interval is safe. Keep-alive lets a silently-dead connection
/// (e.g. a dropped NAT mapping) be detected instead of leaving the long-lived
/// streams — assignments, notifications, sequence updates, the write stream —
/// hanging forever.
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(10);
const KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(3);

pub struct ProviderManager {
    providers: Arc<DashMap<String, OnceCell<OxiaClientClient<AuthChannel>>>>,
    connect_timeout: Duration,
    auth: Option<TokenAuth>,
    #[cfg(feature = "tls")]
    tls: Option<tonic::transport::ClientTlsConfig>,
}

impl ProviderManager {
    pub async fn get_provider(
        &self,
        address: String,
    ) -> Result<OxiaClientClient<AuthChannel>, OxiaError> {
        let once_cell = self.providers.entry(address.clone()).or_default();
        let connect_timeout = self.connect_timeout;
        let auth = self.auth.clone();
        #[cfg(feature = "tls")]
        let tls = self.tls.clone();
        let client = once_cell
            .get_or_try_init(|| async move {
                // With TLS configured, every dial — the bootstrap address and
                // the scheme-less shard-leader addresses that defaulted to
                // http:// — must go over https://.
                #[cfg(feature = "tls")]
                let address = if tls.is_some() {
                    crate::address::force_https(address)
                } else {
                    address
                };
                let endpoint = Endpoint::from_shared(address)?
                    .connect_timeout(connect_timeout)
                    .keep_alive_while_idle(true)
                    .http2_keep_alive_interval(KEEP_ALIVE_INTERVAL)
                    .keep_alive_timeout(KEEP_ALIVE_TIMEOUT);
                #[cfg(feature = "tls")]
                let endpoint = match tls {
                    Some(tls) => endpoint.tls_config(tls)?,
                    None => endpoint,
                };
                let channel = endpoint.connect().await?;
                Ok::<OxiaClientClient<AuthChannel>, OxiaError>(OxiaClientClient::with_interceptor(
                    channel,
                    AuthInterceptor::new(auth),
                ))
            })
            .await?
            .clone();
        Ok(client)
    }

    pub fn new(connect_timeout: Duration) -> ProviderManager {
        ProviderManager {
            providers: Arc::new(DashMap::new()),
            connect_timeout,
            auth: None,
            #[cfg(feature = "tls")]
            tls: None,
        }
    }

    /// Authenticates every connection this manager creates with tokens from
    /// the given provider.
    pub fn with_auth(mut self, auth: Option<TokenAuth>) -> ProviderManager {
        self.auth = auth;
        self
    }

    /// Applies a TLS configuration to every connection this manager creates.
    #[cfg(feature = "tls")]
    pub fn with_tls(mut self, tls: Option<tonic::transport::ClientTlsConfig>) -> ProviderManager {
        self.tls = tls;
        self
    }

    /// Drops all cached connections.
    pub fn clear(&self) {
        self.providers.clear();
    }
}
