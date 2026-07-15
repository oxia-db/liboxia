use crate::errors::OxiaError;
use crate::proto::oxia_client_client::OxiaClientClient;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::OnceCell;
use tonic::transport::{Channel, Endpoint};

/// HTTP/2 keep-alive settings, matching the reference client. The server enforces
/// a minimum client ping interval of 5s and permits pings without an active
/// stream, so a 10s interval is safe. Keep-alive lets a silently-dead connection
/// (e.g. a dropped NAT mapping) be detected instead of leaving the long-lived
/// streams — assignments, notifications, sequence updates, the write stream —
/// hanging forever.
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(10);
const KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(3);

pub struct ProviderManager {
    providers: Arc<DashMap<String, OnceCell<OxiaClientClient<Channel>>>>,
    connect_timeout: Duration,
}

impl ProviderManager {
    pub async fn get_provider(
        &self,
        address: String,
    ) -> Result<OxiaClientClient<Channel>, OxiaError> {
        let once_cell = self.providers.entry(address.clone()).or_default();
        let connect_timeout = self.connect_timeout;
        let client = once_cell
            .get_or_try_init(|| async move {
                let channel = Endpoint::from_shared(address)?
                    .connect_timeout(connect_timeout)
                    .keep_alive_while_idle(true)
                    .http2_keep_alive_interval(KEEP_ALIVE_INTERVAL)
                    .keep_alive_timeout(KEEP_ALIVE_TIMEOUT)
                    .connect()
                    .await?;
                Ok::<OxiaClientClient<Channel>, OxiaError>(OxiaClientClient::new(channel))
            })
            .await?
            .clone();
        Ok(client)
    }

    pub fn new(connect_timeout: Duration) -> ProviderManager {
        ProviderManager {
            providers: Arc::new(DashMap::new()),
            connect_timeout,
        }
    }

    /// Drops all cached connections.
    pub fn clear(&self) {
        self.providers.clear();
    }
}
