//! Builder for configuring and creating an [`OxiaClient`].

use crate::address::ensure_protocol;
use crate::client::OxiaClient;
use crate::client_options::OxiaClientOptions;
use crate::errors::OxiaError;
use std::time::Duration;

/// Configures and creates an [`OxiaClient`].
///
/// ```no_run
/// # async fn example() -> Result<(), oxia::OxiaError> {
/// use oxia::OxiaClient;
/// use std::time::Duration;
///
/// let client = OxiaClient::builder()
///     .service_address("localhost:6648")
///     .namespace("my-namespace")
///     .session_timeout(Duration::from_secs(30))
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default)]
#[must_use = "builders do nothing unless built"]
pub struct OxiaClientBuilder {
    service_address: Option<String>,
    namespace: Option<String>,
    identity: Option<String>,
    batch_linger: Option<Duration>,
    batch_max_size: Option<u32>,
    max_requests_per_batch: Option<u32>,
    session_timeout: Option<Duration>,
    session_keep_alive: Option<Duration>,
    request_timeout: Option<Duration>,
}

impl OxiaClientBuilder {
    /// Creates a builder with all options at their defaults.
    pub fn new() -> Self {
        OxiaClientBuilder::default()
    }

    /// The address of the Oxia service, as `host:port` (default:
    /// `127.0.0.1:6648`).
    pub fn service_address(mut self, service_address: impl Into<String>) -> Self {
        self.service_address = Some(ensure_protocol(service_address.into()));
        self
    }

    /// The Oxia namespace all operations apply to (default: `"default"`).
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// The client identity reported with ephemeral records (default: a random
    /// UUID).
    pub fn identity(mut self, identity: impl Into<String>) -> Self {
        self.identity = Some(identity.into());
        self
    }

    /// How long an incomplete batch may wait for more operations before being
    /// sent (default: 5ms).
    pub fn batch_linger(mut self, batch_linger: Duration) -> Self {
        self.batch_linger = Some(batch_linger);
        self
    }

    /// The maximum byte size of a batch (default: 128KiB).
    pub fn batch_max_size(mut self, batch_max_size: u32) -> Self {
        self.batch_max_size = Some(batch_max_size);
        self
    }

    /// The maximum number of operations in a batch (default: 1000).
    pub fn max_requests_per_batch(mut self, max_requests_per_batch: u32) -> Self {
        self.max_requests_per_batch = Some(max_requests_per_batch);
        self
    }

    /// The session timeout for ephemeral records (default: 15s). Ephemeral
    /// records disappear if the client fails to heartbeat for this long.
    pub fn session_timeout(mut self, session_timeout: Duration) -> Self {
        self.session_timeout = Some(session_timeout);
        self
    }

    /// The interval between session keep-alive heartbeats (default:
    /// `session_timeout / 10`).
    pub fn session_keep_alive(mut self, session_keep_alive: Duration) -> Self {
        self.session_keep_alive = Some(session_keep_alive);
        self
    }

    /// The deadline applied to individual operations (default: 30s).
    pub fn request_timeout(mut self, request_timeout: Duration) -> Self {
        self.request_timeout = Some(request_timeout);
        self
    }

    fn assemble_options(self) -> OxiaClientOptions {
        let mut options = OxiaClientOptions::default();
        if let Some(service_address) = self.service_address {
            options.service_address = service_address
        }
        if let Some(namespace) = self.namespace {
            options.namespace = namespace;
        }
        if let Some(identity) = self.identity {
            options.identity = identity;
        }
        if let Some(batch_linger) = self.batch_linger {
            options.batch_linger = batch_linger;
        }
        if let Some(batch_max_size) = self.batch_max_size {
            options.batch_max_size = batch_max_size;
        }
        if let Some(max_requests_per_batch) = self.max_requests_per_batch {
            options.max_requests_per_batch = max_requests_per_batch;
        }
        if let Some(session_timeout) = self.session_timeout {
            options.session_timeout = session_timeout;
        }
        // Default the keep-alive interval to session_timeout/10, matching the
        // reference client, once the final session timeout is known.
        options.session_keep_alive = self
            .session_keep_alive
            .unwrap_or(options.session_timeout / 10);
        if let Some(request_timeout) = self.request_timeout {
            options.request_timeout = request_timeout;
        }
        options
    }

    /// Connects to the cluster and returns the client.
    ///
    /// Fails fast — within the request timeout — when the cluster is
    /// unreachable, the namespace does not exist, or the first shard
    /// assignments cannot be obtained.
    pub async fn build(self) -> Result<OxiaClient, OxiaError> {
        OxiaClient::new(self.assemble_options()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keep_alive_defaults_to_a_tenth_of_the_session_timeout() {
        let options = OxiaClientBuilder::new()
            .session_timeout(Duration::from_secs(30))
            .assemble_options();
        assert_eq!(options.session_keep_alive, Duration::from_secs(3));
    }

    #[test]
    fn keep_alive_can_be_overridden() {
        let options = OxiaClientBuilder::new()
            .session_timeout(Duration::from_secs(30))
            .session_keep_alive(Duration::from_millis(500))
            .assemble_options();
        assert_eq!(options.session_keep_alive, Duration::from_millis(500));
    }

    #[test]
    fn default_keep_alive_tracks_the_default_session_timeout() {
        let options = OxiaClientBuilder::new().assemble_options();
        assert_eq!(options.session_keep_alive, options.session_timeout / 10);
    }
}
