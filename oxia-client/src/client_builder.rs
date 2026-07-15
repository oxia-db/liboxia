use crate::address::ensure_protocol;
use crate::client::OxiaClient;
use crate::client_options::OxiaClientOptions;
use crate::errors::OxiaError;
use std::time::Duration;

/// Builder for creating an [`OxiaClient`] with custom configuration.
///
/// # Example
/// ```no_run
/// # async fn example() {
/// use oxia::client_builder::OxiaClientBuilder;
/// use std::time::Duration;
///
/// let client = OxiaClientBuilder::new()
///     .service_address("localhost:6648".to_string())
///     .namespace("my-namespace".to_string())
///     .session_timeout(Duration::from_secs(30))
///     .build()
///     .await
///     .unwrap();
/// # }
/// ```
#[derive(Debug, Clone, Default)]
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
    pub fn new() -> Self {
        OxiaClientBuilder::default()
    }

    pub fn service_address(mut self, service_address: String) -> Self {
        self.service_address = Some(ensure_protocol(service_address));
        self
    }

    pub fn namespace(mut self, namespace: String) -> Self {
        self.namespace = Some(namespace);
        self
    }

    pub fn identity(mut self, identity: String) -> Self {
        self.identity = Some(identity);
        self
    }

    pub fn batch_linger(mut self, batch_linger: Duration) -> Self {
        self.batch_linger = Some(batch_linger);
        self
    }

    pub fn batch_max_size(mut self, batch_max_size: u32) -> Self {
        self.batch_max_size = Some(batch_max_size);
        self
    }

    pub fn max_requests_per_batch(mut self, max_requests_per_batch: u32) -> Self {
        self.max_requests_per_batch = Some(max_requests_per_batch);
        self
    }

    pub fn session_timeout(mut self, session_timeout: Duration) -> Self {
        self.session_timeout = Some(session_timeout);
        self
    }

    /// Interval between session keep-alive heartbeats. Defaults to
    /// `session_timeout / 10` when not set.
    pub fn session_keep_alive(mut self, session_keep_alive: Duration) -> Self {
        self.session_keep_alive = Some(session_keep_alive);
        self
    }

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
