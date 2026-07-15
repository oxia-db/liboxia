use std::time::Duration;
use uuid::Uuid;

/// Configuration options for the Oxia client.
#[derive(Debug, Clone)]
pub struct OxiaClientOptions {
    /// The address of the Oxia service (e.g., "http://localhost:6648").
    pub service_address: String,
    /// The Oxia namespace to use (default: "default").
    pub namespace: String,
    /// Client identity string, used for ephemeral record tracking.
    pub identity: String,
    /// Wait time before sending a batch (default: 5ms).
    pub batch_linger: Duration,
    /// Maximum batch size in bytes (default: 128KB).
    pub batch_max_size: u32,
    /// Maximum number of requests per batch (default: 1000).
    pub max_requests_per_batch: u32,
    /// Session timeout for ephemeral records (default: 15s).
    pub session_timeout: Duration,
    /// Interval between session keep-alive heartbeats (default: `session_timeout / 10`).
    pub session_keep_alive: Duration,
    /// Timeout for individual requests (default: 30s).
    pub request_timeout: Duration,
}

impl Default for OxiaClientOptions {
    fn default() -> Self {
        Self {
            service_address: String::from("http://127.0.0.1:6648"),
            namespace: String::from("default"),
            identity: Uuid::new_v4().to_string(),
            batch_linger: Duration::from_millis(5),
            batch_max_size: 128 * 1024,
            max_requests_per_batch: 1000,
            session_timeout: Duration::from_secs(15),
            session_keep_alive: Duration::from_secs(15) / 10,
            request_timeout: Duration::from_secs(30),
        }
    }
}
