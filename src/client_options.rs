use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct OxiaClientOptions {
    pub service_address: String,
    pub namespace: String,
    pub identity: String,
    pub batch_linger: Duration,
    pub batch_max_size: u32,
}

impl Default for OxiaClientOptions {
    fn default() -> Self {
        Self {
            service_address: String::from("http://127.0.0.1:6648"),
            namespace: String::from("default"),
            identity: Uuid::new_v4().to_string(),
            batch_linger: Duration::from_millis(5),
            batch_max_size: 128 * 1024,
        }
    }
}
