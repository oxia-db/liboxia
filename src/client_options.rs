use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct OxiaClientOptions {
    pub service_address: String,
    pub namespace: String,
    pub identity: String,
}

impl Default for OxiaClientOptions {
    fn default() -> Self {
        Self {
            service_address: String::from("http://127.0.0.1:6648"),
            namespace: String::from("default"),
            identity: Uuid::new_v4().to_string(),
        }
    }
}
