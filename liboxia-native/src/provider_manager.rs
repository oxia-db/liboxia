use crate::errors::OxiaError;
use crate::errors::OxiaError::UnexpectedStatus;
use crate::oxia::oxia_client_client::OxiaClientClient;
use dashmap::DashMap;
use std::sync::Arc;
use sync::Mutex;
use tokio::sync;
use tonic::transport::Channel;

pub struct ProviderManager {
    providers: Arc<DashMap<String, Arc<Mutex<OxiaClientClient<Channel>>>>>,
    creation_lock: Arc<Mutex<()>>,
}

impl ProviderManager {
    pub async fn get_provider(
        &self,
        address: &str,
    ) -> Result<Arc<Mutex<OxiaClientClient<Channel>>>, OxiaError> {
        if let Some(client) = self.providers.get(address) {
            return Ok(client.clone());
        }
        let _guard = self.creation_lock.lock().await;
        // double-check
        if let Some(client) = self.providers.get(address) {
            return Ok(client.clone());
        }
        let client = Arc::new(Mutex::new(
            OxiaClientClient::connect(address.to_string())
                .await
                .map_err(|err| UnexpectedStatus(err.to_string()))?,
        ));
        let entry = self.providers.entry(address.to_string()).or_insert(client);
        Ok(entry.clone())
    }

    pub fn new() -> ProviderManager {
        ProviderManager {
            providers: Arc::new(DashMap::new()),
            creation_lock: Arc::new(Mutex::new(())),
        }
    }
}
