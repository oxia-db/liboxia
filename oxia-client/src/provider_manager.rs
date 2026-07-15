use crate::errors::OxiaError;
use crate::oxia::oxia_client_client::OxiaClientClient;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::OnceCell;
use tonic::transport::Channel;

pub struct ProviderManager {
    providers: Arc<DashMap<String, OnceCell<OxiaClientClient<Channel>>>>,
}

impl ProviderManager {
    pub async fn get_provider(
        &self,
        address: String,
    ) -> Result<OxiaClientClient<Channel>, OxiaError> {
        let once_cell = self.providers.entry(address.clone()).or_default();
        let client = once_cell
            .get_or_try_init(|| async {
                let client = OxiaClientClient::connect(address).await?;
                Ok::<OxiaClientClient<Channel>, OxiaError>(client.clone())
            })
            .await?
            .clone();
        Ok(client)
    }

    pub fn new() -> ProviderManager {
        ProviderManager {
            providers: Arc::new(DashMap::new()),
        }
    }

    pub async fn shutdown(self) -> Result<(), OxiaError> {
        self.providers.clear();
        Ok(())
    }
}
