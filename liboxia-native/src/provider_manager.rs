use crate::errors::OxiaError;
use crate::errors::OxiaError::UnexpectedStatus;
use crate::oxia::oxia_client_client::OxiaClientClient;
use dashmap::mapref::one::RefMut;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, OnceCell};
use tonic::transport::Channel;

pub struct ProviderManager {
    providers: Arc<DashMap<String, OnceCell<Arc<Mutex<OxiaClientClient<Channel>>>>>>,
}

impl ProviderManager {
    pub async fn get_provider(
        &self,
        address: String,
    ) -> Result<Arc<Mutex<OxiaClientClient<Channel>>>, OxiaError> {
        let once_cell = self
            .providers
            .entry(address.clone())
            .or_insert_with(|| OnceCell::new());
        let client = once_cell
            .get_or_try_init(|| async {
                let client = OxiaClientClient::connect(address)
                    .await
                    .map_err(|err| UnexpectedStatus(err.to_string()))?;
                Ok::<Arc<Mutex<OxiaClientClient<Channel>>>, OxiaError>(Arc::new(Mutex::new(client)))
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
}
