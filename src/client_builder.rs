use crate::client::{Client, ClientImpl};
use crate::client_options::OxiaClientOptions;
use crate::errors::OxiaError;
use std::time::Duration;

#[derive(Debug, Clone, Default)]
pub struct OxiaClientBuilder {
    service_address: Option<String>,
    namespace: Option<String>,
    identity: Option<String>,
    batch_linger: Option<Duration>,
    batch_max_size: Option<u32>,
}

impl OxiaClientBuilder {
    pub fn new() -> Self {
        OxiaClientBuilder::default()
    }

    pub fn service_address(mut self, service_address: String) -> Self {
        self.service_address = Some(service_address);
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

    pub async fn build(self) -> Result<impl Client, OxiaError> {
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
        ClientImpl::new(options).await
    }
}
