use crate::client::{Client, ClientImpl};
use crate::client_options::OxiaClientOptions;
use crate::errors::OxiaError;

#[derive(Debug, Clone, Default)]
pub struct OxiaClientBuilder {
    service_address: Option<String>,
    namespace: Option<String>,
    identity: Option<String>,
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
        ClientImpl::new(options).await
    }
}
