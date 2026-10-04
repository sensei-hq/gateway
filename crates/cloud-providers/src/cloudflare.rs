//! Cloudflare Workers AI — System One decision models `clef` / `clef-flash`
//! (SP-DEC-2). Stub until T3 lands.

use async_trait::async_trait;
use reqwest::Client;

use kernel::types::config::RouterConfig;
use kernel::types::error::GatewayError;
use kernel::types::io::{DecisionRequest, DecisionResponse};

/// Decision-only adapter for Cloudflare Workers AI.
pub struct CloudflareAdapter {
    client: Client,
    id: String,
}

impl CloudflareAdapter {
    pub fn new() -> Result<Self, GatewayError> {
        Ok(Self {
            client: Client::new(),
            id: "cloudflare".into(),
        })
    }

    pub fn from_config(config: &RouterConfig) -> Result<Self, GatewayError> {
        Ok(Self {
            client: crate::base::build_client(config)?,
            id: "cloudflare".into(),
        })
    }
}

impl kernel::adapters::capability::Model for CloudflareAdapter {
    fn id(&self) -> &str {
        &self.id
    }
}

#[async_trait]
impl kernel::adapters::capability::DecisionModel for CloudflareAdapter {
    async fn decide(
        &self,
        _config: &RouterConfig,
        _req: &DecisionRequest,
    ) -> Result<DecisionResponse, GatewayError> {
        let _ = &self.client;
        Err(GatewayError::Unsupported {
            adapter: self.id.clone(),
            what: "decision (stub)".into(),
        })
    }
}

#[async_trait]
impl kernel::adapters::RegisterInto for CloudflareAdapter {
    async fn register_into(self: std::sync::Arc<Self>, reg: &kernel::adapters::AdapterRegistry) {
        reg.register_decision(self).await;
    }
}
