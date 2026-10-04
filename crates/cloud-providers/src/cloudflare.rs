//! Cloudflare Workers AI — System One decision models `clef` / `clef-flash`
//! (SP-DEC-2).
//!
//! Workers AI serves them only at `POST
//! https://api.cloudflare.com/client/v4/accounts/<account_id>/ai/run/@cf/cloudflare/<model>`
//! — there is no `/v1/systemone` — so this adapter speaks the shared System One
//! core in its [`Dialect::WorkersAi`](crate::systemone) form. Configure the
//! router with `url = "https://api.cloudflare.com/client/v4/accounts/<account_id>/ai"`
//! (the account id is part of every path) and a Workers AI token (Read + Edit)
//! in `api_key` / `api_key_env`.
//!
//! **Long state is truncated silently by Workers AI.** Seed the models'
//! `context_window` (65,536) so the engine's window gate — which counts the
//! state, every question, the prompt framing and each image — never routes a
//! request here that Cloudflare would truncate.

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
        config: &RouterConfig,
        req: &DecisionRequest,
    ) -> Result<DecisionResponse, GatewayError> {
        let Some(model) = req.model.as_deref() else {
            return Err(GatewayError::InvalidRequest {
                message: format!(
                    "a decision call to '{}' needs a model (clef or clef-flash)",
                    self.id
                ),
            });
        };
        let base_url = config.url.trim_end_matches('/');
        if base_url.is_empty() {
            return Err(GatewayError::InvalidConfig(format!(
                "router '{}' needs url = https://api.cloudflare.com/client/v4/accounts/<account_id>/ai",
                self.id
            )));
        }
        let host = crate::systemone::Host {
            adapter: &self.id,
            dialect: crate::systemone::Dialect::WorkersAi,
            images: crate::systemone::ImageEncoding::DataUrl,
        };
        crate::systemone::decide(&self.client, base_url, host, model, config, req)
            .await?
            .map_err(|r| {
                let message = r.message();
                r.into_error(&self.id, message)
            })
    }
}

#[async_trait]
impl kernel::adapters::RegisterInto for CloudflareAdapter {
    async fn register_into(self: std::sync::Arc<Self>, reg: &kernel::adapters::AdapterRegistry) {
        reg.register_decision(self).await;
    }
}
