//! `bench.toml`: the models under test, each becoming its own single-entry
//! gateway chain so a run exercises exactly one model, with no fallback.

use std::collections::HashMap;
use std::time::Duration;

use gateway::types::config::{
    ChainEntry, FallbackChainConfig, GatewayConfig, ModelConfig, RouterConfig,
};
use gateway::{Capability, GatewayBuilder};
use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct BenchConfig {
    pub models: Vec<ModelSpec>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ModelSpec {
    pub id: String,
    pub router: String,
    pub url: String,
    pub api_model_id: String,
    pub context_window: u32,
    #[serde(default)]
    pub api_key_env: Option<String>,
}

/// Router ids the facade registers a decision adapter for (the decision arms of
/// `register_cloud_from_config` in `crates/gateway/src/facade.rs`). The facade
/// silently skips any other id, so a mistyped router would build a config that
/// can never answer.
const DECISION_ROUTERS: &[&str] = &[
    "ollama",
    "openrouter",
    "typesafe",
    "cloudflare",
    "llamacpp",
    "sglang",
    "laya",
];

/// The router timeout of every case a run sends (`check` probes with a
/// shorter one).
pub const ROUTER_TIMEOUT: Duration = Duration::from_secs(120);

/// The id of the one-model chain built for `m`.
pub fn chain_id(m: &ModelSpec) -> String {
    format!("bench-{}", m.id)
}

impl BenchConfig {
    pub fn from_toml(s: &str) -> anyhow::Result<Self> {
        Ok(toml::from_str(s)?)
    }

    /// A gateway config holding only `m`: its router, the model, and a chain of
    /// that one model with no fallback triggers. Built through
    /// [`GatewayBuilder`], so the gateway's own validation rules apply; a failure
    /// names the model.
    pub fn gateway_config_for(&self, m: &ModelSpec) -> anyhow::Result<GatewayConfig> {
        if !DECISION_ROUTERS.contains(&m.router.as_str()) {
            anyhow::bail!(
                "model '{}': router '{}' has no decision adapter in the gateway facade (expected one of {})",
                m.id,
                m.router,
                DECISION_ROUTERS.join(", ")
            );
        }
        let router = RouterConfig {
            url: m.url.clone(),
            api_key_env: m.api_key_env.clone(),
            api_key: None,
            enabled: true,
            timeout_ms: Some(ROUTER_TIMEOUT.as_millis() as u64),
            headers: HashMap::new(),
        };
        let model = ModelConfig {
            id: m.id.clone(),
            api_model_id: Some(m.api_model_id.clone()),
            provider: m.router.clone(),
            family: None,
            capabilities: vec![Capability::Decision],
            context_window: m.context_window,
            max_output_tokens: 1,
            pricing: None,
            catalog: None,
        };
        let chain = FallbackChainConfig {
            id: chain_id(m),
            capability: Capability::Decision,
            models: vec![ChainEntry {
                model: m.id.clone(),
                router: Some(m.router.clone()),
                api_model_id: None,
                priority: 1,
            }],
            fallback_triggers: vec![],
        };
        GatewayBuilder::new()
            .add_router(&m.router, router)
            .add_model(model)
            .add_chain(chain)
            .build()
            .map_err(|errors| {
                anyhow::anyhow!(
                    "model '{}': invalid gateway config: {}",
                    m.id,
                    errors.join("; ")
                )
            })
    }
}

/// Where [`BenchConfig::load`] found the config.
#[derive(Debug, Clone, PartialEq)]
pub struct Loaded {
    pub config: BenchConfig,
    pub path: std::path::PathBuf,
    /// `bench.toml` was missing, so `bench.example.toml` was used.
    pub is_example: bool,
}

impl BenchConfig {
    /// Load `explicit` (relative paths resolve under `crate_dir`), or else
    /// `crate_dir/bench.toml`, or else `crate_dir/bench.example.toml`. Only an
    /// unreadable or unparsable file is an error; model entries are checked by
    /// `check`.
    pub fn load(
        explicit: Option<&std::path::Path>,
        crate_dir: &std::path::Path,
    ) -> anyhow::Result<Loaded> {
        use anyhow::Context;
        let (path, is_example) = match explicit {
            Some(p) => (resolve_under(p, crate_dir), false),
            None => {
                let own = crate_dir.join("bench.toml");
                if own.exists() {
                    (own, false)
                } else {
                    (crate_dir.join("bench.example.toml"), true)
                }
            }
        };
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("reading config {}", path.display()))?;
        // Structural errors only: a model entry the gateway rejects is
        // reported NOT RUN by `check`, so one bad entry cannot stop the rest.
        let config =
            Self::from_toml(&text).with_context(|| format!("parsing config {}", path.display()))?;
        Ok(Loaded {
            config,
            path,
            is_example,
        })
    }
}

/// `p` if absolute, else `p` under `dir`.
pub fn resolve_under(p: &std::path::Path, dir: &std::path::Path) -> std::path::PathBuf {
    if p.is_absolute() {
        p.to_path_buf()
    } else {
        dir.join(p)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gateway::FacadeBuilder;

    fn example() -> BenchConfig {
        BenchConfig::from_toml(include_str!("../bench.example.toml")).unwrap()
    }

    #[test]
    fn the_example_config_is_the_spec_roster_with_single_entry_chains() {
        let cfg = example();
        let roster: Vec<_> = cfg
            .models
            .iter()
            .map(|m| {
                (
                    m.id.as_str(),
                    m.router.as_str(),
                    m.url.as_str(),
                    m.api_model_id.as_str(),
                    m.context_window,
                )
            })
            .collect();
        let laya = "http://127.0.0.1:8766";
        assert_eq!(
            roster,
            vec![
                ("laya-english", "laya", laya, "english", 512),
                ("laya-multilingual", "laya", laya, "multilingual", 1024),
                ("laya-typed", "laya", laya, "typed-decisions", 1024),
                ("laya-ane", "laya", laya, "ane", 96),
                ("nimble", "ollama", "http://127.0.0.1:11434", "nimble", 8192),
                (
                    "nimble-v3",
                    "llamacpp",
                    "http://127.0.0.1:8091",
                    "nimble-v3",
                    8192
                ),
            ]
        );
        for m in &cfg.models {
            let g = cfg.gateway_config_for(m).unwrap();
            let chain = &g.chains[&chain_id(m)];
            assert_eq!(chain.models.len(), 1, "{}: one model per chain", m.id);
            assert!(chain.fallback_triggers.is_empty(), "{}", m.id);
            assert_eq!(g.routers.len(), 1, "{}: only its own router", m.id);
        }
    }

    #[tokio::test]
    async fn every_example_router_is_a_decision_adapter_the_facade_registers() {
        let cfg = example();
        for m in &cfg.models {
            let builder = FacadeBuilder::new(cfg.gateway_config_for(m).unwrap());
            // `build` registers the cloud adapters into the shared registry; keep
            // the clone to look the decision adapter up afterwards.
            let registry = builder.registry().clone();
            let _facade = builder.build().await;
            assert!(
                registry.decision(&m.router).await.is_some(),
                "{}: facade registered no decision adapter for router '{}'",
                m.id,
                m.router
            );
        }
    }

    #[test]
    fn a_router_the_facade_does_not_register_is_an_error_naming_model_and_router() {
        let cfg = example();
        let mut m = cfg.models[5].clone();
        m.router = "llama_cpp".into();
        let err = cfg.gateway_config_for(&m).unwrap_err().to_string();
        assert!(err.contains("nimble-v3"), "must name the model: {err}");
        assert!(err.contains("llama_cpp"), "must name the router: {err}");
    }

    #[test]
    fn every_example_model_passes_gateway_validation() {
        let cfg = example();
        for m in &cfg.models {
            cfg.gateway_config_for(m)
                .unwrap_or_else(|e| panic!("{}: {e:#}", m.id));
        }
    }

    const ONE: &str = r#"
[[models]]
id = "solo"
router = "llamacpp"
url = "http://127.0.0.1:8091"
api_model_id = "nimble-v3"
context_window = 8192
"#;

    #[test]
    fn load_prefers_bench_toml_then_falls_back_to_the_example() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("bench.example.toml"),
            include_str!("../bench.example.toml"),
        )
        .unwrap();
        let l = BenchConfig::load(None, dir.path()).unwrap();
        assert!(l.is_example);
        assert_eq!(l.path, dir.path().join("bench.example.toml"));
        assert_eq!(l.config.models.len(), 6);

        std::fs::write(dir.path().join("bench.toml"), ONE).unwrap();
        let l = BenchConfig::load(None, dir.path()).unwrap();
        assert!(!l.is_example);
        assert_eq!(l.path, dir.path().join("bench.toml"));
        assert_eq!(l.config.models[0].id, "solo");
    }

    #[test]
    fn an_explicit_config_resolves_under_the_crate_dir_and_must_exist() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("mine.toml"), ONE).unwrap();
        let l = BenchConfig::load(Some(std::path::Path::new("mine.toml")), dir.path()).unwrap();
        assert_eq!(l.path, dir.path().join("mine.toml"));
        assert!(!l.is_example);
        let err =
            BenchConfig::load(Some(std::path::Path::new("gone.toml")), dir.path()).unwrap_err();
        assert!(format!("{err:#}").contains("gone.toml"), "{err:#}");
    }

    /// T7 review M3: one bad model entry must not abort every model; `check`
    /// reports it NOT RUN. Only structural errors fail the load.
    #[test]
    fn load_keeps_an_invalid_model_and_rejects_unparsable_toml_naming_the_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("bench.toml"),
            ONE.replace("llamacpp", "nope"),
        )
        .unwrap();
        let l = BenchConfig::load(None, dir.path()).unwrap();
        assert_eq!(l.config.models[0].router, "nope");
        std::fs::write(dir.path().join("bench.toml"), "models = 3").unwrap();
        let err = format!("{:#}", BenchConfig::load(None, dir.path()).unwrap_err());
        assert!(err.contains("bench.toml"), "{err}");
    }

    #[test]
    fn an_invalid_model_is_an_error_naming_the_model() {
        let cfg = example();
        let mut m = cfg.models[0].clone();
        m.url = String::new();
        let err = cfg.gateway_config_for(&m).unwrap_err().to_string();
        assert!(
            err.contains("laya-english"),
            "error must name the model: {err}"
        );
        assert!(
            err.contains("empty URL"),
            "error must carry the reason: {err}"
        );
    }
}
