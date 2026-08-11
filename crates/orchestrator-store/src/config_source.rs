//! Config-source backends (SP-2): the filesystem loader + an in-memory source.
//! `FilesystemConfigSource` isolates ALL md/JSON parsing; the seam itself
//! (`ConfigSource`) is domain-typed, so a DB/HTTP backend drops in unchanged.

use orchestrator_core::{ConfigSource, OrchestratorError, RegistryConfig};

/// A `ConfigSource` returning a fixed `RegistryConfig` — for tests + programmatic
/// config, and the vehicle for exercising `Registry::from_config` off-disk.
#[derive(Clone, Default)]
pub struct InMemoryConfigSource(pub RegistryConfig);

#[async_trait::async_trait]
impl ConfigSource for InMemoryConfigSource {
    async fn load(&self) -> Result<RegistryConfig, OrchestratorError> {
        Ok(self.0.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::{Registry, SkillDef};

    #[tokio::test]
    async fn in_memory_source_round_trips_through_from_config() {
        let cfg = RegistryConfig {
            agents: vec![],
            skills: vec![SkillDef {
                name: "s".into(),
                description: None,
                body: "b".into(),
            }],
            tools: vec![],
        };
        let src = InMemoryConfigSource(cfg);
        let reg = Registry::from_config(src.load().await.unwrap()).unwrap();
        assert!(reg.skill("s").is_some());
    }
}
