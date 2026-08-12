//! Config-source backends (SP-2): the filesystem loader + an in-memory source.
//! `FilesystemConfigSource` isolates ALL md/JSON parsing; the seam itself
//! (`ConfigSource`) is domain-typed, so a DB/HTTP backend drops in unchanged.

use std::path::{Path, PathBuf};

use orchestrator_core::{
    AgentDefinition, ChainBinding, ConfigSource, OrchestratorError, RegistryConfig, SkillDef,
    ToolSpec,
};

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

/// A filesystem `ConfigSource`: reads `<root>/agents/*.md`, `<root>/skills/*.md`,
/// `<root>/tools/*.json` into a `RegistryConfig`. ALL md/JSON parsing is isolated
/// here (the seam is domain-typed). Uses blocking `std::fs` inside the async
/// method — a one-shot startup read, not a hot path (keeps this crate tokio-free).
pub struct FilesystemConfigSource {
    root: PathBuf,
}

impl FilesystemConfigSource {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

/// Read `<root>/<sub>/*.<ext>` in sorted filename order → `(filename, content)`.
/// A missing subdir yields an empty Vec; any other I/O error is loud.
fn read_dir_files(
    root: &Path,
    sub: &str,
    ext: &str,
) -> Result<Vec<(String, String)>, OrchestratorError> {
    let dir = root.join(sub);
    let entries = match std::fs::read_dir(&dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => {
            return Err(OrchestratorError::RegistryLoad(format!(
                "read {}: {e}",
                dir.display()
            )));
        }
    };
    // Propagate per-entry errors loudly (never silently drop a config file mid-scan).
    let mut paths: Vec<PathBuf> = Vec::new();
    for entry in entries {
        let entry = entry
            .map_err(|e| OrchestratorError::RegistryLoad(format!("read {}: {e}", dir.display())))?;
        let p = entry.path();
        if p.extension().and_then(|x| x.to_str()) == Some(ext) {
            paths.push(p);
        }
    }
    paths.sort();
    let mut out = Vec::with_capacity(paths.len());
    for p in paths {
        let content = std::fs::read_to_string(&p)
            .map_err(|e| OrchestratorError::RegistryLoad(format!("read {}: {e}", p.display())))?;
        let name = p
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default()
            .to_string();
        out.push((name, content));
    }
    Ok(out)
}

/// Read an optional top-level `<root>/<name>` file. Missing → `None`; any other
/// I/O error is loud.
fn read_optional_file(root: &Path, name: &str) -> Result<Option<String>, OrchestratorError> {
    let path = root.join(name);
    match std::fs::read_to_string(&path) {
        Ok(s) => Ok(Some(s)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(OrchestratorError::RegistryLoad(format!(
            "read {}: {e}",
            path.display()
        ))),
    }
}

#[async_trait::async_trait]
impl ConfigSource for FilesystemConfigSource {
    async fn load(&self) -> Result<RegistryConfig, OrchestratorError> {
        if !self.root.exists() {
            return Err(OrchestratorError::RegistryLoad(format!(
                "config root not found: {}",
                self.root.display()
            )));
        }
        let mut cfg = RegistryConfig::default();
        for (file, md) in read_dir_files(&self.root, "agents", "md")? {
            cfg.agents
                .push(AgentDefinition::from_frontmatter(&md).map_err(|e| {
                    OrchestratorError::RegistryLoad(format!("parse agent {file}: {e}"))
                })?);
        }
        for (file, md) in read_dir_files(&self.root, "skills", "md")? {
            cfg.skills
                .push(SkillDef::from_frontmatter(&md).map_err(|e| {
                    OrchestratorError::RegistryLoad(format!("parse skill {file}: {e}"))
                })?);
        }
        for (file, json) in read_dir_files(&self.root, "tools", "json")? {
            cfg.tools.push(
                serde_json::from_str::<ToolSpec>(&json).map_err(|e| {
                    OrchestratorError::RegistryLoad(format!("parse tool {file}: {e}"))
                })?,
            );
        }
        if let Some(json) = read_optional_file(&self.root, "chains.json")? {
            cfg.chain_bindings = serde_json::from_str::<Vec<ChainBinding>>(&json)
                .map_err(|e| OrchestratorError::RegistryLoad(format!("parse chains.json: {e}")))?;
        }
        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::{EffectClass, Registry, SkillDef};
    use std::path::{Path, PathBuf};

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
            chain_bindings: vec![],
        };
        let src = InMemoryConfigSource(cfg);
        let reg = Registry::from_config(src.load().await.unwrap()).unwrap();
        assert!(reg.skill("s").is_some());
    }

    fn write(dir: &Path, name: &str, content: &str) {
        std::fs::write(dir.join(name), content).unwrap();
    }

    /// A temp config dir with agents/skills/tools populated. Caller removes it.
    fn temp_config_root() -> PathBuf {
        let root = std::env::temp_dir().join(format!("sp2-cfg-{}", uuid::Uuid::new_v4()));
        for sub in ["agents", "skills", "tools"] {
            std::fs::create_dir_all(root.join(sub)).unwrap();
        }
        write(
            &root.join("agents"),
            "researcher.md",
            "---\nname: researcher\narea: research\nkind: reasoning\nchain: c\ntools: [calc]\nskills: [concise]\n---\nBe careful.\n",
        );
        write(
            &root.join("skills"),
            "concise.md",
            "---\nname: concise\n---\nBe terse.\n",
        );
        write(
            &root.join("tools"),
            "calc.json",
            r#"{"name":"calc","description":"adds","input_schema":{"type":"object"},"effect_class":"Pure","ttl_secs":null,"source":null}"#,
        );
        root
    }

    #[tokio::test]
    async fn filesystem_source_loads_agents_skills_and_tools() {
        let root = temp_config_root();
        let cfg = FilesystemConfigSource::new(&root)
            .load()
            .await
            .expect("loads");
        let reg = Registry::from_config(cfg).expect("valid");
        assert!(reg.agent("researcher").is_some());
        assert!(reg.skill("concise").is_some());
        assert_eq!(
            reg.tool("calc").expect("calc tool").effect_class,
            EffectClass::Pure
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn missing_subdir_is_empty_not_an_error() {
        let root = std::env::temp_dir().join(format!("sp2-cfg-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(root.join("skills")).unwrap(); // no agents/ or tools/
        write(&root.join("skills"), "s.md", "---\nname: s\n---\nb\n");
        let cfg = FilesystemConfigSource::new(&root)
            .load()
            .await
            .expect("loads");
        assert_eq!(cfg.agents.len(), 0);
        assert_eq!(cfg.tools.len(), 0);
        assert_eq!(cfg.skills.len(), 1);
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn missing_root_is_loud() {
        let root = std::env::temp_dir().join(format!("sp2-nope-{}", uuid::Uuid::new_v4()));
        assert!(matches!(
            FilesystemConfigSource::new(&root).load().await,
            Err(OrchestratorError::RegistryLoad(_))
        ));
    }

    #[tokio::test]
    async fn malformed_tool_json_is_a_loud_registry_load_error() {
        let root = temp_config_root();
        write(&root.join("tools"), "bad.json", "{ not valid json");
        let err = FilesystemConfigSource::new(&root).load().await;
        assert!(
            matches!(&err, Err(OrchestratorError::RegistryLoad(m)) if m.contains("bad.json")),
            "got {err:?}"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn filesystem_loads_chain_bindings_from_chains_json() {
        let root = temp_config_root();
        write(
            &root,
            "chains.json",
            r#"[{"area":"research","kind":"reasoning","chain":"research.bulk"}]"#,
        );
        let cfg = FilesystemConfigSource::new(&root)
            .load()
            .await
            .expect("loads");
        assert_eq!(cfg.chain_bindings.len(), 1);
        assert_eq!(cfg.chain_bindings[0].area, "research");
        assert_eq!(cfg.chain_bindings[0].chain, "research.bulk");
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn missing_chains_json_is_an_empty_table() {
        let root = temp_config_root(); // has no chains.json
        let cfg = FilesystemConfigSource::new(&root)
            .load()
            .await
            .expect("loads");
        assert!(cfg.chain_bindings.is_empty());
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn malformed_chains_json_is_a_loud_registry_load_error() {
        let root = temp_config_root();
        write(&root, "chains.json", "{ not an array");
        let err = FilesystemConfigSource::new(&root).load().await;
        assert!(
            matches!(&err, Err(OrchestratorError::RegistryLoad(m)) if m.contains("chains.json")),
            "got {err:?}"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn malformed_agent_md_error_names_the_file() {
        let root = temp_config_root();
        // Missing the required `name:` key → a parse error that must name the file.
        write(
            &root.join("agents"),
            "broken.md",
            "---\narea: a\nkind: k\n---\nb\n",
        );
        let err = FilesystemConfigSource::new(&root).load().await;
        assert!(
            matches!(&err, Err(OrchestratorError::RegistryLoad(m)) if m.contains("broken.md")),
            "a malformed agent .md error names the offending file: {err:?}"
        );
        std::fs::remove_dir_all(&root).ok();
    }
}
