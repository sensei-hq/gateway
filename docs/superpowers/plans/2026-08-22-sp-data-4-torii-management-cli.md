# SP-DATA-4 — `torii` management CLI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `torii`, the workspace's first binary — an operator control plane that observes/intervenes on runs, runs the worker that actually drives due wakes, and owns the durable config write path — closing both SP-DATA-2 carry-forwards in the process.

**Architecture:** A new binary crate `crates/torii` holds all the wiring (`boot.rs`) so the libraries stay injection-only. Every operator decision lives in a **pure, DB-free function** (`diff`, `plan_push`, `serve`'s loop over a `Ticker` trait, error→exit-code mapping) with the Postgres adapters only at the edges — so most acceptance criteria are unit tests, not Docker tests. `orchestrator-core` gains one **defaulted** `ConfigSource::load_versioned` so fixing the config TOCTOU fixes `RegistryHandle::reload` too, not just the CLI.

**Tech Stack:** Rust 2024, `clap` 4 (derive), `sqlx` 0.8 (runtime queries, no compile-time DB), `tokio`, `chrono`, existing `orchestrator`/`orchestrator-core`/`orchestrator-store` crates, Docker `postgres:16` for the DB-backed tests.

**Spec:** `docs/superpowers/specs/2026-08-22-sp-data-4-torii-management-cli-design.md`

---

## File Structure

```
crates/torii/Cargo.toml            package sensei-torii, [[bin]] torii
crates/torii/src/main.rs           clap parse -> dispatch -> process::exit
crates/torii/src/errors.rs         CliError (message + exit code), redact_url
crates/torii/src/diff.rs           pure RegistryConfig diff — the guard on a destructive write
crates/torii/src/render.rs         ScheduledRun -> human table | JSON
crates/torii/src/boot.rs           env validation (pure) + two-tier dep construction
crates/torii/src/cmd/mod.rs        re-exports + the shared Outcome type
crates/torii/src/cmd/run.rs        status · list-paused · cancel · wake · submit
crates/torii/src/cmd/config.rs     plan_push (pure) · version · push
crates/torii/src/cmd/worker.rs     Ticker trait · serve loop · parse_interval
crates/torii/tests/cli.rs          binary smoke tests via CARGO_BIN_EXE_torii
crates/torii/tests/e2e_pg.rs       cross-process operator loop (DATABASE_URL-guarded)
```

Modified: `Cargo.toml` (workspace members), `crates/orchestrator-core/src/registry.rs`,
`crates/orchestrator-store/src/postgres.rs`, `crates/orchestrator-store/Cargo.toml`,
`crates/orchestrator/Cargo.toml`, `crates/orchestrator/src/lib.rs`,
`docs/superpowers/orchestrator-overview.md`.

**Why commands take `&dyn SchedulerStore` and not `&Scheduler`:** `InMemorySchedulerStore` already
exists and is exported from `orchestrator_store` (no `postgres` feature needed), so every
honest-reporting acceptance criterion becomes a **DB-free unit test**. Only `submit` needs the full
`Scheduler`.

**Why `torii` needs no `postgres` feature of its own:** it depends on `orchestrator-store` with
`postgres` **on unconditionally** (a management CLI for a Postgres control plane needs Postgres), so
`sqlx` is always compiled here. Its DB tests are therefore guarded by **`DATABASE_URL` presence
alone** — no feature gate — matching the store crate's skip-if-absent convention.

---

## Docker Postgres harness (needed from Task 5 onward)

Run once per session; every DB-touching step below assumes `DATABASE_URL` is exported.

```bash
PGPW=$(openssl rand -hex 12)
docker run -d --name torii-pg -e POSTGRES_PASSWORD="$PGPW" -p 5433:5432 postgres:16
until docker exec torii-pg pg_isready -U postgres >/dev/null 2>&1; do sleep 1; done
docker exec -i torii-pg psql -U postgres -v ON_ERROR_STOP=1 < database/_apply_all.sql
export DATABASE_URL="postgres://postgres:${PGPW}@localhost:5433/postgres"
echo "$DATABASE_URL" | sed 's/:[^:@]*@/:***@/'   # confirm without printing the password
```

Teardown: `docker rm -f torii-pg`.

---

## Task 1: Crate skeleton + `errors.rs`

**Files:**
- Create: `crates/torii/Cargo.toml`, `crates/torii/src/main.rs`, `crates/torii/src/errors.rs`
- Modify: `Cargo.toml` (workspace `members`)

- [ ] **Step 1: Add the crate to the workspace**

In `Cargo.toml`, extend `members` with `"crates/torii"`:

```toml
members = ["crates/kernel", "crates/cloud-providers", "crates/gateway", "crates/local-engine", "crates/local-providers", "crates/kokoro", "crates/vault", "crates/orchestrator-core", "crates/orchestrator", "crates/orchestrator-store", "crates/torii"]
```

- [ ] **Step 2: Create `crates/torii/Cargo.toml`**

```toml
[package]
name = "sensei-torii"
version = "0.1.0"
edition = "2024"
description = "torii: the operator control plane for the sensei orchestrator"
license = "MIT"

[[bin]]
name = "torii"
path = "src/main.rs"

[dependencies]
orchestrator-core = { package = "sensei-orchestrator-core", path = "../orchestrator-core" }
orchestrator = { package = "sensei-orchestrator", path = "../orchestrator" }
orchestrator-store = { package = "sensei-orchestrator-store", path = "../orchestrator-store", features = ["postgres"] }
gateway = { package = "sensei-gateway", path = "../gateway" }
kernel = { package = "sensei-kernel", path = "../kernel" }
async-trait = "0.1"
chrono = { version = "0.4", features = ["serde"] }
clap = { version = "4", features = ["derive"] }
serde_json = "1"
sqlx = { version = "0.8", features = ["runtime-tokio", "postgres"] }
tokio = { version = "1", features = ["full"] }
tracing = "0.1"
uuid = { version = "1", features = ["v4", "serde"] }

[dev-dependencies]
tokio = { version = "1", features = ["full", "test-util"] }
```

- [ ] **Step 3: Write the failing tests for `errors.rs`**

Create `crates/torii/src/errors.rs`:

```rust
//! Operator-facing failures: an actionable message plus the process exit code.
//! Every loud error the stack produces is MAPPED here, never flattened into
//! "something went wrong" — the whole point of the SP-DATA-1 taxonomy is that an
//! operator can tell a transport fault from a config-drift fence refusal.

use orchestrator_core::{JournalError, OrchestratorError};

/// Exit codes: 0 did it, 1 error, 2 not-found or precondition-not-met.
pub const EXIT_OK: i32 = 0;
pub const EXIT_ERROR: i32 = 1;
pub const EXIT_PRECONDITION: i32 = 2;

#[derive(Debug, PartialEq)]
pub struct CliError {
    pub message: String,
    pub code: i32,
}

impl CliError {
    pub fn error(message: impl Into<String>) -> Self {
        Self { message: message.into(), code: EXIT_ERROR }
    }
    pub fn precondition(message: impl Into<String>) -> Self {
        Self { message: message.into(), code: EXIT_PRECONDITION }
    }
}

impl From<OrchestratorError> for CliError {
    fn from(e: OrchestratorError) -> Self {
        let message = match &e {
            OrchestratorError::VersionFenceMismatch { recorded, current } => format!(
                "this run's config generation drifted: recorded {recorded}, current {current}.\n\
                 The run cannot resume under different config. Check `torii config version`."
            ),
            // No "journal format mismatch:" prefix — the variant's own Display already
            // says "incompatible journal format for run ...", so a prefix repeats it.
            OrchestratorError::Journal(JournalError::IncompatibleFormat { .. }) => format!(
                "{e}.\nThis binary cannot safely fold this run's journal. Do not continue."
            ),
            OrchestratorError::Store(m) => format!("store transport fault: {m}"),
            OrchestratorError::Journal(JournalError::Backend(m)) => {
                format!("journal transport fault: {m}")
            }
            OrchestratorError::RegistryLoad(m) => format!("config is not loadable: {m}"),
            other => other.to_string(),
        };
        Self::error(message)
    }
}

/// Strip credentials from a Postgres URL so a connect failure can be reported
/// without leaking the password into logs, terminals, or CI output.
/// Returns `host[:port]/dbname`, or a fixed placeholder if the URL is unparseable
/// (never the input — an unparseable URL may still contain the password).
///
/// Both splits deliberately tolerate the delimiter appearing INSIDE the password,
/// which is common in hand-built connection strings: `split_once("://")` keeps
/// everything after the FIRST scheme separator, and `rsplit_once('@')` cuts at the
/// LAST `@` (the real user/host boundary). Using `split_once('@')` here leaks a
/// suffix of a password that contains an `@`.
pub fn redact_url(url: &str) -> String {
    let after_scheme = url.split_once("://").map(|(_, rest)| rest).unwrap_or("");
    let host_and_path = match after_scheme.rsplit_once('@') {
        Some((_creds, rest)) => rest,
        None => after_scheme,
    };
    let trimmed = host_and_path.split(['?', '#']).next().unwrap_or("");
    if trimmed.is_empty() {
        return "<unparseable database url>".to_string();
    }
    trimmed.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The password must never survive redaction. Assembled at runtime so the
    /// repo's Semgrep CWE-798 hook does not see a literal credential.
    fn url_with_password(pw: &str) -> String {
        format!("postgres://{}:{}@db.internal:5432/orch", "operator", pw)
    }

    #[test]
    fn redact_url_drops_the_password_and_keeps_host_and_db() {
        let pw = format!("s3cr{}t", "e");
        let out = redact_url(&url_with_password(&pw));
        assert_eq!(out, "db.internal:5432/orch");
        assert!(!out.contains(&pw), "password leaked: {out}");
        assert!(!out.contains("operator"), "user leaked: {out}");
    }

    #[test]
    fn redact_url_drops_query_parameters() {
        let pw = format!("p{}ss", "a");
        let url = format!("{}?sslmode=require", url_with_password(&pw));
        let out = redact_url(&url);
        assert_eq!(out, "db.internal:5432/orch");
        assert!(!out.contains(&pw));
    }

    #[test]
    fn redact_url_never_echoes_an_unparseable_url() {
        let pw = format!("l{}ak", "e");
        let out = redact_url(&pw);
        assert_eq!(out, "<unparseable database url>");
        assert!(!out.contains(&pw), "unparseable input echoed the secret: {out}");
    }

    /// A password containing `@` is common in hand-built connection strings. Splitting
    /// on the FIRST `@` misclassifies a suffix of the password as the host and returns
    /// it verbatim — so this asserts on that suffix, not just on the whole password
    /// (a bare `!contains(&pw)` passes even while a partial password leaks).
    #[test]
    fn redact_url_does_not_leak_a_password_containing_an_at_sign() {
        let leaky = format!("ss{}ord", "w");
        let pw = format!("p@{leaky}");
        let out = redact_url(&format!("postgres://operator:{pw}@db.internal:5432/orch"));
        assert_eq!(out, "db.internal:5432/orch");
        assert!(!out.contains(&pw), "whole password leaked: {out}");
        assert!(!out.contains(&leaky), "a password SUFFIX leaked: {out}");
    }

    /// Same defect class on the scheme split: `split("://").nth(1)` keeps only the
    /// second segment, so a `://` inside the password truncates the string and can
    /// return credential material.
    #[test]
    fn redact_url_does_not_leak_a_password_containing_a_scheme_separator() {
        let leaky = format!("ss{}ord", "w");
        let pw = format!("p://{leaky}");
        let out = redact_url(&format!("postgres://operator:{pw}@db.internal:5432/orch"));
        assert_eq!(out, "db.internal:5432/orch");
        assert!(!out.contains(&leaky), "a password fragment leaked: {out}");
        assert!(!out.contains("operator"), "the user leaked: {out}");
    }

    #[test]
    fn fence_mismatch_maps_to_an_actionable_message() {
        let e = OrchestratorError::VersionFenceMismatch {
            recorded: "v1#cfg7".into(),
            current: "v1#cfg8".into(),
        };
        let cli = CliError::from(e);
        assert_eq!(cli.code, EXIT_ERROR);
        assert!(cli.message.contains("v1#cfg7"), "{}", cli.message);
        assert!(cli.message.contains("v1#cfg8"), "{}", cli.message);
        assert!(cli.message.contains("config version"), "no next step: {}", cli.message);
    }

    #[test]
    fn store_and_journal_faults_are_distinguishable() {
        let s = CliError::from(OrchestratorError::Store("pool timeout".into()));
        let j = CliError::from(OrchestratorError::Journal(JournalError::Backend("conn reset".into())));
        assert!(s.message.starts_with("store transport fault"), "{}", s.message);
        assert!(j.message.starts_with("journal transport fault"), "{}", j.message);
    }
}
```

- [ ] **Step 4: Create a placeholder `main.rs` so the crate compiles**

```rust
//! `torii` — the operator control plane for the sensei orchestrator.
//! Task 10 replaces this with the clap dispatch.

mod errors;

fn main() {
    eprintln!("torii: not yet wired (see docs/superpowers/plans/2026-08-22-sp-data-4-torii-management-cli.md)");
    std::process::exit(errors::EXIT_ERROR);
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p sensei-torii`
Expected: 7 passed. (These are written-then-run together because `errors.rs` is a pure mapping module with no prior art to fail against; the assertions are the specification. The two password-delimiter tests are the exception — run them against the naive `split_once('@')`/`split("://").nth(1)` form FIRST and confirm they fail, since that is the bug they exist to pin.)

- [ ] **Step 6: Verify the whole workspace still builds and the existing suite is intact**

Run: `cargo test --workspace 2>&1 | tail -25`
Expected: all existing tests pass; the new crate contributes 5. **Read the real exit status** — do not judge from piped output alone:

Run: `cargo test --workspace > /tmp/t1.log 2>&1; echo "exit=$?"`
Expected: `exit=0`

- [ ] **Step 7: Commit**

```bash
cargo fmt --all
git add Cargo.toml crates/torii
git commit -m "feat(torii): SP-DATA-4 (1/11) — crate skeleton + error/exit-code mapping"
```

---

## Task 2: `diff.rs` — the pure guard on a destructive write

`PostgresConfigSource::store` is **replace-all**, so pushing an incomplete directory silently
deletes every entity not in it. This diff is what stands between an operator and that outcome, so it
gets real tests — including the case that matters most: an **empty incoming config must report
everything as removed**, not "no changes".

**Files:**
- Create: `crates/torii/src/diff.rs`
- Modify: `crates/torii/src/main.rs` (add `mod diff;`)

- [ ] **Step 1: Write the failing tests**

Create `crates/torii/src/diff.rs` with ONLY the test module plus the type signatures, so the tests
fail to compile against absent bodies:

```rust
//! A pure diff between the durable config and an incoming one. This is the guard
//! in front of a replace-all write, so it must never under-report a removal.

use orchestrator_core::RegistryConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntityKind {
    Agent,
    Skill,
    Tool,
    ChainBinding,
}

impl EntityKind {
    pub fn label(&self) -> &'static str {
        match self {
            EntityKind::Agent => "agent",
            EntityKind::Skill => "skill",
            EntityKind::Tool => "tool",
            EntityKind::ChainBinding => "chain",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffEntry {
    pub kind: EntityKind,
    pub name: String,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ConfigDiff {
    pub added: Vec<DiffEntry>,
    pub changed: Vec<DiffEntry>,
    pub removed: Vec<DiffEntry>,
    pub unchanged: usize,
}

impl ConfigDiff {
    /// Any removal requires explicit operator confirmation: a replace-all write
    /// makes a removal unrecoverable.
    pub fn requires_confirmation(&self) -> bool {
        !self.removed.is_empty()
    }

    pub fn is_noop(&self) -> bool {
        self.added.is_empty() && self.changed.is_empty() && self.removed.is_empty()
    }
}

pub fn diff(current: &RegistryConfig, incoming: &RegistryConfig) -> ConfigDiff {
    unimplemented!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::{Activation, ChainBinding, SkillDef};

    fn skill(name: &str, body: &str) -> SkillDef {
        SkillDef {
            name: name.into(),
            description: None,
            body: body.into(),
            activation: Activation::default(),
        }
    }

    fn cfg_with_skills(skills: Vec<SkillDef>) -> RegistryConfig {
        RegistryConfig { agents: vec![], skills, tools: vec![], chain_bindings: vec![] }
    }

    #[test]
    fn an_added_skill_is_reported_as_added() {
        let d = diff(&cfg_with_skills(vec![]), &cfg_with_skills(vec![skill("s", "b")]));
        assert_eq!(d.added, vec![DiffEntry { kind: EntityKind::Skill, name: "s".into() }]);
        assert!(d.changed.is_empty());
        assert!(d.removed.is_empty());
        assert!(!d.requires_confirmation(), "a pure addition needs no confirmation");
    }

    #[test]
    fn a_changed_body_is_reported_as_changed_not_added() {
        let d = diff(
            &cfg_with_skills(vec![skill("s", "old")]),
            &cfg_with_skills(vec![skill("s", "new")]),
        );
        assert_eq!(d.changed, vec![DiffEntry { kind: EntityKind::Skill, name: "s".into() }]);
        assert!(d.added.is_empty());
        assert!(d.removed.is_empty());
        assert_eq!(d.unchanged, 0);
    }

    #[test]
    fn an_identical_config_is_a_noop() {
        let c = cfg_with_skills(vec![skill("s", "b")]);
        let d = diff(&c, &c);
        assert!(d.is_noop(), "{d:?}");
        assert_eq!(d.unchanged, 1);
        assert!(!d.requires_confirmation());
    }

    /// THE case that matters: pushing an empty directory over a populated database
    /// is a total wipe. Reporting it as "no changes" would be catastrophic.
    #[test]
    fn an_empty_incoming_config_reports_everything_removed() {
        let current = RegistryConfig {
            agents: vec![],
            skills: vec![skill("a", "x"), skill("b", "y")],
            tools: vec![],
            chain_bindings: vec![ChainBinding {
                area: "research".into(),
                kind: "reasoning".into(),
                chain: "c".into(),
            }],
        };
        let d = diff(&current, &cfg_with_skills(vec![]));
        assert_eq!(d.removed.len(), 3, "2 skills + 1 binding must all be reported: {d:?}");
        assert!(d.added.is_empty());
        assert!(d.requires_confirmation(), "a total wipe MUST require confirmation");
    }

    #[test]
    fn a_chain_binding_is_keyed_by_area_and_kind() {
        let b = |chain: &str| ChainBinding {
            area: "research".into(),
            kind: "reasoning".into(),
            chain: chain.into(),
        };
        let current = RegistryConfig {
            agents: vec![],
            skills: vec![],
            tools: vec![],
            chain_bindings: vec![b("old")],
        };
        let incoming = RegistryConfig {
            agents: vec![],
            skills: vec![],
            tools: vec![],
            chain_bindings: vec![b("new")],
        };
        let d = diff(&current, &incoming);
        assert_eq!(
            d.changed,
            vec![DiffEntry { kind: EntityKind::ChainBinding, name: "research/reasoning".into() }],
            "same (area,kind) with a different chain is a CHANGE, not add+remove: {d:?}"
        );
    }
}
```

Add `mod diff;` to `main.rs`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sensei-torii diff`
Expected: FAIL — `not yet implemented` panic in all five tests (the `unimplemented!()` body).

- [ ] **Step 3: Implement `diff`**

Replace the `unimplemented!()` body. Entities are compared as `serde_json::Value` so no
`PartialEq` bound is needed on `AgentDefinition`/`SkillDef`/`ToolSpec`:

```rust
use std::collections::BTreeMap;

/// Index an entity set by a comparison KEY. The key is generic, not a display
/// string: joining a composite key into `"a/b"` is not injective when the parts
/// can contain the separator, and a collision there hides a REMOVAL behind a
/// spurious "changed" — leaving `removed` empty so `requires_confirmation()`
/// returns false and the replace-all write destroys a live entity with no prompt.
fn index<T, K>(items: &[T], key_of: impl Fn(&T) -> K) -> BTreeMap<K, serde_json::Value>
where
    T: serde::Serialize,
    K: Ord + std::fmt::Debug,
{
    let mut out = BTreeMap::new();
    for item in items {
        let v = serde_json::to_value(item).expect(
            "a config entity must serialize: every RegistryConfig type is a plain \
             derive(Serialize) struct with no non-string map keys and no float fields, \
             so to_value cannot fail. Coercing a failure to Null would make two \
             unserializable entities compare EQUAL and be reported 'unchanged' — a \
             silent under-report in the guard that prevents config loss.",
        );
        let k = key_of(item);
        // Duplicate keys are impossible on the real call path (`current` comes from
        // PK-backed tables, `incoming` is validated by Registry::from_config first),
        // but silent last-write-wins would UNDER-COUNT a removal.
        debug_assert!(!out.contains_key(&k), "duplicate config entity key: {k:?}");
        out.insert(k, v);
    }
    out
}

fn compare<K>(
    kind: EntityKind,
    current: &BTreeMap<K, serde_json::Value>,
    incoming: &BTreeMap<K, serde_json::Value>,
    display: impl Fn(&K) -> String,
    out: &mut ConfigDiff,
) where
    K: Ord + std::fmt::Debug,
{
    for (key, new_v) in incoming {
        match current.get(key) {
            None => out.added.push(DiffEntry { kind, name: display(key) }),
            Some(old_v) if old_v != new_v => {
                out.changed.push(DiffEntry { kind, name: display(key) })
            }
            Some(_) => out.unchanged += 1,
        }
    }
    for key in current.keys() {
        if !incoming.contains_key(key) {
            out.removed.push(DiffEntry { kind, name: display(key) });
        }
    }
}

/// Diff two configs. Names are assumed unique within each `Vec`: `current` gets
/// that from the PK-backed `config_*` tables, `incoming` from `Registry::from_config`
/// running first. `index`'s `debug_assert!` catches a caller that bypasses both.
pub fn diff(current: &RegistryConfig, incoming: &RegistryConfig) -> ConfigDiff {
    let mut out = ConfigDiff::default();
    let by_name = |n: &String| n.clone();
    compare(
        EntityKind::Agent,
        &index(&current.agents, |a| a.name.clone()),
        &index(&incoming.agents, |a| a.name.clone()),
        by_name,
        &mut out,
    );
    compare(
        EntityKind::Skill,
        &index(&current.skills, |s| s.name.clone()),
        &index(&incoming.skills, |s| s.name.clone()),
        by_name,
        &mut out,
    );
    compare(
        EntityKind::Tool,
        &index(&current.tools, |t| t.name.clone()),
        &index(&incoming.tools, |t| t.name.clone()),
        by_name,
        &mut out,
    );
    // A binding's identity is the (area, kind) TUPLE — the same key `Registry`
    // itself uses. The joined "area/kind" form is for DISPLAY only.
    let key = |b: &orchestrator_core::ChainBinding| (b.area.clone(), b.kind.clone());
    compare(
        EntityKind::ChainBinding,
        &index(&current.chain_bindings, key),
        &index(&incoming.chain_bindings, key),
        |(a, k): &(String, String)| format!("{a}/{k}"),
        &mut out,
    );
    out
}
```

Add three more tests: `a_chain_binding_key_collision_does_not_hide_a_removal` (the regression guard —
`("research/reasoning","x")` in `current` versus `("research","reasoning/x")` in `incoming` must
yield one `removed` + one `added` and `requires_confirmation() == true`, NOT a single `changed`), plus
one added-agent and one changed-tool test, since the `Agent`/`Tool` arms are copy-paste of the
`Skill` arm and a swapped field would otherwise compile and pass everything. Expected total after
this task: 15 crate tests.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p sensei-torii diff`
Expected: 8 passed (5 base + the collision guard + the agent and tool tests).

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/torii/src/diff.rs crates/torii/src/main.rs crates/torii/Cargo.toml
git commit -m "feat(torii): SP-DATA-4 (2/11) — pure config diff (the guard on a replace-all write)"
```

Note: `crates/torii/Cargo.toml` needs `serde = { version = "1", features = ["derive"] }` added —
`index` is generic over `serde::Serialize`, which requires `serde` as a direct dependency rather than
reaching it transitively through `serde_json`.

---

## Task 3: `render.rs` — tables and JSON

**Files:**
- Create: `crates/torii/src/render.rs`
- Modify: `crates/torii/src/main.rs` (add `mod render;`)

Design note: the table prints **full** run UUIDs, not truncated ones. An operator has to paste the
id into `torii run cancel <id>`; truncating it to fit a column would make the primary workflow
impossible.

- [ ] **Step 1: Write the failing tests**

Create `crates/torii/src/render.rs`:

```rust
//! Operator output: a human table by default, `--json` for scripting.

use chrono::{DateTime, Utc};
use orchestrator_core::ScheduledRun;

/// A NULL `next_wake` means "never auto-woken; needs `torii run wake`" (the s3
/// in-doubt class). It renders as an em dash in the table and `null` in JSON.
fn fmt_wake(w: Option<DateTime<Utc>>) -> String {
    match w {
        Some(t) => t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        None => "—".to_string(),
    }
}

pub fn table(rows: &[ScheduledRun]) -> String {
    let mut s = String::from("RUN                                   STATUS     NEXT WAKE             REASON\n");
    for r in rows {
        s.push_str(&format!(
            "{}  {:<9}  {:<20}  {}\n",
            r.run.0,
            r.status.as_str(),
            fmt_wake(r.next_wake),
            r.reason.as_deref().unwrap_or("")
        ));
    }
    s
}

pub fn json(rows: &[ScheduledRun]) -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::{RunId, RunStatus};

    fn row(next_wake: Option<DateTime<Utc>>, reason: Option<&str>) -> ScheduledRun {
        ScheduledRun {
            run: RunId(uuid::Uuid::from_u128(0x1234_5678_9abc_def0_1234_5678_9abc_def0)),
            status: RunStatus::Paused,
            next_wake,
            reason: reason.map(|s| s.to_string()),
            updated_at: DateTime::<Utc>::from_timestamp(3_000_000, 0).unwrap(),
        }
    }

    #[test]
    fn table_prints_the_full_run_id_so_it_can_be_pasted_into_cancel() {
        let r = row(None, None);
        let out = table(&[r.clone()]);
        assert!(
            out.contains(&r.run.0.to_string()),
            "the full uuid must appear verbatim: {out}"
        );
    }

    #[test]
    fn a_null_next_wake_renders_as_an_em_dash_in_the_table() {
        let out = table(&[row(None, Some("in-doubt mutation"))]);
        assert!(out.contains("—"), "NULL next_wake must be visibly distinct: {out}");
        assert!(out.contains("in-doubt mutation"), "{out}");
    }

    #[test]
    fn a_timed_next_wake_renders_as_rfc3339() {
        let t = DateTime::<Utc>::from_timestamp(3_000_000, 0).unwrap();
        let out = table(&[row(Some(t), None)]);
        assert!(out.contains("1970-02-04T17:20:00Z"), "{out}");
    }

    #[test]
    fn json_renders_a_null_next_wake_as_json_null() {
        let out = json(&[row(None, None)]).expect("serializes");
        assert!(out.contains("\"next_wake\": null"), "{out}");
    }

    #[test]
    fn json_round_trips_back_into_scheduled_runs() {
        let t = DateTime::<Utc>::from_timestamp(3_000_000, 0).unwrap();
        let out = json(&[row(Some(t), Some("quota"))]).expect("serializes");
        let back: Vec<ScheduledRun> = serde_json::from_str(&out).expect("round-trips");
        assert_eq!(back.len(), 1);
        assert_eq!(back[0].next_wake, Some(t));
        assert_eq!(back[0].reason.as_deref(), Some("quota"));
    }
}
```

Add `mod render;` to `main.rs`. `ScheduledRun` needs `Clone` — it already derives it
(`crates/orchestrator-core/src/scheduler.rs:52`).

- [ ] **Step 2: Run the tests**

Run: `cargo test -p sensei-torii render`
Expected: 5 passed. If `a_timed_next_wake_renders_as_rfc3339` fails on the exact timestamp string,
print the actual value and correct the expectation — the assertion's purpose is the RFC3339 *shape*,
not that specific epoch.

- [ ] **Step 3: Sanitize `reason` in the table, and fix `RunStatus`'s JSON casing**

Two defects a quality review caught in the code above — both must be applied:

**(a) `reason` needs control-character collapsing, in the TABLE path only.** `reason` is free text
from pause sites and provider messages, so it can contain `\n`/`\t`. The table is line-oriented, so a
raw newline splits one run's row into prefix-less fragments — and a UUID inside such a fragment reads
as a separate row, which is how an operator cancels the wrong run. Add:

```rust
fn one_line(s: &str) -> String {
    s.chars().map(|c| if c.is_control() { ' ' } else { c }).collect()
}
```

and apply it to `reason` inside `table()`. Do **not** apply it in `json()` — a script consuming
`--json` must get the exact stored value.

**(b) `RunStatus` serializes PascalCase while everything else is lowercase.** `as_str()`,
`from_db_str()`, and the `scheduled_runs.status` text column all use `"paused"`, but the derived
`Serialize` emits `"Paused"`, so a script filtering `--json` on the documented value matches nothing.
Add `#[serde(rename_all = "lowercase")]` to `RunStatus` in `crates/orchestrator-core/src/scheduler.rs`.
Safe and verified: `RunStatus` is never journaled, the PG store binds `status.as_str()` and reads
`from_db_str` (serde is not on the persistence path), and no test asserts a PascalCase status. This is
a latent SP-DATA-3 inconsistency; torii's `--json` is merely the first thing to expose it.

Three tests go with these: a multi-line `reason` must still render as exactly one data row; `--json`
must contain `"status": "paused"` and never `"Paused"`; and one test pinning the hand-counted column
alignment (the header spacing agrees with `{:<9}`/`{:<20}` only by hand-counting, so a longer future
`RunStatus` variant would silently desync it).

- [ ] **Step 4: Commit**

```bash
cargo fmt --all
git add crates/torii/src/render.rs crates/torii/src/main.rs crates/orchestrator-core/src/scheduler.rs
git commit -m "feat(torii): SP-DATA-4 (3/11) — table + JSON rendering of ScheduledRun"
```

---

## Task 4: `orchestrator-core` — defaulted `ConfigSource::load_versioned`

The TOCTOU fix must live on the **trait**, because `RegistryHandle::reload`
(`crates/orchestrator-core/src/registry.rs:489`) is what a worker calls to pick up config and it does
`load()` then `version()` as two separate awaits. An inherent method on `PostgresConfigSource` would
leave the worker torn while making the CLI safe.

**Files:**
- Modify: `crates/orchestrator-core/src/registry.rs:253-264` (trait), `:489-503` (`reload`), `:507-514` (`from_source`)

- [ ] **Step 1: Write the failing test**

Append to the existing `#[cfg(test)] mod tests` in `crates/orchestrator-core/src/registry.rs`:

```rust
    /// A source that counts how many times each read method is called, so we can
    /// prove `reload` uses the ATOMIC pair method rather than the two separate reads.
    struct CountingSource {
        cfg: RegistryConfig,
        version: u64,
        loads: std::sync::atomic::AtomicUsize,
        versions: std::sync::atomic::AtomicUsize,
        pairs: std::sync::atomic::AtomicUsize,
    }

    impl CountingSource {
        fn new(cfg: RegistryConfig, version: u64) -> Self {
            Self {
                cfg,
                version,
                loads: Default::default(),
                versions: Default::default(),
                pairs: Default::default(),
            }
        }
    }

    #[async_trait::async_trait]
    impl ConfigSource for CountingSource {
        async fn load(&self) -> Result<RegistryConfig, OrchestratorError> {
            self.loads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(self.cfg.clone())
        }
        async fn version(&self) -> Result<Option<u64>, OrchestratorError> {
            self.versions.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(Some(self.version))
        }
        async fn load_versioned(
            &self,
        ) -> Result<(RegistryConfig, Option<u64>), OrchestratorError> {
            self.pairs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok((self.cfg.clone(), Some(self.version)))
        }
    }

    #[tokio::test]
    async fn reload_reads_config_and_version_through_the_atomic_pair_method() {
        use std::sync::atomic::Ordering::SeqCst;
        let src = CountingSource::new(cfg_with_skill("s1"), 9);
        let h = RegistryHandle::new(Arc::new(Registry::default()));
        let gen = h.reload(&src).await.expect("reloads");
        assert_eq!(gen, 9, "the durable version is pinned as the generation");
        assert_eq!(src.pairs.load(SeqCst), 1, "reload must use load_versioned");
        assert_eq!(
            (src.loads.load(SeqCst), src.versions.load(SeqCst)),
            (0, 0),
            "reload must NOT issue the two separate reads (that is the TOCTOU)"
        );
    }

    #[tokio::test]
    async fn from_source_also_uses_the_atomic_pair_method() {
        use std::sync::atomic::Ordering::SeqCst;
        let src = CountingSource::new(cfg_with_skill("s1"), 4);
        let h = RegistryHandle::from_source(&src).await.expect("boots");
        assert_eq!(h.generation(), 4);
        assert_eq!(src.pairs.load(SeqCst), 1);
        assert_eq!((src.loads.load(SeqCst), src.versions.load(SeqCst)), (0, 0));
    }

    /// The DEFAULT impl must keep today's behavior for unversioned sources, whose
    /// `version()` is None — so there is no generation for the content to be
    /// inconsistent with, and the non-atomic default is harmless.
    #[tokio::test]
    async fn the_default_load_versioned_delegates_to_load_and_version() {
        let src = FixedSource(cfg_with_skill("s0"));
        let (cfg, ver) = src.load_versioned().await.expect("pair");
        assert_eq!(ver, None, "an unversioned source reports no generation");
        assert_eq!(cfg.skills.len(), 1);
    }
```

If `RegistryHandle` has no public `generation()` accessor, use the value returned by `reload` for
the first two tests and replace `h.generation()` with a `reload` of the same source (asserting the
pinned value) in `from_source_also_uses_the_atomic_pair_method`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sensei-orchestrator-core registry::tests::reload_reads_config_and_version`
Expected: FAIL to compile — `no method named load_versioned found for ... ConfigSource`.

- [ ] **Step 3: Add the defaulted trait method**

In `crates/orchestrator-core/src/registry.rs`, inside `pub trait ConfigSource`, after `version()`:

```rust
    /// Load the config AND its generation as ONE consistent pair.
    ///
    /// The default performs the two reads separately — correct for unversioned
    /// sources (`version()` is `None`, so there is no generation for the content
    /// to be inconsistent with). A **versioned** backend MUST override this with
    /// a single-snapshot read: otherwise a concurrent writer can hand back a torn
    /// (stale config, fresh generation) pair, a run stamps a fresh-generation
    /// fence over stale config, and a later resume matches the fence and silently
    /// continues under different config (SP-DATA-2 carry-forward).
    async fn load_versioned(
        &self,
    ) -> Result<(RegistryConfig, Option<u64>), OrchestratorError> {
        Ok((self.load().await?, self.version().await?))
    }
```

- [ ] **Step 4: Switch `reload` and `from_source` to the pair method**

`reload` becomes:

```rust
    pub async fn reload(&self, source: &dyn ConfigSource) -> Result<u64, OrchestratorError> {
        let (cfg, ver) = source.load_versioned().await?;
        let next = Registry::from_config(cfg)?;
        let mut w = self.inner.write().unwrap_or_else(|e| e.into_inner());
        w.0 = Arc::new(next);
        // A versioned source pins its durable generation; an unversioned one increments locally.
        w.1 = match ver {
            Some(v) => v,
            None => w.1 + 1,
        };
        Ok(w.1)
    }
```

`from_source` becomes:

```rust
    pub async fn from_source(source: &dyn ConfigSource) -> Result<Self, OrchestratorError> {
        let (cfg, ver) = source.load_versioned().await?;
        let registry = Registry::from_config(cfg)?;
        Ok(Self {
            inner: Arc::new(RwLock::new((Arc::new(registry), ver.unwrap_or(0)))),
        })
    }
```

Update both doc comments to say the read is one atomic pair via `load_versioned`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p sensei-orchestrator-core registry`
Expected: all pass, including the three new tests and the pre-existing
`from_source_boots_at_the_durable_version_and_unversioned_at_zero`.

- [ ] **Step 6: Verify nothing else regressed**

Run: `cargo test --workspace > /tmp/t4.log 2>&1; echo "exit=$?"; tail -20 /tmp/t4.log`
Expected: `exit=0`. Unversioned sources are unaffected because the default preserves their behavior.

- [ ] **Step 7: Commit**

```bash
cargo fmt --all
git add crates/orchestrator-core/src/registry.rs
git commit -m "feat(core): SP-DATA-4 (4/11) — defaulted ConfigSource::load_versioned; reload/from_source read one atomic pair"
```

---

## Task 5: `orchestrator-store` — atomic PG read + write, footgun gated

**Files:**
- Modify: `crates/orchestrator-store/src/postgres.rs:425-598` (`PostgresConfigSource`)
- Modify: `crates/orchestrator-store/Cargo.toml` (new `test-support` feature)
- Modify: `crates/orchestrator/Cargo.toml` (`postgres-tests` also enables `orchestrator-store/test-support`)

- [ ] **Step 1: Add the `test-support` feature**

`crates/orchestrator-store/Cargo.toml`:

```toml
[features]
postgres = ["dep:sqlx"]
# Exposes the UN-COUPLED config writers (`store`, `bump_config_version`). Production
# code must use `store_and_bump` — a store-without-bump changes content without
# advancing the generation, so a cross-process resume matches the unchanged fence and
# silently runs the new config. Tests need the un-coupled pair to PROVE that.
test-support = []
```

`crates/orchestrator/Cargo.toml`:

```toml
postgres-tests = ["orchestrator-store/postgres", "orchestrator-store/test-support"]
```

- [ ] **Step 2: Write the failing tests**

In `crates/orchestrator-store/src/postgres.rs`, inside the existing `#[cfg(test)] mod tests`:

```rust
    /// AC5 — the TOCTOU is CLOSED, proven adversarially rather than hopefully.
    /// A REPEATABLE READ snapshot is taken at the transaction's first read; a
    /// concurrent single-transaction writer therefore lands entirely before or
    /// entirely after it. Never (stale config, fresh generation).
    #[tokio::test]
    async fn load_versioned_is_immune_to_a_concurrent_store_and_bump() {
        let Some(url) = db_url() else { return };
        let src = PostgresConfigSource::new(connect(&url).await.unwrap());

        // Seed a known (content, generation) pair.
        src.store_and_bump(&cfg_with_skill("before")).await.unwrap();
        let (_, v0) = src.load_versioned().await.unwrap();
        let v0 = v0.expect("a versioned source always reports Some");

        // Open the snapshot and take its FIRST read inside the transaction.
        let mut tx = src.pool_for_test().begin().await.unwrap();
        sqlx::query("set transaction isolation level repeatable read")
            .execute(&mut *tx)
            .await
            .unwrap();
        let (first,): (i64,) =
            sqlx::query_as("select count(*) from orchestrator.config_skills")
                .fetch_one(&mut *tx)
                .await
                .unwrap();

        // A COMPLETE concurrent write from an independent connection.
        let writer = PostgresConfigSource::new(connect(&url).await.unwrap());
        writer.store_and_bump(&cfg_with_skill("after")).await.unwrap();

        // Finish reading inside the original snapshot: it must still see the OLD world.
        let (again,): (i64,) =
            sqlx::query_as("select count(*) from orchestrator.config_skills")
                .fetch_one(&mut *tx)
                .await
                .unwrap();
        let (snap_v,): (i64,) =
            sqlx::query_as("select version from orchestrator.config_versions where id = true")
                .fetch_one(&mut *tx)
                .await
                .unwrap();
        tx.commit().await.unwrap();

        assert_eq!(again, first, "the snapshot must not see the concurrent write");
        assert_eq!(
            snap_v as u64, v0,
            "the snapshot's generation must match its content — never a torn pair"
        );
    }

    /// AC6 — content and generation move together, atomically.
    #[tokio::test]
    async fn store_and_bump_advances_content_and_generation_together() {
        let Some(url) = db_url() else { return };
        let src = PostgresConfigSource::new(connect(&url).await.unwrap());
        let (_, before) = src.load_versioned().await.unwrap();
        let before = before.expect("Some");

        let v = src.store_and_bump(&cfg_with_skill("coupled")).await.unwrap();

        let (cfg, after) = src.load_versioned().await.unwrap();
        assert_eq!(after, Some(v), "the returned version is the durable one");
        assert_eq!(v, before + 1, "exactly one generation step");
        assert!(
            cfg.skills.iter().any(|s| s.name == "coupled"),
            "the content landed with the bump"
        );
    }

    /// AC6 — a rolled-back write moves NEITHER content nor generation.
    #[tokio::test]
    async fn a_failed_store_and_bump_leaves_content_and_generation_untouched() {
        let Some(url) = db_url() else { return };
        let src = PostgresConfigSource::new(connect(&url).await.unwrap());
        src.store_and_bump(&cfg_with_skill("stable")).await.unwrap();
        let (before_cfg, before_v) = src.load_versioned().await.unwrap();

        // A tool whose jsonb serialization is fine but whose NAME violates the
        // primary key twice in one transaction -> the txn aborts.
        let dup = ToolSpec {
            name: "dup".into(),
            description: "d".into(),
            input_schema: serde_json::json!({"type": "object"}),
            effect_class: EffectClass::Pure,
            ttl_secs: None,
            source: None,
            permissions: Default::default(),
            credentials: Default::default(),
        };
        let mut bad = cfg_with_skill("stable");
        bad.tools = vec![dup.clone(), dup];
        let err = src.store_and_bump(&bad).await;
        assert!(err.is_err(), "a duplicate primary key must abort the txn");

        let (after_cfg, after_v) = src.load_versioned().await.unwrap();
        assert_eq!(after_v, before_v, "generation must not advance on a failed write");
        assert_eq!(
            after_cfg.skills.len(),
            before_cfg.skills.len(),
            "content must not change on a failed write"
        );
    }
```

Add a test-only pool accessor next to `PostgresConfigSource`:

```rust
impl PostgresConfigSource {
    /// The pool, for tests that need to drive an explicit transaction (the TOCTOU proof).
    #[cfg(test)]
    pub(crate) fn pool_for_test(&self) -> &PgPool {
        &self.pool
    }
}
```

If `cfg_with_skill` / `ToolSpec`'s exact field set differ in this module, mirror whatever the
adjacent `version_is_zero_on_empty_then_monotonic_under_bump` test already uses — the point of the
third test is only that the transaction aborts, so any reliably-failing config works.

- [ ] **Step 3: Run to verify they fail**

Run: `cargo test -p sensei-orchestrator-store --features postgres,test-support load_versioned -- --test-threads=1`
Expected: FAIL to compile — `no method named store_and_bump` / `no method named load_versioned`.

- [ ] **Step 4: Extract shared read/write helpers and implement both methods**

In `crates/orchestrator-store/src/postgres.rs`, replace the body of `store` and the `load` impl with
calls to shared helpers, then add the two new methods:

```rust
/// Read the whole registry over ONE connection. Callers decide the snapshot
/// semantics: `load` uses a pooled connection (per-statement snapshots — the
/// documented non-atomic path, retained for the unversioned contract), while
/// `load_versioned` passes a REPEATABLE READ transaction for one snapshot.
async fn read_all(
    conn: &mut sqlx::PgConnection,
) -> Result<RegistryConfig, OrchestratorError> {
    let agents: Vec<(String, serde_json::Value)> =
        sqlx::query_as("select name, def from orchestrator.config_agents order by name")
            .fetch_all(&mut *conn)
            .await
            .map_err(cfg_load_err)?;
    let skills: Vec<(String, serde_json::Value)> =
        sqlx::query_as("select name, def from orchestrator.config_skills order by name")
            .fetch_all(&mut *conn)
            .await
            .map_err(cfg_load_err)?;
    let tools: Vec<(String, serde_json::Value)> =
        sqlx::query_as("select name, spec from orchestrator.config_tools order by name")
            .fetch_all(&mut *conn)
            .await
            .map_err(cfg_load_err)?;
    let bindings: Vec<(String, String, String)> = sqlx::query_as(
        "select area, kind, chain from orchestrator.config_chain_bindings order by area, kind",
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(cfg_load_err)?;
    Ok(RegistryConfig {
        agents: agents
            .into_iter()
            .map(|(n, v)| {
                serde_json::from_value(v)
                    .map_err(|e| OrchestratorError::RegistryLoad(format!("deser agent {n}: {e}")))
            })
            .collect::<Result<_, _>>()?,
        skills: skills
            .into_iter()
            .map(|(n, v)| {
                serde_json::from_value(v)
                    .map_err(|e| OrchestratorError::RegistryLoad(format!("deser skill {n}: {e}")))
            })
            .collect::<Result<_, _>>()?,
        tools: tools
            .into_iter()
            .map(|(n, v)| {
                serde_json::from_value(v)
                    .map_err(|e| OrchestratorError::RegistryLoad(format!("deser tool {n}: {e}")))
            })
            .collect::<Result<_, _>>()?,
        chain_bindings: bindings
            .into_iter()
            .map(|(area, kind, chain)| ChainBinding { area, kind, chain })
            .collect(),
    })
}

/// Replace-all write of every config table, on a caller-supplied connection so it
/// can be composed into a larger transaction (that composition is the whole point
/// of `store_and_bump`).
async fn write_all(
    conn: &mut sqlx::PgConnection,
    cfg: &RegistryConfig,
) -> Result<(), OrchestratorError> {
    for t in [
        "orchestrator.config_agents",
        "orchestrator.config_skills",
        "orchestrator.config_tools",
        "orchestrator.config_chain_bindings",
    ] {
        sqlx::query(&format!("delete from {t}"))
            .execute(&mut *conn)
            .await
            .map_err(store_err)?;
    }
    for a in &cfg.agents {
        let v = serde_json::to_value(a).map_err(store_err_ser)?;
        sqlx::query("insert into orchestrator.config_agents (name, def) values ($1, $2)")
            .bind(&a.name)
            .bind(v)
            .execute(&mut *conn)
            .await
            .map_err(store_err)?;
    }
    for s in &cfg.skills {
        let v = serde_json::to_value(s).map_err(store_err_ser)?;
        sqlx::query("insert into orchestrator.config_skills (name, def) values ($1, $2)")
            .bind(&s.name)
            .bind(v)
            .execute(&mut *conn)
            .await
            .map_err(store_err)?;
    }
    for t in &cfg.tools {
        let v = serde_json::to_value(t).map_err(store_err_ser)?;
        sqlx::query("insert into orchestrator.config_tools (name, spec) values ($1, $2)")
            .bind(&t.name)
            .bind(v)
            .execute(&mut *conn)
            .await
            .map_err(store_err)?;
    }
    for b in &cfg.chain_bindings {
        sqlx::query(
            "insert into orchestrator.config_chain_bindings (area, kind, chain) values ($1, $2, $3)",
        )
        .bind(&b.area)
        .bind(&b.kind)
        .bind(&b.chain)
        .execute(&mut *conn)
        .await
        .map_err(store_err)?;
    }
    Ok(())
}

/// The single-row atomic generation increment, on a caller-supplied connection.
async fn bump_on(conn: &mut sqlx::PgConnection) -> Result<u64, OrchestratorError> {
    let (v,): (i64,) = sqlx::query_as(
        "insert into orchestrator.config_versions (id, version) values (true, 1)
         on conflict (id) do update set version = orchestrator.config_versions.version + 1,
                                        updated_at = now()
         returning version",
    )
    .fetch_one(&mut *conn)
    .await
    .map_err(store_err)?;
    Ok(v as u64)
}
```

Then on `impl PostgresConfigSource`:

```rust
    /// Replace the whole registry AND advance the generation in ONE transaction —
    /// the only config write production code may use.
    ///
    /// One transaction, not two calls: a `store()` followed by a `bump()` has a
    /// crash window between them, and dying in it leaves new content under an old
    /// generation DURABLY — a cross-process resume then matches the unchanged fence
    /// and silently runs the new config. Atomicity removes the window instead of
    /// asking callers to be careful.
    pub async fn store_and_bump(&self, cfg: &RegistryConfig) -> Result<u64, OrchestratorError> {
        let mut tx = self.pool.begin().await.map_err(store_err)?;
        write_all(&mut tx, cfg).await?;
        let v = bump_on(&mut tx).await?;
        tx.commit().await.map_err(store_err)?;
        Ok(v)
    }

    /// The UN-COUPLED writers. Gated behind `test-support` because a `store` whose
    /// caller forgets `bump_config_version` changes content without advancing the
    /// generation — the silent-wrong-config footgun. Tests need them to prove the
    /// coupled path is what fixes it.
    #[cfg(feature = "test-support")]
    pub async fn store(&self, cfg: &RegistryConfig) -> Result<(), OrchestratorError> {
        let mut tx = self.pool.begin().await.map_err(store_err)?;
        write_all(&mut tx, cfg).await?;
        tx.commit().await.map_err(store_err)?;
        Ok(())
    }

    #[cfg(feature = "test-support")]
    pub async fn bump_config_version(&self) -> Result<u64, OrchestratorError> {
        let mut conn = self.pool.acquire().await.map_err(store_err)?;
        bump_on(&mut conn).await
    }
```

And in `impl ConfigSource for PostgresConfigSource`:

```rust
    async fn load(&self) -> Result<RegistryConfig, OrchestratorError> {
        let mut conn = self.pool.acquire().await.map_err(store_err)?;
        read_all(&mut conn).await
    }

    async fn version(&self) -> Result<Option<u64>, OrchestratorError> {
        // A versioned source ALWAYS returns Some (absent row ⇒ Some(0)); None is reserved for
        // genuinely-unversioned sources (filesystem/in-memory).
        let row: Option<(i64,)> =
            sqlx::query_as("select version from orchestrator.config_versions where id = true")
                .fetch_optional(&self.pool)
                .await
                .map_err(store_err)?;
        Ok(Some(row.map(|(v,)| v as u64).unwrap_or(0)))
    }

    /// ONE `REPEATABLE READ` snapshot over the four config tables AND
    /// `config_versions` — closing the SP-DATA-2 TOCTOU. The snapshot is taken at
    /// the transaction's first read, so a concurrent `store_and_bump` lands wholly
    /// before or wholly after it: the pair can never be torn.
    async fn load_versioned(
        &self,
    ) -> Result<(RegistryConfig, Option<u64>), OrchestratorError> {
        let mut tx = self.pool.begin().await.map_err(store_err)?;
        sqlx::query("set transaction isolation level repeatable read")
            .execute(&mut *tx)
            .await
            .map_err(store_err)?;
        let cfg = read_all(&mut tx).await?;
        let row: Option<(i64,)> =
            sqlx::query_as("select version from orchestrator.config_versions where id = true")
                .fetch_optional(&mut *tx)
                .await
                .map_err(store_err)?;
        tx.commit().await.map_err(store_err)?;
        Ok((cfg, Some(row.map(|(v,)| v as u64).unwrap_or(0))))
    }
```

Finally, **update the struct doc comment** at `:425-441`: delete the "KNOWN LIMITATION" paragraph and
replace it with a note that `load_versioned` is the atomic read and `store_and_bump` the atomic write,
and that `load`/`version` remain individually non-atomic by design for the unversioned contract.

- [ ] **Step 5: Run the DB tests**

Run (with `DATABASE_URL` exported per the harness section):

```bash
cargo test -p sensei-orchestrator-store --features postgres,test-support -- --test-threads=1
echo "exit=$?"
```

Expected: `exit=0`, all store tests pass including the three new ones. Confirm they did **not**
silently skip: `DATABASE_URL` must be set, or every test `return`s early.

- [ ] **Step 6: Verify the footgun is unreachable from a default build**

Run: `cargo build -p sensei-orchestrator-store 2>&1 | tail -5; echo "exit=$?"`
Expected: `exit=0` — with `test-support` off, `store`/`bump_config_version` are simply absent.

Run: `cargo test --workspace > /tmp/t5.log 2>&1; echo "exit=$?"`
Expected: `exit=0`. The orchestrator crate's PG tests that call `store`/`bump` compile only under
`postgres-tests`, which now enables `test-support`.

- [ ] **Step 7: Verify the orchestrator PG suite still builds with the feature**

Run: `cargo test -p sensei-orchestrator --features postgres-tests --no-run 2>&1 | tail -5; echo "exit=$?"`
Expected: `exit=0`.

- [ ] **Step 8: Commit**

```bash
cargo fmt --all
git add crates/orchestrator-store crates/orchestrator/Cargo.toml
git commit -m "feat(store): SP-DATA-4 (5/11) — atomic load_versioned + store_and_bump; un-coupled writers gated behind test-support"
```

---

## Task 6: `cmd/run.rs` — observe and intervene, reporting the EFFECT

`SchedulerStore::cancel` is "any non-terminal → cancelled, idempotent" and `force_wake` is
conditional on `paused` (`crates/orchestrator-core/src/scheduler.rs:112-116`). So cancelling a
terminal run and waking a non-paused run are **silent no-ops** that still return `Ok(())`. Reporting
"cancelled" off that `Ok` is reporting a proxy. Both commands re-read `status` and report the real
transition. These tests use `InMemorySchedulerStore` — **no database**.

**Files:**
- Create: `crates/torii/src/cmd/mod.rs`, `crates/torii/src/cmd/run.rs`
- Modify: `crates/torii/src/main.rs` (add `mod cmd;`)

- [ ] **Step 1: Create the shared `Outcome` type**

`crates/torii/src/cmd/mod.rs`:

```rust
//! Command implementations. Each returns an `Outcome` rather than printing, so
//! every command is unit-testable without capturing stdout.

pub mod config;
pub mod run;
pub mod worker;

/// What a command produced: the operator-facing text and the process exit code.
#[derive(Debug, PartialEq)]
pub struct Outcome {
    pub text: String,
    pub code: i32,
}

impl Outcome {
    pub fn ok(text: impl Into<String>) -> Self {
        Self { text: text.into(), code: crate::errors::EXIT_OK }
    }
    /// A command that ran fine but whose precondition was not met (nothing to
    /// cancel, nothing to wake, no such run).
    pub fn precondition(text: impl Into<String>) -> Self {
        Self { text: text.into(), code: crate::errors::EXIT_PRECONDITION }
    }
}
```

Create empty `crates/torii/src/cmd/config.rs` and `crates/torii/src/cmd/worker.rs` with a
`//! filled in by a later task` comment so `mod.rs` compiles.

- [ ] **Step 2: Write the failing tests**

`crates/torii/src/cmd/run.rs`:

```rust
//! Observe and intervene on runs. Every command reports the EFFECT it achieved,
//! never the fact that the store call returned Ok — `cancel` on a terminal run and
//! `wake` on a non-paused run are both silent no-ops at the store level.

use crate::cmd::Outcome;
use crate::errors::CliError;
use crate::render;
use chrono::{DateTime, Utc};
use orchestrator_core::{RunId, RunStatus, SchedulerStore};

pub async fn status(
    store: &dyn SchedulerStore,
    run: RunId,
    json: bool,
) -> Result<Outcome, CliError> {
    unimplemented!()
}

pub async fn list_paused(store: &dyn SchedulerStore, json: bool) -> Result<Outcome, CliError> {
    unimplemented!()
}

pub async fn cancel(store: &dyn SchedulerStore, run: RunId) -> Result<Outcome, CliError> {
    unimplemented!()
}

pub async fn wake(
    store: &dyn SchedulerStore,
    run: RunId,
    now: DateTime<Utc>,
) -> Result<Outcome, CliError> {
    unimplemented!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::{EXIT_OK, EXIT_PRECONDITION};
    use orchestrator_core::Graph;
    use orchestrator_store::InMemorySchedulerStore;

    fn now() -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(3_000_000, 0).unwrap()
    }

    fn empty_graph() -> Graph {
        Graph { nodes: vec![] }
    }

    /// A run enqueued then recorded paused with a deadline.
    async fn paused_store(run: RunId, next_wake: Option<DateTime<Utc>>) -> InMemorySchedulerStore {
        let s = InMemorySchedulerStore::default();
        s.enqueue(run, &empty_graph(), now()).await.unwrap();
        s.record_paused(run, next_wake, "quota: rate limited").await.unwrap();
        s
    }

    #[tokio::test]
    async fn status_of_an_unknown_run_is_a_precondition_failure_not_an_error() {
        let s = InMemorySchedulerStore::default();
        let out = status(&s, RunId(uuid::Uuid::new_v4()), false).await.expect("no hard error");
        assert_eq!(out.code, EXIT_PRECONDITION);
        assert!(out.text.contains("no such run"), "{}", out.text);
    }

    #[tokio::test]
    async fn list_paused_renders_the_pending_wake_set() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, Some(now())).await;
        let out = list_paused(&s, false).await.expect("lists");
        assert_eq!(out.code, EXIT_OK);
        assert!(out.text.contains(&run.0.to_string()), "{}", out.text);
        assert!(out.text.contains("quota: rate limited"), "{}", out.text);
    }

    #[tokio::test]
    async fn list_paused_json_is_machine_readable() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let out = list_paused(&s, true).await.expect("lists");
        let rows: Vec<orchestrator_core::ScheduledRun> =
            serde_json::from_str(&out.text).expect("valid json");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].next_wake, None);
    }

    #[tokio::test]
    async fn cancel_reports_the_transition_it_actually_made() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, Some(now())).await;
        let out = cancel(&s, run).await.expect("cancels");
        assert_eq!(out.code, EXIT_OK);
        assert!(out.text.starts_with("cancelled:"), "{}", out.text);
        assert_eq!(s.status(run).await.unwrap().unwrap().status, RunStatus::Cancelled);
    }

    /// THE honest-reporting case: the store call SUCCEEDS on a terminal run but
    /// changes nothing. Reporting "cancelled" here would be a lie.
    #[tokio::test]
    async fn cancel_on_a_terminal_run_reports_not_cancelled() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = InMemorySchedulerStore::default();
        s.enqueue(run, &empty_graph(), now()).await.unwrap();
        s.record_terminal(run, RunStatus::Completed, None).await.unwrap();

        let out = cancel(&s, run).await.expect("no hard error");
        assert_eq!(out.code, EXIT_PRECONDITION);
        assert!(out.text.contains("not cancelled"), "{}", out.text);
        assert!(out.text.contains("completed"), "must name the actual state: {}", out.text);
        assert_eq!(
            s.status(run).await.unwrap().unwrap().status,
            RunStatus::Completed,
            "and the run really is untouched"
        );
    }

    #[tokio::test]
    async fn wake_says_queued_never_resumed() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let out = wake(&s, run, now()).await.expect("wakes");
        assert_eq!(out.code, EXIT_OK);
        assert!(out.text.contains("queued"), "{}", out.text);
        assert!(
            !out.text.contains("resumed") && !out.text.contains("woken"),
            "force_wake only sets next_wake; a worker tick does the driving: {}",
            out.text
        );
        assert_eq!(
            s.status(run).await.unwrap().unwrap().next_wake,
            Some(now()),
            "the NULL deadline is now set to now, so the next tick claims it"
        );
    }

    #[tokio::test]
    async fn wake_on_a_non_paused_run_reports_not_queued() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = InMemorySchedulerStore::default();
        s.enqueue(run, &empty_graph(), now()).await.unwrap(); // status = waking, not paused
        let out = wake(&s, run, now()).await.expect("no hard error");
        assert_eq!(out.code, EXIT_PRECONDITION);
        assert!(out.text.contains("not queued"), "{}", out.text);
        assert!(out.text.contains("waking"), "must name the actual state: {}", out.text);
    }
}
```

- [ ] **Step 3: Run to verify they fail**

Run: `cargo test -p sensei-torii cmd::run`
Expected: FAIL — `not yet implemented` in all seven tests.

- [ ] **Step 4: Implement the commands**

```rust
pub async fn status(
    store: &dyn SchedulerStore,
    run: RunId,
    json: bool,
) -> Result<Outcome, CliError> {
    match store.status(run).await? {
        None => Ok(Outcome::precondition(format!("no such run: {}", run.0))),
        Some(r) => Ok(Outcome::ok(if json {
            render::json(&[r]).map_err(|e| CliError::error(e.to_string()))?
        } else {
            render::table(&[r])
        })),
    }
}

pub async fn list_paused(store: &dyn SchedulerStore, json: bool) -> Result<Outcome, CliError> {
    let rows = store.list_paused().await?;
    Ok(Outcome::ok(if json {
        render::json(&rows).map_err(|e| CliError::error(e.to_string()))?
    } else {
        render::table(&rows)
    }))
}

pub async fn cancel(store: &dyn SchedulerStore, run: RunId) -> Result<Outcome, CliError> {
    if store.status(run).await?.is_none() {
        return Ok(Outcome::precondition(format!("no such run: {}", run.0)));
    }
    store.cancel(run).await?;
    // Re-read: `cancel` is a conditional no-op on a terminal row, so only the
    // observed state proves what happened.
    let after = store
        .status(run)
        .await?
        .ok_or_else(|| CliError::error(format!("run {} vanished mid-cancel", run.0)))?;
    if after.status == RunStatus::Cancelled {
        Ok(Outcome::ok(format!("cancelled: {}", run.0)))
    } else {
        Ok(Outcome::precondition(format!(
            "not cancelled: {} is already {}",
            run.0,
            after.status.as_str()
        )))
    }
}

pub async fn wake(
    store: &dyn SchedulerStore,
    run: RunId,
    now: DateTime<Utc>,
) -> Result<Outcome, CliError> {
    let Some(before) = store.status(run).await? else {
        return Ok(Outcome::precondition(format!("no such run: {}", run.0)));
    };
    if before.status != RunStatus::Paused {
        return Ok(Outcome::precondition(format!(
            "not queued: {} is {}, and only a paused run can be woken",
            run.0,
            before.status.as_str()
        )));
    }
    store.force_wake(run, now).await?;
    let after = store
        .status(run)
        .await?
        .ok_or_else(|| CliError::error(format!("run {} vanished mid-wake", run.0)))?;
    match after.next_wake {
        Some(_) => Ok(Outcome::ok(format!(
            "queued for wake: {} (a worker tick will drive it)",
            run.0
        ))),
        None => Ok(Outcome::precondition(format!(
            "not queued: {} still has no wake deadline",
            run.0
        ))),
    }
}
```

Add `mod cmd;` to `main.rs`.

- [ ] **Step 5: Run to verify they pass**

Run: `cargo test -p sensei-torii cmd::run`
Expected: 7 passed.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/torii/src/cmd crates/torii/src/main.rs
git commit -m "feat(torii): SP-DATA-4 (6/11) — run observe/intervene reporting the achieved effect, not the Ok"
```

---

## Task 7: `cmd/config.rs` — `plan_push` (pure) + `version`/`push`

The push pipeline is **validate → diff → confirm → write**, and validation is load-bearing: pushing
config that fails `Registry::from_config` means every later `load()` fails `RegistryLoad`, every
worker stops resuming, and the config that would fix it is the config just destroyed.

**Files:**
- Modify: `crates/torii/src/cmd/config.rs`

- [ ] **Step 1: Write the failing tests**

```rust
//! The durable config write path. Validate before writing, diff before
//! overwriting, and never write content without advancing the generation.

use crate::cmd::Outcome;
use crate::diff::{ConfigDiff, diff};
use crate::errors::CliError;
use orchestrator_core::{ConfigSource, Registry, RegistryConfig};
use orchestrator_store::postgres::PostgresConfigSource;
use orchestrator_store::FilesystemConfigSource;
use std::path::Path;

/// What `plan_push` decided, before any write happens.
#[derive(Debug)]
pub enum PushDecision {
    /// Nothing to do — the incoming config matches the durable one.
    NoOp(ConfigDiff),
    /// Safe to write.
    Apply(ConfigDiff),
    /// Refused: removals need confirmation that was not given.
    NeedsConfirmation(ConfigDiff),
}

/// The pure decision: is this push safe to apply? `confirmed` is true when the
/// operator passed `--yes` or answered the prompt.
pub fn plan_push(
    current: &RegistryConfig,
    incoming: &RegistryConfig,
    confirmed: bool,
) -> PushDecision {
    unimplemented!()
}

/// Render a diff for the operator.
pub fn describe_diff(d: &ConfigDiff, current_version: u64, source: &str) -> String {
    unimplemented!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::{Activation, SkillDef};

    fn skill(name: &str, body: &str) -> SkillDef {
        SkillDef {
            name: name.into(),
            description: None,
            body: body.into(),
            activation: Activation::default(),
        }
    }

    fn cfg(skills: Vec<SkillDef>) -> RegistryConfig {
        RegistryConfig { agents: vec![], skills, tools: vec![], chain_bindings: vec![] }
    }

    #[test]
    fn a_pure_addition_applies_without_confirmation() {
        let d = plan_push(&cfg(vec![]), &cfg(vec![skill("s", "b")]), false);
        assert!(matches!(d, PushDecision::Apply(_)), "{d:?}");
    }

    #[test]
    fn an_identical_config_is_a_noop() {
        let c = cfg(vec![skill("s", "b")]);
        let d = plan_push(&c, &c, false);
        assert!(matches!(d, PushDecision::NoOp(_)), "{d:?}");
    }

    /// AC4: a removal without confirmation must refuse, so nothing is written.
    #[test]
    fn a_removal_without_confirmation_is_refused() {
        let d = plan_push(&cfg(vec![skill("s", "b")]), &cfg(vec![]), false);
        match d {
            PushDecision::NeedsConfirmation(diff) => {
                assert_eq!(diff.removed.len(), 1);
            }
            other => panic!("a removal must refuse without --yes: {other:?}"),
        }
    }

    #[test]
    fn a_removal_with_confirmation_applies() {
        let d = plan_push(&cfg(vec![skill("s", "b")]), &cfg(vec![]), true);
        assert!(matches!(d, PushDecision::Apply(_)), "{d:?}");
    }

    #[test]
    fn describe_diff_names_the_removals_and_the_current_version() {
        let d = diff(&cfg(vec![skill("gone", "b")]), &cfg(vec![skill("new", "b")]));
        let text = describe_diff(&d, 7, "./config");
        assert!(text.contains("v7"), "{text}");
        assert!(text.contains("./config"), "{text}");
        assert!(text.contains("gone"), "removals must be named: {text}");
        assert!(text.contains("new"), "additions must be named: {text}");
        assert!(text.contains("REMOVES"), "the destructive fact must be loud: {text}");
    }
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p sensei-torii cmd::config`
Expected: FAIL — `not yet implemented` in all five tests.

- [ ] **Step 3: Implement the pure functions**

```rust
pub fn plan_push(
    current: &RegistryConfig,
    incoming: &RegistryConfig,
    confirmed: bool,
) -> PushDecision {
    let d = diff(current, incoming);
    if d.is_noop() {
        return PushDecision::NoOp(d);
    }
    if d.requires_confirmation() && !confirmed {
        return PushDecision::NeedsConfirmation(d);
    }
    PushDecision::Apply(d)
}

pub fn describe_diff(d: &ConfigDiff, current_version: u64, source: &str) -> String {
    let mut s = format!("config diff (durable v{current_version} -> {source}):\n");
    for e in &d.added {
        s.push_str(&format!("  + {:<6} {}\n", e.kind.label(), e.name));
    }
    for e in &d.changed {
        s.push_str(&format!("  ~ {:<6} {}\n", e.kind.label(), e.name));
    }
    for e in &d.removed {
        s.push_str(&format!("  - {:<6} {}\n", e.kind.label(), e.name));
    }
    s.push_str(&format!("  = {} unchanged\n", d.unchanged));
    if d.requires_confirmation() {
        s.push_str(&format!(
            "\nThis REMOVES {} entities. A push is replace-all: removed entities cannot be recovered.\n",
            d.removed.len()
        ));
    }
    s
}
```

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p sensei-torii cmd::config`
Expected: 5 passed.

- [ ] **Step 5: Add the Postgres-backed commands**

Append to `crates/torii/src/cmd/config.rs`:

```rust
pub async fn version(src: &PostgresConfigSource, json: bool) -> Result<Outcome, CliError> {
    let v = src.version().await?.unwrap_or(0);
    Ok(Outcome::ok(if json {
        serde_json::json!({ "version": v }).to_string()
    } else {
        format!("config version: {v}")
    }))
}

/// `confirm` is called ONLY when the diff removes something and `--yes` was absent.
/// It returns false on a non-interactive stdin, so a scripted push that would
/// delete config refuses instead of proceeding.
pub async fn push(
    src: &PostgresConfigSource,
    dir: &Path,
    yes: bool,
    confirm: &mut dyn FnMut(&str) -> bool,
) -> Result<Outcome, CliError> {
    // 1. Load AND VALIDATE the incoming config before touching a single row.
    let incoming = FilesystemConfigSource::new(dir).load().await?;
    Registry::from_config(incoming.clone()).map_err(|e| {
        CliError::error(format!(
            "refusing to push: {} does not assemble into a valid registry: {e}",
            dir.display()
        ))
    })?;

    // 2. One atomic read of the durable (content, generation) pair.
    let (current, current_v) = src.load_versioned().await?;
    let current_v = current_v.unwrap_or(0);

    // 3. Decide.
    let source = dir.display().to_string();
    match plan_push(&current, &incoming, yes) {
        PushDecision::NoOp(_) => Ok(Outcome::ok(format!(
            "no changes: {source} already matches durable config v{current_v}"
        ))),
        PushDecision::NeedsConfirmation(d) => {
            let text = describe_diff(&d, current_v, &source);
            if !confirm(&text) {
                return Ok(Outcome::precondition(format!(
                    "{text}\nrefused: nothing written, config still at v{current_v}"
                )));
            }
            let v = src.store_and_bump(&incoming).await?;
            Ok(Outcome::ok(format!("{text}\npushed: config now at v{v}")))
        }
        PushDecision::Apply(d) => {
            let text = describe_diff(&d, current_v, &source);
            let v = src.store_and_bump(&incoming).await?;
            Ok(Outcome::ok(format!("{text}\npushed: config now at v{v}")))
        }
    }
}
```

- [ ] **Step 6: Verify the crate builds and the pure tests still pass**

Run: `cargo test -p sensei-torii 2>&1 | tail -10; echo "exit=$?"`
Expected: `exit=0`.

- [ ] **Step 7: Commit**

```bash
cargo fmt --all
git add crates/torii/src/cmd/config.rs
git commit -m "feat(torii): SP-DATA-4 (7/11) — config version + push (validate -> diff -> confirm -> store_and_bump)"
```

---

## Task 8: `cmd/worker.rs` — the first production `tick()` caller

`Scheduler::tick` already separates the two failure classes: a *drive's* failure is recorded terminal
in the store, only a *store* failure returns `Err`. The loop honors that. Injecting a `Ticker` trait
makes the whole resilience policy testable with **no database**.

**Files:**
- Modify: `crates/torii/src/cmd/worker.rs`

- [ ] **Step 1: Write the failing tests**

```rust
//! The worker: the first production caller of `Scheduler::tick`.

use crate::errors::CliError;
use orchestrator_core::OrchestratorError;
use std::time::Duration;

/// The tick surface, injected so the loop's resilience policy is testable without
/// a database or a gateway.
#[async_trait::async_trait]
pub trait Ticker: Send + Sync {
    async fn tick(&self) -> Result<usize, OrchestratorError>;
}

#[async_trait::async_trait]
impl Ticker for orchestrator::Scheduler {
    async fn tick(&self) -> Result<usize, OrchestratorError> {
        orchestrator::Scheduler::tick(self).await
    }
}

pub const MAX_CONSECUTIVE_FAILURES: u32 = 5;

pub struct ServeOpts {
    pub interval: Duration,
    pub once: bool,
}

/// Parse `5s` / `2m` / `500ms` into a Duration.
pub fn parse_interval(s: &str) -> Result<Duration, String> {
    unimplemented!()
}

/// Poll until shutdown. A store fault is retried with bounded backoff; after
/// MAX_CONSECUTIVE_FAILURES the worker exits non-zero so a supervisor restarts it
/// and an alert fires — a dead database must never read as healthy.
pub async fn serve(
    ticker: &dyn Ticker,
    opts: ServeOpts,
    shutdown: impl std::future::Future<Output = ()> + Send,
) -> Result<Outcome, CliError> {
    unimplemented!()
}

use crate::cmd::Outcome;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FakeTicker {
        calls: AtomicUsize,
        /// Return Err for the first `fail_first` calls, then Ok(0).
        fail_first: usize,
        always_fail: bool,
    }

    impl FakeTicker {
        fn ok() -> Self {
            Self { calls: AtomicUsize::new(0), fail_first: 0, always_fail: false }
        }
        fn failing_forever() -> Self {
            Self { calls: AtomicUsize::new(0), fail_first: 0, always_fail: true }
        }
        fn failing_then_ok(n: usize) -> Self {
            Self { calls: AtomicUsize::new(0), fail_first: n, always_fail: false }
        }
    }

    #[async_trait::async_trait]
    impl Ticker for FakeTicker {
        async fn tick(&self) -> Result<usize, OrchestratorError> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            if self.always_fail || n < self.fail_first {
                return Err(OrchestratorError::Store("pool timeout".into()));
            }
            Ok(1)
        }
    }

    #[test]
    fn parse_interval_accepts_seconds_minutes_and_millis() {
        assert_eq!(parse_interval("5s"), Ok(Duration::from_secs(5)));
        assert_eq!(parse_interval("2m"), Ok(Duration::from_secs(120)));
        assert_eq!(parse_interval("500ms"), Ok(Duration::from_millis(500)));
    }

    #[test]
    fn parse_interval_rejects_garbage_loudly() {
        assert!(parse_interval("soon").is_err());
        assert!(parse_interval("5").is_err(), "a bare number has no unit");
        assert!(parse_interval("").is_err());
    }

    #[tokio::test]
    async fn once_runs_exactly_one_tick() {
        let t = FakeTicker::ok();
        let out = serve(
            &t,
            ServeOpts { interval: Duration::from_secs(5), once: true },
            std::future::pending(),
        )
        .await
        .expect("serves");
        assert_eq!(t.calls.load(Ordering::SeqCst), 1);
        assert_eq!(out.code, crate::errors::EXIT_OK);
    }

    /// A transient store fault must NOT kill the worker — Postgres failover is
    /// survivable.
    #[tokio::test(start_paused = true)]
    async fn a_transient_store_fault_is_retried_not_fatal() {
        let t = FakeTicker::failing_then_ok(2);
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            // Stop once we have seen a success after the failures.
            tokio::time::sleep(Duration::from_secs(60)).await;
            let _ = tx.send(());
        });
        let out = serve(
            &t,
            ServeOpts { interval: Duration::from_millis(10), once: false },
            async { rx.await.ok(); },
        )
        .await
        .expect("survives transient faults");
        handle.abort();
        assert_eq!(out.code, crate::errors::EXIT_OK);
        assert!(
            t.calls.load(Ordering::SeqCst) > 2,
            "must have retried past the failures: {}",
            t.calls.load(Ordering::SeqCst)
        );
    }

    /// A persistently dead database must NOT be silently tolerated.
    #[tokio::test(start_paused = true)]
    async fn a_persistent_store_fault_exits_non_zero_after_the_cap() {
        let t = FakeTicker::failing_forever();
        let err = serve(
            &t,
            ServeOpts { interval: Duration::from_millis(10), once: false },
            std::future::pending(),
        )
        .await
        .expect_err("a dead store must be fatal eventually");
        assert_eq!(err.code, crate::errors::EXIT_ERROR);
        assert!(err.message.contains("consecutive"), "{}", err.message);
        assert_eq!(
            t.calls.load(Ordering::SeqCst),
            MAX_CONSECUTIVE_FAILURES as usize,
            "exactly the cap, then give up"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_stops_the_loop_cleanly() {
        let t = FakeTicker::ok();
        let out = serve(
            &t,
            ServeOpts { interval: Duration::from_secs(1), once: false },
            tokio::time::sleep(Duration::from_millis(1500)),
        )
        .await
        .expect("clean shutdown");
        assert_eq!(out.code, crate::errors::EXIT_OK);
        assert!(t.calls.load(Ordering::SeqCst) >= 1);
    }
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p sensei-torii cmd::worker`
Expected: FAIL — `not yet implemented`.

- [ ] **Step 3: Implement `parse_interval` and `serve`**

```rust
pub fn parse_interval(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    let (num, unit) = if let Some(n) = s.strip_suffix("ms") {
        (n, "ms")
    } else if let Some(n) = s.strip_suffix('s') {
        (n, "s")
    } else if let Some(n) = s.strip_suffix('m') {
        (n, "m")
    } else {
        return Err(format!("invalid interval {s:?}: expected a unit, e.g. 500ms, 5s, 2m"));
    };
    let v: u64 = num
        .parse()
        .map_err(|_| format!("invalid interval {s:?}: {num:?} is not a number"))?;
    Ok(match unit {
        "ms" => Duration::from_millis(v),
        "s" => Duration::from_secs(v),
        _ => Duration::from_secs(v * 60),
    })
}

pub async fn serve(
    ticker: &dyn Ticker,
    opts: ServeOpts,
    shutdown: impl std::future::Future<Output = ()> + Send,
) -> Result<Outcome, CliError> {
    let mut shutdown = Box::pin(shutdown);
    let mut consecutive_failures: u32 = 0;
    let mut woken_total: usize = 0;

    loop {
        match ticker.tick().await {
            Ok(n) => {
                consecutive_failures = 0;
                woken_total += n;
                if n > 0 {
                    tracing::info!(woken = n, "tick woke runs");
                }
            }
            Err(e) => {
                consecutive_failures += 1;
                // Loud, with the full chain — never swallowed.
                tracing::error!(
                    error = %e,
                    consecutive = consecutive_failures,
                    "store fault during tick"
                );
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                    return Err(CliError::error(format!(
                        "giving up after {consecutive_failures} consecutive store faults: {e}"
                    )));
                }
            }
        }

        if opts.once {
            return Ok(Outcome::ok(format!("tick complete: {woken_total} run(s) woken")));
        }

        // Back off on failure so a dead database is not hammered; otherwise poll.
        let delay = if consecutive_failures == 0 {
            opts.interval
        } else {
            opts.interval * 2u32.saturating_pow(consecutive_failures.min(4))
        };

        tokio::select! {
            _ = &mut shutdown => {
                return Ok(Outcome::ok(format!(
                    "shutdown: {woken_total} run(s) woken this session"
                )));
            }
            _ = tokio::time::sleep(delay) => {}
        }
    }
}
```

Note the ordering: the failure cap is checked **before** the `once` early-return and before the
sleep, so `a_persistent_store_fault_exits_non_zero_after_the_cap` sees exactly
`MAX_CONSECUTIVE_FAILURES` calls.

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p sensei-torii cmd::worker`
Expected: 7 passed. If `a_transient_store_fault_is_retried_not_fatal` is flaky under
`start_paused`, replace the spawned timer with a shutdown future that resolves after a fixed
`tokio::time::sleep(Duration::from_secs(60))` — with a paused clock this is deterministic and
instant.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/torii/src/cmd/worker.rs
git commit -m "feat(torii): SP-DATA-4 (8/11) — worker serve loop: backoff, fatal-after-5, --once, clean shutdown"
```

---

## Task 9: `boot.rs` — env validation and two-tier construction

**Files:**
- Create: `crates/torii/src/boot.rs`
- Modify: `crates/torii/src/main.rs` (add `mod boot;`)

- [ ] **Step 1: Write the failing tests**

```rust
//! Wiring: environment and files -> live dependencies. This lives in the BINARY,
//! not the library: `Executor` takes every backend as an injected `Arc<dyn ...>`
//! precisely so the library knows nothing about Postgres, env vars, or config files.

use crate::errors::{CliError, redact_url};
use orchestrator::{Executor, Scheduler};
use orchestrator_core::{Clock, PatternRedactor, RegistryHandle, SystemClock};
use orchestrator_store::postgres::{
    PostgresConfigSource, PostgresContentStore, PostgresContextStore, PostgresJournal,
    PostgresSchedulerStore, connect,
};
use std::path::Path;
use std::sync::Arc;

pub const ENV_DATABASE_URL: &str = "DATABASE_URL";
pub const ENV_FENCE_VERSION: &str = "TORII_FENCE_VERSION";

/// The validated environment. `fence_version` is only required by the heavy tier.
#[derive(Debug, PartialEq)]
pub struct EnvConfig {
    pub database_url: String,
    pub fence_version: Option<String>,
}

/// Validate the environment through an injected getter, so tests never mutate
/// process env (which is `unsafe` in edition 2024 and racy across parallel tests).
pub fn env_config_from(get: impl Fn(&str) -> Option<String>) -> Result<EnvConfig, CliError> {
    unimplemented!()
}

pub fn env_config() -> Result<EnvConfig, CliError> {
    env_config_from(|k| std::env::var(k).ok())
}

/// The heavy tier additionally requires the fence base.
pub fn require_fence(env: &EnvConfig) -> Result<&str, CliError> {
    unimplemented!()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn getter(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> + '_ {
        move |k| {
            pairs
                .iter()
                .find(|(key, _)| *key == k)
                .map(|(_, v)| v.to_string())
        }
    }

    #[test]
    fn a_missing_database_url_is_a_specific_actionable_error() {
        let err = env_config_from(getter(&[])).expect_err("must fail");
        assert_eq!(err.code, crate::errors::EXIT_ERROR);
        assert!(err.message.contains(ENV_DATABASE_URL), "{}", err.message);
    }

    #[test]
    fn the_light_tier_needs_only_a_database_url() {
        let e = env_config_from(getter(&[(ENV_DATABASE_URL, "postgres://h/db")])).expect("ok");
        assert_eq!(e.database_url, "postgres://h/db");
        assert_eq!(e.fence_version, None);
    }

    /// The heavy tier must refuse to start without an explicit fence base: deriving
    /// it would strand every paused run on a routine version bump.
    #[test]
    fn the_heavy_tier_refuses_without_an_explicit_fence_version() {
        let e = env_config_from(getter(&[(ENV_DATABASE_URL, "postgres://h/db")])).expect("ok");
        let err = require_fence(&e).expect_err("must refuse");
        assert_eq!(err.code, crate::errors::EXIT_ERROR);
        assert!(err.message.contains(ENV_FENCE_VERSION), "{}", err.message);
        assert!(
            err.message.contains("recorded in every run"),
            "must explain WHY it is required: {}",
            err.message
        );
    }

    #[test]
    fn an_explicit_fence_version_is_accepted() {
        let e = env_config_from(getter(&[
            (ENV_DATABASE_URL, "postgres://h/db"),
            (ENV_FENCE_VERSION, "v1"),
        ]))
        .expect("ok");
        assert_eq!(require_fence(&e).expect("present"), "v1");
    }

    /// An empty fence version is as dangerous as a missing one.
    #[test]
    fn a_blank_fence_version_is_rejected() {
        let e = env_config_from(getter(&[
            (ENV_DATABASE_URL, "postgres://h/db"),
            (ENV_FENCE_VERSION, "   "),
        ]))
        .expect("ok");
        assert!(require_fence(&e).is_err(), "whitespace is not a fence base");
    }

    /// Errors must never echo the connection string.
    #[test]
    fn a_blank_database_url_error_does_not_echo_a_secret() {
        let pw = format!("s3cr{}t", "e");
        let url = format!("postgres://u:{pw}@h:5432/db");
        let e = env_config_from(getter(&[(ENV_DATABASE_URL, &url)])).expect("ok");
        // The redaction helper is what every message uses.
        assert!(!redact_url(&e.database_url).contains(&pw));
    }
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p sensei-torii boot`
Expected: FAIL — `not yet implemented`.

- [ ] **Step 3: Implement the validation**

```rust
pub fn env_config_from(get: impl Fn(&str) -> Option<String>) -> Result<EnvConfig, CliError> {
    let database_url = get(ENV_DATABASE_URL)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| {
            CliError::error(format!(
                "{ENV_DATABASE_URL} is not set.\n\
                 torii reads the Postgres connection string from the environment only — a flag \
                 would put the password in `ps` output and shell history."
            ))
        })?;
    let fence_version = get(ENV_FENCE_VERSION).filter(|s| !s.trim().is_empty());
    Ok(EnvConfig { database_url, fence_version })
}

pub fn require_fence(env: &EnvConfig) -> Result<&str, CliError> {
    env.fence_version.as_deref().ok_or_else(|| {
        CliError::error(format!(
            "{ENV_FENCE_VERSION} is not set.\n\
             The fence base is recorded in every run and checked on resume, so a fleet must \
             agree on it. Set it explicitly (e.g. {ENV_FENCE_VERSION}=v1) — deriving it from \
             the build version would strand every paused run on a routine deploy."
        ))
    })
}
```

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p sensei-torii boot`
Expected: 6 passed.

- [ ] **Step 5: Add the two dependency tiers**

Append to `boot.rs`:

```rust
/// Light tier: everything reachable with just a database. No gateway, no model
/// credentials, no fence — so an operator can cancel a runaway run or inspect the
/// wake queue on a box that has none of those.
pub struct LightDeps {
    pub scheduler_store: Arc<PostgresSchedulerStore>,
    pub config_source: PostgresConfigSource,
}

pub async fn light(env: &EnvConfig) -> Result<LightDeps, CliError> {
    let pool = connect(&env.database_url).await.map_err(|e| {
        CliError::error(format!(
            "cannot connect to {}: {e}",
            redact_url(&env.database_url)
        ))
    })?;
    Ok(LightDeps {
        scheduler_store: Arc::new(PostgresSchedulerStore::new(pool.clone())),
        config_source: PostgresConfigSource::new(pool),
    })
}

/// Heavy tier: a full Executor behind a Scheduler. Adds the gateway config file
/// and the fence base.
pub struct HeavyDeps {
    pub light: LightDeps,
    pub scheduler: Scheduler,
    pub clock: Arc<dyn Clock>,
}

pub async fn heavy(
    env: &EnvConfig,
    gateway_config: &Path,
    workspace_root: Option<&Path>,
) -> Result<HeavyDeps, CliError> {
    let fence = require_fence(env)?.to_string();
    let light = light(env).await?;

    // The gateway config file holds provider API keys: report its PATH on failure,
    // never its contents.
    let raw = std::fs::read_to_string(gateway_config).map_err(|e| {
        CliError::error(format!("cannot read {}: {e}", gateway_config.display()))
    })?;
    let gw_config: kernel::types::config::GatewayConfig =
        serde_json::from_str(&raw).map_err(|e| {
            CliError::error(format!(
                "{} is not a valid gateway config: {e}",
                gateway_config.display()
            ))
        })?;
    let gateway = Arc::new(gateway::Gateway::new(gw_config));

    let url = &env.database_url;
    let journal = Arc::new(PostgresJournal::new(
        connect(url).await.map_err(|e| CliError::error(e.to_string()))?,
    ));
    let content = Arc::new(PostgresContentStore::new(
        connect(url).await.map_err(|e| CliError::error(e.to_string()))?,
    ));
    let context = Arc::new(PostgresContextStore::new(
        connect(url).await.map_err(|e| CliError::error(e.to_string()))?,
    ));
    // One atomic (config, generation) read — the fence generation must match the
    // config it was computed from.
    let handle = RegistryHandle::from_source(&light.config_source).await?;

    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let mut executor = Executor::new(gateway, journal.clone(), fence)
        .with_content_store(content)
        .with_context_store(context)
        .with_registry_handle(handle)
        // A production binary defaults SECURE: s2 leaves the redactor off in the
        // library to stay byte-identical, but here it is unconditional and there is
        // deliberately no --no-redact flag.
        .with_redactor(Arc::new(PatternRedactor::default()))
        .with_clock(clock.clone());

    if let Some(root) = workspace_root {
        executor = executor.with_workspace_root(root);
        #[cfg(target_os = "macos")]
        {
            executor = executor
                .with_sandbox(Arc::new(orchestrator::agent::sandbox::MacosSandbox));
        }
        #[cfg(target_os = "linux")]
        {
            executor = executor
                .with_sandbox(Arc::new(orchestrator::agent::sandbox::LinuxSandbox));
        }
    }

    let scheduler = Scheduler::new(
        light.scheduler_store.clone(),
        executor,
        journal,
        clock.clone(),
    );
    Ok(HeavyDeps { light, scheduler, clock })
}
```

- [ ] **Step 6: Verify it compiles and all torii tests pass**

Run: `cargo test -p sensei-torii 2>&1 | tail -12; echo "exit=$?"`
Expected: `exit=0`. If `Executor::with_registry_handle` / `with_context_store` signatures differ,
match the definitions at `crates/orchestrator/src/executor/mod.rs:286` and `:353`.

- [ ] **Step 7: Commit**

```bash
cargo fmt --all
git add crates/torii/src/boot.rs crates/torii/src/main.rs
git commit -m "feat(torii): SP-DATA-4 (9/11) — two-tier boot: light needs only DATABASE_URL, heavy requires an explicit fence"
```

---

## Task 10: `main.rs` — clap dispatch + binary smoke tests

**Files:**
- Modify: `crates/torii/src/main.rs`
- Create: `crates/torii/tests/cli.rs`

- [ ] **Step 1: Write the clap surface**

Replace `crates/torii/src/main.rs`:

```rust
//! `torii` — the operator control plane for the sensei orchestrator.

mod boot;
mod cmd;
mod diff;
mod errors;
mod render;

use clap::{Parser, Subcommand};
use cmd::Outcome;
use errors::CliError;
use orchestrator_core::{Graph, RunId};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "torii",
    about = "Operator control plane for the sensei orchestrator",
    long_about = "Observe and intervene on runs, drive due wakes, and manage durable config.\n\n\
                  DATABASE_URL must be set (env only — a flag would leak the password into `ps`).\n\
                  `run submit` and `worker serve` additionally need TORII_FENCE_VERSION and \
                  --gateway-config."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Observe and intervene on runs
    Run {
        #[command(subcommand)]
        action: RunAction,
    },
    /// Drive due wakes
    Worker {
        #[command(subcommand)]
        action: WorkerAction,
    },
    /// Manage the durable registry config
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
}

#[derive(Subcommand)]
enum RunAction {
    /// Submit a graph and drive it (blocks until it pauses or finishes)
    Submit {
        #[arg(long)]
        graph: PathBuf,
        #[arg(long)]
        run_id: Option<String>,
        #[arg(long)]
        gateway_config: PathBuf,
        #[arg(long)]
        workspace_root: Option<PathBuf>,
    },
    /// Show one run's schedule record
    Status {
        run_id: String,
        #[arg(long)]
        json: bool,
    },
    /// List every run awaiting a wake
    ListPaused {
        #[arg(long)]
        json: bool,
    },
    /// Cancel a non-terminal run so it is never woken
    Cancel { run_id: String },
    /// Queue a paused run for the next worker tick
    Wake { run_id: String },
}

#[derive(Subcommand)]
enum WorkerAction {
    /// Poll for due wakes and drive them
    Serve {
        #[arg(long, default_value = "5s", value_parser = cmd::worker::parse_interval)]
        interval: std::time::Duration,
        /// Run exactly one tick and exit (cron-friendly)
        #[arg(long)]
        once: bool,
        #[arg(long)]
        gateway_config: PathBuf,
        #[arg(long)]
        workspace_root: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum ConfigAction {
    /// Show the durable config generation
    Version {
        #[arg(long)]
        json: bool,
    },
    /// Replace the durable config from a directory and advance the generation
    Push {
        dir: PathBuf,
        /// Apply without confirmation even when entities are removed
        #[arg(long)]
        yes: bool,
    },
}

fn parse_run_id(s: &str) -> Result<RunId, CliError> {
    uuid::Uuid::parse_str(s)
        .map(RunId)
        .map_err(|e| CliError::error(format!("invalid run id {s:?}: {e}")))
}

#[tokio::main]
async fn main() {
    tracing_subscriber_init();
    let cli = Cli::parse();
    match dispatch(cli).await {
        Ok(out) => {
            print!("{}", ensure_newline(&out.text));
            std::process::exit(out.code);
        }
        Err(e) => {
            eprintln!("torii: {}", e.message);
            std::process::exit(e.code);
        }
    }
}

/// `tracing` output goes to stderr so `--json` on stdout stays machine-parseable.
fn tracing_subscriber_init() {
    // Deliberately minimal: no subscriber crate dependency. `tracing` events are
    // no-ops without a subscriber, and the worker's own status lines go to stdout.
}

fn ensure_newline(s: &str) -> String {
    if s.ends_with('\n') || s.is_empty() {
        s.to_string()
    } else {
        format!("{s}\n")
    }
}

async fn dispatch(cli: Cli) -> Result<Outcome, CliError> {
    let env = boot::env_config()?;
    match cli.command {
        Command::Run { action } => match action {
            RunAction::Status { run_id, json } => {
                let d = boot::light(&env).await?;
                cmd::run::status(d.scheduler_store.as_ref(), parse_run_id(&run_id)?, json).await
            }
            RunAction::ListPaused { json } => {
                let d = boot::light(&env).await?;
                cmd::run::list_paused(d.scheduler_store.as_ref(), json).await
            }
            RunAction::Cancel { run_id } => {
                let d = boot::light(&env).await?;
                cmd::run::cancel(d.scheduler_store.as_ref(), parse_run_id(&run_id)?).await
            }
            RunAction::Wake { run_id } => {
                let d = boot::light(&env).await?;
                let now = chrono::Utc::now();
                cmd::run::wake(d.scheduler_store.as_ref(), parse_run_id(&run_id)?, now).await
            }
            RunAction::Submit { graph, run_id, gateway_config, workspace_root } => {
                let run = match run_id {
                    Some(s) => parse_run_id(&s)?,
                    None => RunId(uuid::Uuid::new_v4()),
                };
                let raw = std::fs::read_to_string(&graph).map_err(|e| {
                    CliError::error(format!("cannot read {}: {e}", graph.display()))
                })?;
                let g: Graph = serde_json::from_str(&raw).map_err(|e| {
                    CliError::error(format!("{} is not a valid graph: {e}", graph.display()))
                })?;
                let d = boot::heavy(&env, &gateway_config, workspace_root.as_deref()).await?;
                // Print the id BEFORE driving: an operator who loses the terminal
                // must still be able to find the run.
                println!("submitted: {}", run.0);
                cmd::run::submit(&d.scheduler, run, g).await
            }
        },
        Command::Worker { action } => match action {
            WorkerAction::Serve { interval, once, gateway_config, workspace_root } => {
                let d = boot::heavy(&env, &gateway_config, workspace_root.as_deref()).await?;
                let shutdown = async {
                    let _ = tokio::signal::ctrl_c().await;
                };
                cmd::worker::serve(
                    &d.scheduler,
                    cmd::worker::ServeOpts { interval, once },
                    shutdown,
                )
                .await
            }
        },
        Command::Config { action } => {
            let d = boot::light(&env).await?;
            match action {
                ConfigAction::Version { json } => cmd::config::version(&d.config_source, json).await,
                ConfigAction::Push { dir, yes } => {
                    let mut confirm = |text: &str| -> bool {
                        eprint!("{text}\nContinue? [y/N] ");
                        use std::io::Write;
                        let _ = std::io::stderr().flush();
                        let mut line = String::new();
                        // EOF or a non-interactive stdin yields 0 bytes -> refuse.
                        match std::io::stdin().read_line(&mut line) {
                            Ok(0) | Err(_) => false,
                            Ok(_) => matches!(line.trim(), "y" | "Y" | "yes"),
                        }
                    };
                    cmd::config::push(&d.config_source, &dir, yes, &mut confirm).await
                }
            }
        }
    }
}
```

- [ ] **Step 2: Add `cmd::run::submit`**

Append to `crates/torii/src/cmd/run.rs`:

```rust
/// Submit a fresh run and drive it inline. Blocks until the run pauses or ends —
/// there is no `--detach` yet, because `enqueue` stamps the row `waking`, so a
/// detached run would only be picked up once the lease expired and the crash-reclaim
/// path grabbed it. Abusing crash recovery as a scheduling primitive was rejected;
/// a real `pending` status is the fix.
pub async fn submit(
    scheduler: &orchestrator::Scheduler,
    run: RunId,
    graph: orchestrator_core::Graph,
) -> Result<Outcome, CliError> {
    let outcome = scheduler.submit(run, graph).await?;
    if let Some(p) = &outcome.paused {
        return Ok(Outcome::ok(format!(
            "paused: {} at node {} ({})",
            run.0, p.node.0, p.reason
        )));
    }
    if let Some((node, msg)) = &outcome.failed {
        return Ok(Outcome::precondition(format!(
            "failed: {} at node {} ({msg})",
            run.0, node.0
        )));
    }
    Ok(Outcome::ok(format!(
        "completed: {} ({} node(s))",
        run.0,
        outcome.completed.len()
    )))
}
```

- [ ] **Step 3: Write the binary smoke tests**

Create `crates/torii/tests/cli.rs`:

```rust
//! Binary-level smoke tests. `CARGO_BIN_EXE_torii` is set by cargo for integration
//! tests, so no `assert_cmd` dependency is needed.

use std::process::Command;

fn torii() -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_torii"));
    // Never inherit a developer's real database.
    c.env_remove("DATABASE_URL");
    c.env_remove("TORII_FENCE_VERSION");
    c
}

#[test]
fn help_lists_all_three_command_groups() {
    let out = torii().arg("--help").output().expect("runs");
    assert!(out.status.success(), "exit: {:?}", out.status);
    let text = String::from_utf8_lossy(&out.stdout);
    for group in ["run", "worker", "config"] {
        assert!(text.contains(group), "missing {group} in help:\n{text}");
    }
}

#[test]
fn a_missing_database_url_fails_with_a_named_variable() {
    let out = torii()
        .args(["run", "list-paused"])
        .output()
        .expect("runs");
    assert_eq!(out.status.code(), Some(1));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("DATABASE_URL"), "{err}");
}

#[test]
fn an_invalid_run_id_is_rejected_before_any_connection() {
    let out = torii()
        .env("DATABASE_URL", "postgres://nobody@127.0.0.1:1/none")
        .args(["run", "status", "not-a-uuid"])
        .output()
        .expect("runs");
    assert_eq!(out.status.code(), Some(1));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("invalid run id"), "{err}");
}

#[test]
fn an_unparseable_interval_is_rejected_by_the_parser() {
    let out = torii()
        .args(["worker", "serve", "--interval", "soon", "--gateway-config", "/nonexistent"])
        .output()
        .expect("runs");
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("invalid interval"), "{err}");
}

/// A connect failure must never echo the password.
#[test]
fn a_connect_failure_does_not_leak_the_password() {
    let pw = format!("s3cr{}t", "e");
    let url = format!("postgres://operator:{pw}@127.0.0.1:1/none");
    let out = torii()
        .env("DATABASE_URL", &url)
        .args(["config", "version"])
        .output()
        .expect("runs");
    assert!(!out.status.success());
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!combined.contains(&pw), "password leaked:\n{combined}");
}
```

- [ ] **Step 4: Run the tests**

Run: `cargo test -p sensei-torii 2>&1 | tail -15; echo "exit=$?"`
Expected: `exit=0`, including the 5 binary tests. If clap's `value_parser` rejects
`parse_interval` because its error type must be `Display + Send + Sync`, the `String` error already
satisfies that; if not, wrap it in `clap::Error` per clap's `value_parser!` docs.

- [ ] **Step 5: Verify the whole workspace**

Run: `cargo test --workspace > /tmp/t10.log 2>&1; echo "exit=$?"; grep -c "test result: ok" /tmp/t10.log`
Expected: `exit=0`.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/torii/src/main.rs crates/torii/src/cmd/run.rs crates/torii/tests/cli.rs
git commit -m "feat(torii): SP-DATA-4 (10/11) — clap dispatch, run submit, binary smoke tests"
```

---

## Task 11: Cross-process e2e, mutation verification, docs

**Files:**
- Modify: `crates/orchestrator/src/lib.rs` (expose `test_support` under a feature)
- Modify: `crates/orchestrator/Cargo.toml` (add the `test-support` feature)
- Modify: `crates/torii/Cargo.toml` (dev-dependency on it)
- Create: `crates/torii/tests/e2e_pg.rs`
- Modify: `docs/superpowers/orchestrator-overview.md`

- [ ] **Step 1: Expose the gateway test doubles to the torii crate**

`crates/orchestrator/src/lib.rs`:

```rust
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
```

`crates/orchestrator/Cargo.toml`:

```toml
[features]
postgres-tests = ["orchestrator-store/postgres", "orchestrator-store/test-support"]
# Exposes the gateway/clock test doubles (`recording_gateway`, `FakeClock`, ...) to
# OTHER crates' dev-dependencies. Dev-only: nothing in a production build enables it.
test-support = []
```

`crates/torii/Cargo.toml` dev-dependencies:

```toml
[dev-dependencies]
tokio = { version = "1", features = ["full", "test-util"] }
orchestrator = { package = "sensei-orchestrator", path = "../orchestrator", features = ["test-support"] }
orchestrator-store = { package = "sensei-orchestrator-store", path = "../orchestrator-store", features = ["postgres", "test-support"] }
uuid = { version = "1", features = ["v4"] }
```

Run: `cargo build --workspace 2>&1 | tail -5; echo "exit=$?"`
Expected: `exit=0` — with the feature off, `test_support` stays `#[cfg(test)]`-private as before.

- [ ] **Step 2: Write the cross-process operator-loop e2e**

Create `crates/torii/tests/e2e_pg.rs`:

```rust
//! AC8 — the operator loop, end to end, across a process boundary.
//!
//! `DATABASE_URL`-guarded (not feature-gated): torii depends on
//! `orchestrator-store/postgres` unconditionally, so absent a database each test
//! returns early and the default suite stays DB-free.
//!
//! Process A submits a graph that pauses on a gated gateway. A FRESH set of
//! stores/executor (process B) — sharing NOTHING in-process — observes the durable
//! pause through the operator commands, queues it, and drives it to completion with
//! zero re-spend.

use chrono::{DateTime, Duration, Utc};
use orchestrator::test_support::{FakeClock, recording_gateway, timeout_gateway};
use orchestrator::{Executor, Scheduler};
use orchestrator_core::{
    Graph, Node, NodeId, NodeKind, RunId, RunStatus, SchedulerStore,
};
use orchestrator_store::postgres::{
    PostgresContentStore, PostgresJournal, PostgresSchedulerStore, connect,
};
use std::sync::Arc;

fn db_url() -> Option<String> {
    std::env::var("DATABASE_URL").ok()
}

fn one_node_graph() -> Graph {
    Graph {
        nodes: vec![Node {
            id: NodeId("n1".into()),
            kind: NodeKind::ModelCall {
                chain: "c".into(),
                payload: serde_json::json!({ "prompt": "go" }),
            },
            deps: vec![],
        }],
    }
}

#[tokio::test]
async fn the_operator_loop_drives_a_paused_run_to_completion_across_processes() {
    let Some(url) = db_url() else { return };

    let run = RunId(uuid::Uuid::new_v4());
    let graph = one_node_graph();
    let clock = FakeClock::new(DateTime::<Utc>::from_timestamp(3_000_000, 0).unwrap());

    // ---- Process A: submit against a gated gateway -> a durable pause ----------
    let store_a = Arc::new(PostgresSchedulerStore::new(connect(&url).await.unwrap()));
    let journal_a = Arc::new(PostgresJournal::new(connect(&url).await.unwrap()));
    let gw = timeout_gateway().await;
    let exec_a = Executor::new(Arc::new(gw), journal_a.clone(), "v1").with_clock(clock.clone());
    let sched_a = Scheduler::new(store_a.clone(), exec_a, journal_a.clone(), clock.clone());
    let out = sched_a.submit(run, graph.clone()).await.unwrap();
    assert!(out.paused.is_some(), "the run pauses on the gate");

    // ---- Operator, light tier: the run shows up in the pending-wake view -------
    let store_b = Arc::new(PostgresSchedulerStore::new(connect(&url).await.unwrap()));
    let listed = store_b.list_paused().await.unwrap();
    assert!(
        listed.iter().any(|r| r.run == run),
        "list-paused must surface the durable pause"
    );

    let status = store_b.status(run).await.unwrap().expect("a record");
    assert_eq!(status.status, RunStatus::Paused);

    // ---- Operator: queue it for the next tick ---------------------------------
    let woken_at = status.next_wake.unwrap_or_else(|| clock.now()) + Duration::seconds(1);
    store_b.force_wake(run, woken_at).await.unwrap();
    assert_eq!(
        store_b.status(run).await.unwrap().unwrap().next_wake,
        Some(woken_at),
        "force_wake sets the deadline; it does NOT drive the run"
    );

    // ---- Process B: a FRESH worker drives it ----------------------------------
    let journal_b = Arc::new(PostgresJournal::new(connect(&url).await.unwrap()));
    let (gw_b, calls_b) = recording_gateway().await;
    let clock_b = FakeClock::new(woken_at + Duration::seconds(1));
    let exec_b = Executor::new(Arc::new(gw_b), journal_b.clone(), "v1")
        .with_content_store(Arc::new(PostgresContentStore::new(
            connect(&url).await.unwrap(),
        )))
        .with_clock(clock_b.clone());
    let sched_b = Scheduler::new(store_b.clone(), exec_b, journal_b.clone(), clock_b.clone());

    let out = torii_serve_once(&sched_b).await;
    assert_eq!(out.code, 0, "{}", out.text);

    // ---- The run completed, and the completed prefix was not re-spent ---------
    assert_eq!(
        store_b.status(run).await.unwrap().unwrap().status,
        RunStatus::Completed,
        "the woken run completes in the fresh process"
    );
    assert_eq!(
        calls_b.lock().unwrap().len(),
        1,
        "exactly the one un-run node — zero re-spend of the completed prefix"
    );
}

/// Drive one tick through torii's own worker loop, so the e2e exercises the CLI's
/// code path rather than calling `Scheduler::tick` directly.
async fn torii_serve_once(sched: &Scheduler) -> ServeOutcome {
    // The worker module is private to the binary, so the e2e re-implements the
    // single-tick contract it is asserting: exactly one tick, then stop.
    match sched.tick().await {
        Ok(n) => ServeOutcome { code: 0, text: format!("tick complete: {n} run(s) woken") },
        Err(e) => ServeOutcome { code: 1, text: e.to_string() },
    }
}

struct ServeOutcome {
    code: i32,
    text: String,
}
```

**Note on the last helper:** `cmd::worker` lives in a binary crate, so an integration test cannot
import it. Two honest options — pick one and record it:
- **(a)** Keep the helper above, and rely on Task 8's unit tests for `serve`'s policy. The e2e then
  proves the *durable* claim/drive path, not torii's loop code.
- **(b)** Convert `crates/torii` to a lib+bin (`src/lib.rs` exporting the modules, `src/main.rs`
  depending on it) so the e2e calls `torii::cmd::worker::serve` directly.

**Choose (b)** — it is a ~10-line change and it removes the duplication that option (a) bakes into
the test. Add `[lib] name = "torii"` / `path = "src/lib.rs"` to `crates/torii/Cargo.toml`, move the
`mod` declarations into `src/lib.rs` as `pub mod`, and reduce `src/main.rs` to the clap types plus
`dispatch`. Then replace the helper with:

```rust
    let out = torii::cmd::worker::serve(
        &sched_b,
        torii::cmd::worker::ServeOpts {
            interval: std::time::Duration::from_millis(10),
            once: true,
        },
        std::future::pending(),
    )
    .await
    .expect("one tick");
    assert_eq!(out.code, 0, "{}", out.text);
```

- [ ] **Step 3: Run the e2e against Docker Postgres**

```bash
cargo test -p sensei-torii --test e2e_pg -- --test-threads=1
echo "exit=$?"
```

Expected: `exit=0`, 1 test passed. Confirm it did not skip — `DATABASE_URL` must be set.

- [ ] **Step 4: Add the `config push` DB test**

Append to `crates/torii/tests/e2e_pg.rs`:

```rust
/// AC3 + AC4 against a live database: validation refuses before writing, an
/// unconfirmed removal writes nothing, and a confirmed push bumps the generation.
#[tokio::test]
async fn config_push_validates_refuses_and_then_applies() {
    let Some(url) = db_url() else { return };
    use orchestrator_core::ConfigSource;
    use orchestrator_store::postgres::PostgresConfigSource;

    let src = PostgresConfigSource::new(connect(&url).await.unwrap());
    let dir = std::env::temp_dir().join(format!("torii-cfg-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(dir.join("skills")).unwrap();
    std::fs::write(dir.join("skills").join("a.md"), "---\nname: a\n---\nbody\n").unwrap();

    // Seed a known durable state, then push the one-skill directory.
    let v0 = src.store_and_bump(&orchestrator_core::RegistryConfig::default()).await.unwrap();
    let mut always_yes = |_: &str| true;
    let out = torii::cmd::config::push(&src, &dir, false, &mut always_yes).await.unwrap();
    assert_eq!(out.code, 0, "{}", out.text);
    let v1 = src.version().await.unwrap().unwrap();
    assert_eq!(v1, v0 + 1, "a successful push bumps exactly once");

    // An empty directory REMOVES the skill -> unconfirmed must write nothing.
    let empty = std::env::temp_dir().join(format!("torii-cfg-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&empty).unwrap();
    let mut always_no = |_: &str| false;
    let refused = torii::cmd::config::push(&src, &empty, false, &mut always_no).await.unwrap();
    assert_eq!(refused.code, 2, "{}", refused.text);
    assert!(refused.text.contains("refused"), "{}", refused.text);
    assert_eq!(
        src.version().await.unwrap().unwrap(),
        v1,
        "a refused push must not advance the generation"
    );
    assert!(
        src.load().await.unwrap().skills.iter().any(|s| s.name == "a"),
        "a refused push must not delete content"
    );

    // A directory that does not assemble must be refused BEFORE any write.
    let bad = std::env::temp_dir().join(format!("torii-cfg-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&bad).unwrap();
    std::fs::write(
        bad.join("chains.json"),
        r#"[{"area":"r","kind":"k","chain":"a"},{"area":"r","kind":"k","chain":"b"}]"#,
    )
    .unwrap();
    let err = torii::cmd::config::push(&src, &bad, true, &mut always_yes).await;
    assert!(err.is_err(), "a duplicate chain binding must be refused");
    assert_eq!(
        src.version().await.unwrap().unwrap(),
        v1,
        "an invalid push must not advance the generation"
    );

    std::fs::remove_dir_all(&dir).ok();
    std::fs::remove_dir_all(&empty).ok();
    std::fs::remove_dir_all(&bad).ok();
}
```

Run: `cargo test -p sensei-torii --test e2e_pg -- --test-threads=1; echo "exit=$?"`
Expected: `exit=0`, 2 tests passed.

- [ ] **Step 5: Mutation-verify the two guards**

Prove these tests are real guards, not theatre. **Commit the working code first** — a
`git checkout` to revert a mutation also discards uncommitted work.

```bash
git status --short   # must be clean before mutating
```

Mutation 1 — break the diff's removal detection in `crates/torii/src/diff.rs`, inside `compare`:

```rust
    // MUTATION: skip removal detection entirely
    for name in current.keys() {
        if !incoming.contains_key(name) && false {
            out.removed.push(DiffEntry { kind, name: name.clone() });
        }
    }
```

Run: `cargo test -p sensei-torii diff; echo "exit=$?"`
Expected: **FAIL** (`an_empty_incoming_config_reports_everything_removed`). Then revert:
`git checkout crates/torii/src/diff.rs`

Mutation 2 — revert `load_versioned` to the torn two-read form in
`crates/orchestrator-store/src/postgres.rs`:

```rust
    async fn load_versioned(
        &self,
    ) -> Result<(RegistryConfig, Option<u64>), OrchestratorError> {
        Ok((self.load().await?, self.version().await?)) // MUTATION: no snapshot
    }
```

Run: `cargo test -p sensei-orchestrator-store --features postgres,test-support load_versioned_is_immune -- --test-threads=1; echo "exit=$?"`
Expected: **FAIL** — the concurrent write becomes visible mid-read. Then revert:
`git checkout crates/orchestrator-store/src/postgres.rs`

Re-run both suites after reverting to confirm green:

```bash
cargo test -p sensei-torii; echo "exit=$?"
cargo test -p sensei-orchestrator-store --features postgres,test-support -- --test-threads=1; echo "exit=$?"
```

- [ ] **Step 6: Full verification**

```bash
cargo fmt --all --check; echo "fmt exit=$?"
cargo clippy --workspace --all-targets -- -D warnings; echo "clippy exit=$?"
cargo test --workspace > /tmp/final.log 2>&1; echo "test exit=$?"; tail -30 /tmp/final.log
cargo test -p sensei-orchestrator-store --features postgres,test-support -- --test-threads=1; echo "store-pg exit=$?"
cargo test -p sensei-orchestrator --features postgres-tests -- --test-threads=1; echo "orch-pg exit=$?"
cargo test -p sensei-torii --test e2e_pg -- --test-threads=1; echo "torii-pg exit=$?"
```

Expected: every `exit=0`. Record the workspace test count and confirm no pre-existing test was
removed or skipped.

- [ ] **Step 7: Update the overview doc**

In `docs/superpowers/orchestrator-overview.md`:
1. Add an **SP-DATA slice 4** bullet to the decision log, in the same dense style as slices 1-3:
   the two carry-forwards closed (atomic read via a defaulted trait method so `reload` is fixed too,
   atomic write removing the crash window), the footgun gated behind `test-support`, torii's two boot
   tiers, honest effect reporting on `cancel`/`wake`, validate-before-write on `config push`, the
   explicit `TORII_FENCE_VERSION`, and the accepted tradeoff that `sqlx` now compiles by default.
2. Add the spec + plan to the index table with status `✅ merged (develop)`.
3. Update the SP-DATA feature-status line: s4 done; **s5 cost/token budget** is the remaining slice.
4. Carry forward, explicitly: no HTTP surface; no `--detach`; wake backoff/`max_attempts` still open
   (a poison-pill run can crash-loop a worker); pause reasons still unredacted in operator output;
   no `config pull`/rollback; pool sizing still hardcoded.

- [ ] **Step 8: Commit and push**

```bash
cargo fmt --all
git add -A
git commit -m "test(torii): SP-DATA-4 (11/11) — cross-process operator-loop e2e + config-push DB tests + mutation verification + docs"
git push origin develop
```

---

## Self-Review

**Spec coverage.** Every numbered spec section maps to a task: §4.1 two tiers → Task 9; §4.2 gateway
config file → Task 9; §5 command semantics → Tasks 6, 7, 10; §5.1 honest reporting → Task 6;
§5.2 validate-before-write → Task 7; §5.3 inline submit → Task 10; §6.1 atomic read → Tasks 4 + 5;
§6.2 atomic write → Task 5; §6.3 footgun gate → Task 5; §7.1 loop resilience → Task 8; §7.2 crash-loop
limitation → documented in Task 11 Step 7; §7.3 shutdown → Task 8; §7.4 error mapping → Task 1;
§7.5 secret hygiene → Tasks 1, 9, 10; §7.6 explicit fence → Task 9; §8 pause-reason exposure →
Task 11 Step 7 carry-forward.

**Acceptance criteria.** AC1 → Task 10 (`help`, missing-fence) + Task 9; AC2 → Task 6; AC3 → Task 11
Step 4; AC4 → Task 7 + Task 11 Step 4; AC5 → Task 5; AC6 → Task 5; AC7 → Task 5 Step 6; AC8 → Task 11
Step 2; AC9 → Task 8; AC10 → Tasks 1 + 10; AC11 → Tasks 2 + 11 Step 5; AC12 → Steps 6 of Tasks 4, 5
and Task 11 Step 6; AC13 → Task 11 Step 6.

**Two open implementation decisions flagged in-plan rather than papered over:**
1. Task 11 Step 2 — converting `torii` to lib+bin so the e2e can import `cmd::worker`. Option (b) is
   chosen with the reason stated.
2. Task 4 Step 1 — `RegistryHandle::generation()` may not be public; the fallback assertion is given.

**Type consistency checked:** `Outcome` (`text`/`code`) is used identically in Tasks 6-10;
`CliError` (`message`/`code`) throughout; `ConfigDiff`/`DiffEntry`/`EntityKind` from Task 2 are used
unchanged in Task 7; `Ticker`/`ServeOpts` from Task 8 are used unchanged in Task 10 and Task 11;
`EnvConfig`/`LightDeps`/`HeavyDeps` from Task 9 are used unchanged in Task 10.
