---
title: SP-DATA slice 1 — Durable run state (PostgresJournal + persistent CAS + fence)
doctype: design
module: orchestrator
spec: SP-DATA
status: approved
companion: ./2026-08-06-sensei-orchestrator-design.md (§ persistence / data-tier, config-versioning); ./2026-08-08-sp1-orchestrator-spine-design.md (the `ExecutionJournal` + effect_id + version fence this makes durable); ./2026-08-09-sp1-slice3-fanout-blackboard-cas-design.md (the `ContentStore`/`ContextStore` CAS+blackboard); ./2026-08-12-sp2-hot-reload-design.md (the in-process config generation this phase later makes durable); crates/vault (the schema-agnostic sqlx Postgres-adapter precedent); ~/Developer/torii/database (the dbd schema convention)
date: 2026-08-17
---

# SP-DATA slice 1 — Durable run state (PostgresJournal + persistent CAS + fence)

## 1. Goal

Make a run's durable state — its **journal**, its content-addressed **blobs** (CAS), and its
**blackboard context** — persist in Postgres, so **a run journaled in one process/host resumes
in a fresh process against the same database, with no token re-spend and no silent
corruption**. This is the first slice of SP-DATA (the persistence / control-plane phase that was
deliberately held off while the SP-0→SP-4 runtime was built on in-memory config). It turns the
executor's already-durable-by-design replay model (journal + memo + fence) into a *cross-process*
one, and makes the two-phase Mutation `in_doubt→reconcile` real across a true crash.

**Scope:** Postgres implementations of the three existing run-state seams — `ExecutionJournal`,
`ContentStore`, `ContextStore` — in `orchestrator-store` behind a `postgres` feature, against a
**dbd-managed schema** (`gateway/database/`, the sensei/torii family convention), plus a durable
**journal-format / effect-id version fence**. The `Executor` is UNCHANGED (it injects these
trait objects through the existing backend-agnostic seam). Config persistence, the scheduler, the
management/control plane, and the cost/budget model are the later SP-DATA slices.

**Full SP-DATA phase (user-chosen, built slice-by-slice):** ‑1 durable run state (this) · ‑2
durable config (`PostgresConfigSource` + `config_versions`/`bump_config_version`) · ‑3 durable
scheduler (re-arm a paused run at `resume_after`) · ‑4 management CLI/API · ‑5 cost/token budget
model.

## 2. Background & impact review

- **The seams already exist and are async + backend-agnostic** (`orchestrator-core`):
  `ExecutionJournal { append→Seq, load, load_since, snapshot, compact }`,
  `ContentStore { put→Digest, get }`, `ContextStore { put, get, load, insert_ref }`. The whole
  point of these traits was a swappable backend; `orchestrator-store` already ships the
  `InMemory*` impls (the parity reference). This slice adds `Postgres*` siblings.
- **sqlx 0.8 (postgres) is the workspace's established DB library** — `crates/vault` uses it
  behind an optional `sqlx` feature as a **schema-agnostic adapter** (the consumer supplies the
  tables). SP-DATA follows both: sqlx 0.8 + a feature-gated adapter.
- **The schema convention is dbd** — `~/Developer/torii/database/{design.yaml, ddl/}` is a
  dbd-managed project, and a `dbd-pattern-verifier` agent reviews it. SP-DATA defines its schema
  in `gateway/database/` (dbd), NOT embedded in the Rust crate.
- **The version fence exists but is in-process** — resume fences on `RunStarted.version`
  (`"{executor}#cfg{gen}"`). Once journals PERSIST, two new durable break-risks appear: the
  **effect-id scheme change** (an SP-1-s3 journal-format break) and the **journal schema shape** —
  a persisted journal from an incompatible format must fence LOUDLY, not silently mis-resume.
- **Impact:** additive — a `postgres` feature on `orchestrator-store` (off by default ⇒ the
  InMemory path + the whole existing 1120-test suite are byte-identical); new dbd schema files;
  no `orchestrator`/`orchestrator-core` change beyond a `format_version` fence constant. A
  deployment opts in by injecting the `Postgres*` stores.

## 4. Design

### 4.1 Schema (dbd, in `gateway/database/`)

Logical shape (expressed as dbd `design.yaml` + `ddl/`, matching torii's convention; reviewed by
the `dbd-pattern-verifier` agent):

- **`journal_events`** — `seq BIGSERIAL PRIMARY KEY` (the global monotonic `Seq`), `run_id uuid`,
  `event jsonb` (the serialized `JournalEvent`), `created_at timestamptz default now()`. Index
  `(run_id, seq)`. `append` = `INSERT … RETURNING seq` (atomic, concurrency-safe — the DB assigns
  the monotonic Seq). `load(run)` = `SELECT … WHERE run_id=$ ORDER BY seq`. `load_since(run,seq)` =
  `… AND seq > $`.
- **`cas_blobs`** — `digest text PRIMARY KEY`, `bytes bytea`, `created_at`. `put` =
  `INSERT … ON CONFLICT (digest) DO NOTHING` (content-addressed dedupe), `get` = `SELECT bytes`.
- **`context_refs`** — the blackboard `ContextStore`: `(scope, key)` → a content ref/digest
  (resolved via `cas_blobs`), reject-on-collision like the InMemory impl.
- **`run_snapshots`** — `run_id uuid PRIMARY KEY`, `seq bigint`, `snapshot jsonb`; latest-wins
  upsert for `snapshot`/`latest_snapshot`.
- **`runs`** (or a column on the first `RunStarted`) — carries the durable **`format_version`**
  (see §4.3) + `run_id`, `created_at`, terminal status (for later slices' queries).

All DDL idempotent; enums as Postgres enums (not string CHECKs) per dbd conventions; secrets never
in `design.yaml`.

### 4.2 The adapter (`orchestrator-store`, `postgres` feature)

A `sqlx::PgPool` injected into three structs, each implementing its async trait:
- `PostgresJournal { pool }` — `append`/`load`/`load_since`/`snapshot`/`latest_snapshot`; `compact`
  = a transaction (`DELETE … WHERE seq = ANY($remove)` then `INSERT $add`), matching the
  remove-then-append primitive.
- `PostgresContentStore { pool }` — `put` (dedupe insert) / `get`.
- `PostgresContextStore { pool }` — `put`/`get`/`load`/`insert_ref`.

Schema-agnostic like `vault`: the crate runs against whatever the dbd schema created; it does not
embed or run migrations. All errors map to the existing `JournalError`/`OrchestratorError` (a
Postgres error → the loud variant; a missing blob → the same `ContentDigestMiss` the InMemory impl
raises, so the fold behaves identically).

### 4.3 Cross-process resume + the durable fence (the crux)

- **Journal + CAS + context share one database** so an `EffectOutput::Ref` materializes from the
  persisted CAS during a resume fold — transactional consistency, no dangling refs.
- **Cross-process resume:** process B (a fresh `Executor` of the same version + config generation,
  wired with a `PostgresJournal`/`PostgresContentStore` on the same DB) calls `.start(run, graph)`
  → folds the persisted journal → resumes with **zero gateway calls for completed effects**
  (the memo replays from the durable journal). This is the SP-DATA headline: durable resume is no
  longer bound to one process's memory.
- **The durable fence:** on resume the executor's existing `RunStarted.version` fence
  (`VersionFenceMismatch`) still applies (executor + config gen). This slice adds a
  **`format_version`** — a constant stamping the journal's effect-id/serialization scheme — stored
  per run and checked on load; a mismatch fences LOUDLY (`JournalError::IncompatibleFormat`),
  never a silent mis-fold. This closes the SP-1-s3 "effect-id scheme change is a journal-format
  break once journals persist" carry-forward.
- **Durable in-doubt reconcile:** a crash between the two-phase `EffectIntent` and `EffectRecorded`
  now leaves a standing Intent IN POSTGRES → resume sees `teid ∈ fold.intents` → the existing
  `in_doubt→reconcile` (Confirmed/NotApplied/Indeterminate→pause) runs for real across a process
  death — the mechanism SP-1-s4 built, now durable.

### 4.4 Human-on-the-loop — a design note (mechanism DEFERRED)

Two human patterns matter for the agentic system, and this slice is the substrate for one of them:
- **Human-in-the-loop** (the human is a blocking node — `HumanGate`/`AwaitSignal`, human-as-Agent)
  = **SP-6**; needs the durable pause + signal delivery this phase enables.
- **Human-on-the-loop** (supervisory: the run runs autonomously, the human OBSERVES and can
  INTERVENE — pause/cancel/resume/redirect — out of band). Its **observe** half is exactly the
  durable, queryable, streamable run state this slice delivers (the persisted `journal_events` +
  the existing `OrchestratorHooks` live feed). Its **intervene** half (an out-of-band control
  request the executor honors at node boundaries + the control API) is DEFERRED to the SP-DATA-4 /
  SP-6 boundary.

**Design obligation for THIS slice (so intervention isn't a painful retrofit):** the durable model
must remain *control-ready* — do NOT preclude representing an out-of-band control request. Concretely:
the append-only `journal_events` + `runs` shape can later carry a `RunControlRequested{pause|cancel|
…}` event or a `run_control` row without a schema break (jsonb events + an additive table). No
control mechanism, table, or executor change ships in ‑1 — this is a note + a shape constraint only.

### 4.5 Verification (dbd + Docker Postgres, feature-gated)

The `postgres` tests need a live database, so — like the sandbox slice's Docker harness — a
**Docker Postgres** + the **dbd-applied schema** + `cargo test -p sensei-orchestrator-store
--features postgres`. Default `cargo test` (feature off, no DB) is untouched. CI gets a `postgres`
service. A reusable harness (a `docker run postgres` + `dbd apply`/the ddl + `DATABASE_URL`) is
documented in the plan, analogous to the Linux slice's Docker command.

### 4.6 Additive & trust boundary

- **Additive:** the `postgres` feature is off by default ⇒ `InMemory*` remain the wired stores ⇒
  the full existing suite is **byte-identical** (1120). A deployment opts in.
- **Trust boundary:** the Postgres stores are a durability/availability layer; determinism +
  no-re-spend + the fence are inherited unchanged from the executor (the backend only persists the
  same events the InMemory impl held). Tenant-agnostic core is preserved — a tenant is still a
  wrapper (one DB/pool per tenant); no `tenant_id` enters the core traits.

## 5. Decisions

- **D1 — global `BIGSERIAL` for `Seq`** [approved]: the DB assigns the monotonic Seq atomically on
  `INSERT … RETURNING`; concurrency-safe (Map fan-out appends), monotonic per run (the fold orders
  by seq), and the Seq is fold-ordering only (never hashed) so sparse-per-run values are fine.
- **D2 — CAS blobs as Postgres `bytea`** [approved]: transactional with the journal, one backend;
  the 4 KiB CAS threshold bounds what lands here. External/S3 blob store deferred (§6).
- **D3 — journal + CAS + context in ONE database** [approved]: transactional consistency so a
  resume never sees a journaled `Ref` without its blob.
- **D4 — a durable `format_version` fence** [approved]: closes the effect-id/journal-format break;
  a mismatch fences loud, never a silent mis-fold.
- **D5 — feature-gated + Docker-Postgres verification** [approved]: default `cargo test` stays
  DB-free + byte-identical; the durable path is proven on a real Postgres (Docker + CI service).
- **D6 — dbd schema in `gateway/database/`; `orchestrator-store` a schema-agnostic sqlx adapter**
  [approved]: matches the torii/sensei dbd convention + vault's adapter precedent; reviewed by the
  `dbd-pattern-verifier` agent.

## 6. Deferred (stated)

- **External/S3 blob store** for large CAS objects (bytea is fine at the current bounded sizes).
- **Journal retention / GC / partitioning** (old-run pruning; a `runs` lifecycle) — a later
  ops concern.
- **The human-on-the-loop intervene mechanism** (control request + executor honoring it + the
  control API) — SP-DATA-4 / SP-6 (see §4.4).
- **Config persistence** (`PostgresConfigSource` + durable `config_versions`) — SP-DATA-2;
  **durable scheduler** — SP-DATA-3; **management CLI/API** — SP-DATA-4; **cost/token budget** —
  SP-DATA-5.
- **Pool sizing / retry / backpressure tuning** — start with sqlx defaults.

## 7. Acceptance criteria (TDD; `--features postgres`, run on Docker Postgres + CI)

1. **Journal round-trip parity.** `PostgresJournal` `append`→monotonic `Seq`, `load` returns the
   events in `seq` order, `load_since(seq)` filters — byte-identical semantics to `InMemoryJournal`
   (a shared parity test-suite run against both backends where practical).
2. **Snapshot + compact.** `snapshot`/`latest_snapshot` upsert+read; `compact(remove_seqs, add)`
   removes the seqs and appends the event transactionally (a Map's child events collapse to a
   `MapCompacted` exactly as InMemory).
3. **CAS dedupe + get.** `put` of identical bytes twice yields one row (same digest); `get`
   returns the bytes; a missing digest raises the same miss error the fold expects.
4. **THE HEADLINE — cross-process resume.** Journal a partial run via one `PostgresJournal` +
   `PostgresContentStore` (to a Docker PG); construct a **fresh** `Executor` + fresh
   `PostgresJournal`/`ContentStore` instances on the SAME DB; `.start(run, graph)` → the run
   COMPLETES, completed effects replay from the durable journal with **zero gateway calls**
   (a fake-gateway counter proves no re-spend), and a journaled `EffectOutput::Ref` materializes
   from the persisted CAS.
5. **In-doubt reconcile is durable.** Seed a journal with a standing `EffectIntent` and no
   `EffectRecorded` (a simulated crash mid-Mutation) in Postgres; resume via a fresh executor →
   the `in_doubt→reconcile` path runs (not a blind re-run), matching the SP-1-s4 in-doubt behavior.
6. **Format-version fence.** A persisted journal stamped with a different `format_version` →
   resume fences LOUDLY (`IncompatibleFormat`), zero gateway calls; a matching version resumes.
7. **Additive / byte-identical.** With the `postgres` feature OFF, the full existing workspace
   suite (1120) passes unchanged; the `InMemory*` stores are the default; no `orchestrator` core
   behavior changes.
