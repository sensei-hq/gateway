# SP-DOC-1 — doc-truth pass Implementation Plan

> **For agentic workers:** Steps use checkbox (`- [ ]`) syntax. Task ids here are `SP-DOC-1 Task N`.
> They are NOT any implementation slice's `Task N`, and NOT `SP-7b.1 Task 1-5`, which is complete.

**Goal:** Correct every specification claim about EXISTING CODE that the code falsifies. Docs only —
no behaviour change, no test change.

**Why this is a plan and not a spec->plan cycle:** every item is a finding produced by a 13-agent
audit and then re-derived by an independent adversarial verifier instructed to REFUTE. 36 candidates
went in, 5 were refuted, 31 survived. Three were additionally re-checked by hand before this plan
was written (the sequential DAG loop, the unwired snapshot resume, the missing M1 annotation).

**Why it matters, concretely:** this is the defect class that has now bitten three times in two
slices — blackboard D5's dead `summary` branch, D5 and AC7 citing a `PromptOverBudget` halt deleted
by SP-7a, and SP-7b §5.2 naming the human path's wrapper as the model path's renderer. A false claim
about code reads as perfectly good prose; only a check against the repo exposes it, and the next
slice builds on it.

**Method for every item:** AMEND, do not silently rewrite — quote the original and put the
correction beside it, the convention SP-7b §5.5 and the D5 amendment already use. A design record's
history is what explains why the code looks as it does.

**Baseline:** `cargo test --workspace` = 1760 passed / 0 failed at `15fff55`; clippy and fmt clean.
Docs-only, so the baseline must be UNCHANGED at the end, not merely green.

---

## Coverage

| Task | Area | Items |
|---|---|---|
| 1 | SP-0 / SP-1 specs and plans | 6 |
| 2 | SP-2 / SP-3 specs | 5 |
| 3 | SP-4 specs and the Linux-sandbox plan | 8 |
| 4 | SP-DATA-1/2/3 specs | 6 |
| 5 | SP-DATA-4/5 specs | 3 |
| 6 | SP-6 specs | 3 |

**31 items across 6 tasks, touching 23 files.** Partitions are disjoint by file, so the tasks are independent.

---

## Task 1: SP-0 / SP-1 specs and plans — 6 items

- [x] **`docs/superpowers/specs/2026-08-09-sp1-slice3-fanout-blackboard-cas-design.md:65 (also :33)`**
  - *Claims:* "The executor dispatches all ready nodes concurrently, capped by `Executor.concurrency` (default 8; a `tokio::sync::Semaphore`)"; §1:33 "a **DAG scheduler** (ready-node dispatch under a bounded global concurrency cap)".
  - *Verified false by:* `sed -n '1175,1195p' crates/orchestrator/src/executor/mod.rs` → the drive loop is `let ready = ready_nodes(..); if ready.is_empty() { break; } for node in ready { let result = self.run_node(run, node, fold, &state.outcome.outputs, nested).await?; self.apply_node_result(..).await?; }` — one `.await` per node, strictly sequential; no spawn/join at th
  - *Correction:* §3.2 second bullet should read: "The executor computes the ready set and runs those nodes **sequentially, one at a time** (`drive`'s `for node in ready { run_node(..).await }`) — declaration order, deterministic. Concurrency lives INSIDE a `Map`, not across the DAG: `Executor.concurrency` (default 8) is not a scheduler cap but the ceiling on how many `Map` **children** run at once (`min(map.concurrency, executor.concurrency)`), and the only `tokio::sync::Semaphore` is in `executor/fanout.rs`. Two independent ready branches of a DAG therefore run one after the other. After a round completes it re-computes ready nodes, writes a snapshot (§5.2), and continues." §1:33 should say "a **DAG scheduler** (round-based ready-node dispatch, sequential within a round; the bounded concurrency cap applie

- [x] **`docs/superpowers/specs/2026-08-09-sp1-slice3-fanout-blackboard-cas-design.md:122 (AC8 at :142; decision 4 at :24)`**
  - *Claims:* "**Resume** = load the latest `Snapshot` + replay only the journal tail (events with `Seq >` the snapshot's) → bounds fold cost for wide/long runs." AC8: "a run that dies mid-fan-out resumes from the latest snapshot + tail and re-spends nothing".
  - *Verified false by:* `sed -n '1034,1080p' crates/orchestrator/src/executor/mod.rs` → the resume entry `start_inner` does `let events = self.journal.load(run).await` (the WHOLE journal) then `let (fold, node_last_output, completed) = fold_journal(&events);`; no `latest_snapshot`, no `load_since`. `rg --no-ignore -g '!target' -n 'latest_snapshot' crates/ docs/` → 12 hits
  - *Correction:* §5.2 second bullet: "**Snapshots are write-only today.** `write_snapshot` runs at every round boundary, but resume folds the WHOLE journal — `Executor::start_inner` calls `journal.load(run)` and `fold_journal`; `latest_snapshot` has no non-test caller and `load_since`'s only production caller is `Scheduler::earliest_resume_after` (which folds no run state). Snapshot-seeded resume is an UNWIRED optimisation; `Snapshot` now carries `spent`/`budget` — the two scalars a tail-only fold cannot re-derive — so whoever wires it does not silently uncap the budget gate." AC8 should read: "**Resume without re-spend (headline)** — a run that dies mid-fan-out resumes by folding the FULL journal and re-spends nothing for completed children. (Snapshot-seeded tail-only resume is not wired; the shipped snap

- [x] **`docs/superpowers/plans/2026-08-11-sp1-quota-pause.md:5 (restated at :108-122, :148-153, :245, :369)`**
  - *Claims:* "`AllGated{None}` (all gates terminal) → fail-fast with the human-action hint", with the shipped classifier quoted at :148-153 ("only `AllGated{resume_after: Some(t)}` pauses … every other error — including `AllGated{None}` — fails") and a guard test at :108-122 asserting `AllGated{resume_after: Non
  - *Verified false by:* `rg -n 'fn classify_gateway_error' -B 50 -A 40 crates/orchestrator/src/executor/support.rs` → support.rs:696-703 now carries a SECOND pause arm: `GatewayError::AllGated { resume_after: None, human_action: Some(_), .. } => GatewayDisposition::Pause { resume_after: None, reason: err.to_string() }`, and the function doc at :645 is headed "# The human-
  - *Correction:* Add to the Goal at :5 and to the Task-2 block, matching the wording its two siblings already carry: "(**Superseded 2026-09-04:** risk M1 was reversed. `AllGated { resume_after: None, human_action: Some(_) }` now PAUSES indefinitely — the HOTL class, `resume_after: None` so the scheduler stores a NULL `next_wake` and only `force_wake` clears it. Only an `AllGated` carrying NEITHER a deadline nor an action still fails. `GatewayDisposition::Pause.resume_after` is `Option<DateTime<Utc>>`, and the Task-2 guard test was renamed to `classify_gateway_error_pauses_on_a_deadline_or_a_human_action_and_fails_on_neither`, whose human-action case now asserts `Pause`. See M1 in `docs/design/selection-policy-pipeline.md`.)"

- [x] **`docs/superpowers/specs/2026-08-09-sp1-slice3-fanout-blackboard-cas-design.md:125 (AC9 at :143)`**
  - *Claims:* "Once a `Map`'s children are all terminal **and** its `Consolidate` is `Completed`, the per-child journal records collapse to `{ index, status, digest }`" — stated for every Map, with no body qualifier.
  - *Verified false by:* `rg --no-ignore -g '!target' -n 'compact_map|consolidate_compaction_target' crates/ | grep -v tests.rs` → 6 hits; the ONLY call site is executor/mod.rs:1245-1246 `if let Some(over) = consolidate_compaction_target(graph, node) { self.compact_map(run, over, &state.outcome).await?; }`. `sed -n '39,60p' crates/orchestrator/src/executor/support.rs` → th
  - *Correction:* §5.3: "Once a `Consolidate` over a **`ModelCall`-body** `Map` is `Completed`, that Map's per-child `EffectRecorded` records collapse to a `MapCompacted` manifest of `{ index, status, digest, input_hash, usage }` … **An `Agent`-body `Map` is never compacted** — its children are multi-effect ReAct sub-runs (`consolidate_compaction_target` returns `None` for them), so their per-child records stay in the hot fold path. Compaction also requires a configured `ContentStore`; a no-CAS executor skips it (nowhere to keep the content addressable)." AC9 should name the ModelCall-Map case it actually covers, and state the Agent-Map non-compaction as the explicit gap.

- [x] **`docs/superpowers/specs/2026-08-09-sp1-slice3-fanout-blackboard-cas-design.md:121 (variant also listed in §2:46)`**
  - *Claims:* "After each **round** the executor writes `SnapshotWritten{seq}` + a `Snapshot { completed, skipped, memo_digests, context_refs, map_manifests }` to the store"; §2 lists `SnapshotWritten{seq}` among the new `JournalEvent` variants slice 3 adds to `orchestrator-core`.
  - *Verified false by:* `rg --no-ignore -g '!target' -n 'SnapshotWritten' .` → exactly 2 matches, both inside this one design file (:46 and :121); zero in `crates/`. The variant list (`sed -n '129,720p' crates/orchestrator-core/src/journal.rs | rg '^ [A-Z]'`) is RunStarted, NodeStarted, EffectRecorded, EffectIntent, NodeCompleted, NodeFailed, NodeSkipped, MapExpanded, Map
  - *Correction:* §5.2 first bullet: "After each **round** (§3.2) the executor writes a `Snapshot { seq, completed, skipped, outputs, spent, budget }` to the journal's snapshot store (keyed by `RunId`, latest wins) **out-of-band — there is NO journal event for it**, deliberately, so the control-flow event order stays byte-identical to slice-1/2. The per-effect memo for a partially-completed node is rebuilt by folding the tail rather than stored; the blackboard's `context_refs` and per-Map manifests are not carried." §2's variant list should drop `SnapshotWritten{seq}` (and, for accuracy, add `MapCompacted`, which slice 3 did ship).

- [x] **`docs/superpowers/specs/2026-08-07-sp-cat-catalog-design.md:156`**
  - *Claims:* "**'Refresh' in SP-CAT = re-audit/validation**, NOT an external fetch: a `catalog::audit(catalog)` pass recomputes totals, validates pool-dedup consistency, and a **test/CI gate** fails the build if a documented headline … drifts from `free_tier_totals`."
  - *Verified false by:* `rg --no-ignore -g '!target' -n '\baudit\b' crates/gateway/src/` → exit 1, zero matches (no definition, no re-export, no rename candidate inside the crate). `rg --no-ignore -g '!target' -n '^pub fn ' crates/gateway/src/catalog/*.rs` → the module's whole public surface is `cost_band` (mod.rs:17), `assemble` (assemble.rs:62), `free_tier_totals` (tota
  - *Correction:* §7 last paragraph: "**'Refresh' in SP-CAT = re-audit/validation**, NOT an external fetch. What ships is `free_tier_totals(models)` itself — it recomputes the totals with pool-dedup — plus a **test gate**: the `EXAMPLE_*` constants in `crates/gateway/src/catalog/totals.rs` and `example_catalog_totals_match_documented_headline`, which fail the build when `testdata/example_free_catalog.json` and the documented headline drift apart (the `free-tier-catalog.md` 'docs-counts' scenario at unit granularity). **There is no separate `catalog::audit(catalog)` entry point** — the re-audit is `free_tier_totals` + the gate. External catalog fetch/import is SP-DATA."


## Task 2: SP-2 / SP-3 specs — 5 items

- [x] **`docs/superpowers/specs/2026-08-12-sp2-tool-permissions-design.md:147 (also :18, :161, :209, :223)`**
  - *Claims:* §4.5 "Extend `Registry::validate` … require `agent.grants.get(tool)` to exist and `.covers(&tool.permissions)`; otherwise → `OrchestratorError::PermissionNotGranted`"; §1 :18 "`Registry::validate` statically checks that every agent's grant covers its tools' declared needs"; D2 :161 "Absence of a gra
  - *Verified false by:* I re-ran every check and looked for a rescuing reading; none exists. (1) `rg --no-ignore -g '!target' -n 'PermissionNotGranted' --stats crates/ docs/` → 23 matches / 6 files, NOT truncated; the only hit under crates/ is `crates/orchestrator-core/src/error.rs:50` (the enum variant). Zero constructors, zero matches in registry.rs. (2) `rg --no-ignore
  - *Correction:* §1 :18 → "…and `Registry::validate` checks that every agent's skill/tool/chain reference resolves. **(Superseded by SP-4 s1: the load-time grant⊇need check was REMOVED — a grant narrower than a tool's declared surface is legal and is enforced per call at runtime as a ceiling.)**" §4.5 :147-152 → replace the body with: "**SUPERSEDED by SP-4 s1.** `Registry::validate` performs no grant check (`crates/orchestrator-core/src/registry.rs:489-492`, :493-620) and `OrchestratorError::PermissionNotGranted` (`crates/orchestrator-core/src/error.rs:50`) has no constructor in the workspace. Authorization happens per call in `execute_tool_effect` (`crates/orchestrator/src/executor/agent.rs:1008-1029`): deny unless the tool is LISTED in `agent.tools` AND `agent.grants[tool].covers(Tool::required(&args))`;

- [x] **`docs/superpowers/specs/2026-08-14-sp3-coordinator-loops-of-graphs-design.md:187-189 (D6) and :199-200 (§6 Deferred)`**
  - *Claims:* D6: the gate path `"{loop}/{i}/__gate__"` colliding with a `Subgraph` body node named `__gate__` "is a stated authoring constraint, **not enforced this slice**" because "the Loop does not run `feasible` on a `Subgraph` body"; §6 defers "a `feasible`/reserved-id guard on `Subgraph`/`Expand` **loop bo
  - *Verified false by:* The guard exists and is enforced at BOTH intakes today, so the deferral's justification ("nothing checks it") is false. (1) `rg --no-ignore -g '!target' -n 'RESERVED_GATE_ID|RESERVED_SEGMENTS' crates/ --stats` → 41 matches / 7 files, not truncated. (2) I read `crates/orchestrator-core/src/graph.rs:540-598`: block 1c defines `const RESERVED_SEGMENTS
  - *Correction:* D6 :187-189 → "…cannot collide with a body node. **Enforced (later slices):** `Graph::validate_dag` block 1c refuses any node id equal to `__plan__`/`__gate__`/`__select__` and recurses into `LoopBody::Subgraph` (`crates/orchestrator-core/src/graph.rs:573-598`, :894-908), and both `run_inner`/`start_inner` call it before any node runs (`crates/orchestrator/src/executor/mod.rs:980`, `:1039`); `plan::feasible`'s `check_reserved_ids` recurses the same way for an untrusted planner's graph, returning the typed `PlanError::ReservedNodeId` (`crates/orchestrator-core/src/plan.rs:175-198`). It is no longer an authoring constraint." §6 :199-200 → delete "a `feasible`/reserved-id guard on `Subgraph`/`Expand` loop bodies (authoring constraint only this slice)" from the deferred list and record it as S

- [x] **`docs/superpowers/specs/2026-08-12-sp2-activation-policy-design.md:40 (also :131, :151; plans/2026-08-12-sp2-activation-policy.md:7, :464)`**
  - *Claims:* §3 :40 "`over_budget` (same file) **halts loud** when the estimate exceeds the chain's min window — there is no compaction or selection today." §4.4 :131 "…the existing `over_budget` **halt-loud** stands — no silent truncation." D6 :151 "over-budget still halts loud after activation; no silent trunc
  - *Verified false by:* Both halves are false today. (1) `rg --no-ignore -g '!target' -n 'fn over_budget' crates/` → NO matches, real exit code 1 (I echoed `$?`). `rg -n 'over_budget|PromptOverBudget' crates/ --stats` → 33 matches / 9 files, not truncated, and every orchestrator hit is a comment: `crates/orchestrator/src/agent/prompt.rs:575` "`over_budget(min_window, syst
  - *Correction:* §3 :40 → "`over_budget` was DELETED by SP-7a along with `OrchestratorError::PromptOverBudget` (tombstones: `crates/orchestrator/src/agent/prompt.rs:575`, `crates/orchestrator-core/src/error.rs:75-85`). Window fit is now the gateway's `ContextWindowGate`, asked per CANDIDATE rather than against the chain minimum; an over-every-window request surfaces as `GatewayError::AllGated` with each candidate's own window and a remedy. SP-7b then added the context budget: `PromptParts::join_bounded` truncates dependency bodies with a marker and drops whole tool schemas (`prompt.rs:97-152`), disclosed on four channels (per-entry marker + `(N of M dependencies shown)` tail, the `ContextBudgeted` journal record, the `context_budgeted` output key, an operator warn)." §4.4 :131 and D6 :151 → "If the activat

- [x] **`docs/superpowers/specs/2026-08-12-sp2-tool-permissions-design.md:112 and :115 (restated at AC1 :200)`**
  - *Claims:* §4.3: "**paths:** ∀ needed path `p`, ∃ granted path `g` with `p.starts_with(g)`"; "**network:** … `Hosts(G)` covers `Hosts(N)` iff N ⊆ G"; AC1 :200 "paths (prefix covers; non-prefix fails)".
  - *Verified false by:* I read the implementation rather than trusting the report. `crates/orchestrator-core/src/registry.rs:143-150`: `Permissions::covers` delegates paths to `path_covers(g, p)`, not `starts_with`. `path_covers` (registry.rs:193-209) returns false for an EMPTY grant, false when the need contains a `..` segment, and otherwise splits both on `/` and requir
  - *Correction:* §4.3 :112-114 → "**paths:** component-aware prefix — ∀ needed path `p`, ∃ granted path `g` whose `/`-separated segments are a prefix of `p`'s (`path_covers`, `crates/orchestrator-core/src/registry.rs:193-209`). `/workspace` covers `/workspace/src/main.rs` but NOT `/workspace-secret`; an empty grant path covers nothing; a need containing a `..` segment is rejected outright. (Hardened by SP-4 s1 — this was a byte `starts_with` as shipped in SP-2 s3.)" §4.3 :115-116 → "**network:** `Any` covers everything; `Hosts(G)` covers `Hosts(N)` iff every `n ∈ N` is matched by some `g ∈ G` under `host_covers` (`registry.rs:214-222`) — case-insensitive exact match, or a `*.suffix` wildcard grant matching any strict SUBdomain of `suffix` (not the bare domain), so `Hosts(["*.example.com"]) ⊇ Hosts(["api.ex

- [x] **`docs/superpowers/specs/2026-08-11-sp2-role-chain-resolution-design.md:38 (also :149, :232)`**
  - *Claims:* §3 :36-39 `drive_agent` "reads `agent.chain.clone()` straight into `gateway.min_context_window(&chain)`, `agent_input_hash(&chain, …)`, `build_chat_request(&chain, …)`, and the `on_agent_started(…, &chain)` hook"; §4.4 :149 "Everything downstream (`min_win`, `agent_input_hash`, `build_chat_request`,
  - *Verified false by:* Low value, but the named sink is genuinely gone. `rg --no-ignore -g '!target' -n '\.min_context_window\(' crates/ --stats` → 5 matches / 1 file, not truncated, ALL in `crates/gateway/src/engine/mod.rs` unit tests (:843, :844, :877, :990, :999); zero call sites in `crates/orchestrator`. `crates/orchestrator/src/executor/agent.rs:34-37` (the `AgentRu
  - *Correction:* §3 :36-39 → "…reads `agent.chain.clone()` straight into `agent_input_hash(&chain, …)`, `build_chat_request(&chain, …)`, and the `on_agent_started(…, &chain)` hook. (A fourth sink, `gateway.min_context_window(&chain)`, was removed by SP-7a together with the agent path's window pre-check — `AgentRun` no longer carries a `min_win`; see `crates/orchestrator/src/executor/agent.rs:34-37`.)" §4.4 :149 → "Everything downstream (`agent_input_hash`, `build_chat_request`, the `on_agent_started` hook) uses the resolved chain unchanged." AC6 :232 → "observable: a recording gateway that knows ONLY the bound chain-id serves a real turn (`agent_routes_via_area_kind_binding_end_to_end`, `crates/orchestrator/src/executor/tests.rs:337`)" — drop the `min_context_window` mechanism.


## Task 3: SP-4 specs and the Linux-sandbox plan — 8 items

- [x] **`docs/superpowers/plans/2026-08-17-sp4-linux-sandbox.md:255-256,264 (and specs/2026-08-17-sp4-linux-sandbox-design.md:90-91)`**
  - *Claims:* The prescribed `build_landlock_ruleset` carries `/// Best-effort forward-ABI; the ABI-1 write handling is the security core.` and `let abi = ABI::V1;`
  - *Verified false by:* `rg --no-ignore -g '!target' -n 'ABI::V[0-9]' crates/ docs/ --stats` → 6 matches, 3 files, NOT truncated: `crates/orchestrator/src/agent/sandbox.rs:437: let abi = ABI::V5;`, `sandbox.rs:1085-1086` (a regression test whose comment says "Non-vacuous: under an `ABI::V1` pin this fails (the outside file becomes size 0)"), `docs/.../plans/2026-08-17-sp4
  - *Correction:* In docs/superpowers/plans/2026-08-17-sp4-linux-sandbox.md replace lines 255-256 with: "/// remove/make-* + TRUNCATE/REFER/IOCTL_DEV) confined to the canonical workspace subpath, plus a WriteFile+Truncate carve-out for safe pseudo-devices (`/dev/null` &c). Handles the **ABI V5** access set (`CompatLevel::BestEffort` degrades on older kernels) — CRUCIAL: landlock mediates only the rights it HANDLES, so handling only ABI-1 leaves `truncate(2)`/`ftruncate(2)` (the ABI-3 TRUNCATE right) UNMEDIATED — a confined command could zero any writable file OUTSIDE the workspace. Built in the PARENT; `restrict_self` in the child." and line 264 with `let abi = ABI::V5;`. Also note in the sketch that the workspace `PathFd` must be opened explicitly and a failed open must fail the build (do not route it thro

- [x] **`docs/superpowers/specs/2026-08-16-sp4-subprocess-sandbox-design.md:78-79, 201, 242`**
  - *Claims:* "RLIMIT_AS => alloc fails ... All-unix (macOS + Linux)" (§4.1); "cpu/mem/wall kill on all unix" (§4.6 trust boundary); AC2 "**Cap-kill: mem (portable).** ... `killed: Some(Mem)` or a nonzero exit ... a child within the cap succeeds."
  - *Verified false by:* Platform fact re-tested directly on this host, not taken from the report: `python3 -c "...libc.setrlimit(5, byref(rl))..."` → `platform macOS-26.6.2-arm64-arm-64bit-Mach-O` / `setrlimit(RLIMIT_AS) -> -1 errno 22 Invalid argument`. Mechanism re-read at `sed -n '25,130p' crates/orchestrator/src/agent/sandbox.rs`: line 90 `let mem = caps.mem_bytes;`, 
  - *Correction:* §4.1 (line 78-79): "RLIMIT_CPU => kernel SIGXCPU/SIGKILL on cpu overrun. **RLIMIT_AS is effective on Linux only**: Darwin's `setrlimit(RLIMIT_AS)` returns EINVAL, so on macOS a `mem_bytes: Some(_)` cap makes the child's `pre_exec` hook fail and `spawn` return `Err` — the call refuses fail-closed rather than running uncapped. A `None` cap = unlimited. Process group, wall-kill and bounded capture are all-unix." §4.6 (line 201): "cpu/wall kill on all unix; **mem (RLIMIT_AS) is enforced on Linux and fail-closed-refused on macOS**". AC2 (line 242): "**Cap-kill: mem.** A `mem_bytes` cap yields NO clean success — on Linux the applied `RLIMIT_AS` aborts/kills the over-allocating child; on macOS `spawn_capped` refuses at spawn (`Err`), so the cap is never silently dropped. (Shipped as `mem_cap_prev

- [x] **`docs/superpowers/specs/2026-08-15-sp4-credential-broker-design.md:105-106 (and plans/2026-08-15-sp4-credential-broker.md:344-345, 352)`**
  - *Claims:* §4.4's shipped-ordering pseudocode is `self.redact(&result)` (s2 pattern) THEN `scrub_secret_values(&result, ctx.secret_values())` (s4 exact-value); the plan restates "The gate is: redact THEN scrub"
  - *Verified false by:* `rg --no-ignore -g '!target' -n 'scrub_secret_values|self.redact\(' crates/orchestrator/src/executor/*.rs --stats` → 13 matches / 6 files, not truncated; the only `record_tool_effect` sites are `agent.rs:1418` (scrub) and `agent.rs:1422` (redact) — scrub is FIRST. Read `sed -n '1400,1430p' crates/orchestrator/src/executor/agent.rs`: "SP-4 broker: s
  - *Correction:* §4.4 code block (spec lines 104-107) becomes: `let result = tool.call_ctx(args, &ctx)?;` / `let result = scrub_secret_values(&result, &ctx.exposed_secret_values()); // s4 exact-value, THIS call's creds — FIRST` / `let result = self.redact(&result); // then s2 pattern over the residual`, with the sentence: "the exact-value scrub runs **before** the s2 pattern pass — a pattern hit inside a wrapped/composite secret fragments its high-entropy span, after which the whole-value match no longer fires and a prefix such as `wrap-…` survives into the journal." Plan line 352 becomes: "The gate is: **scrub THEN redact**, both before `split_output` and the returned `result`." Plan lines 19/166/174/180/182/195/403/412 should also say `exposed_secret_values()`.

- [x] **`docs/superpowers/specs/2026-08-15-sp4-exactly-once-idempotency-design.md:195-197`**
  - *Claims:* AC4: "`EffectIntent.idempotency_key` is folded into `fold.intents` (teid→key); a matching `EffectRecorded` **removes the entry** (no longer in-doubt), exactly as the old set did."
  - *Verified false by:* `rg --no-ignore -g '!target' -n 'intents' crates/ --stats` → 8 matches / 3 files / 201 files searched, not truncated: the ONLY write is `crates/orchestrator/src/executor/support.rs:138-145` (`JournalEvent::EffectIntent {..} => fold.intents.insert(...)`); the reads are `agent.rs:1095` (`contains_key`) and `agent.rs:1152` (`.get`). `rg --no-ignore -g
  - *Correction:* AC4 becomes: "**Fold `intents` map.** `EffectIntent.idempotency_key` is folded into `fold.intents` (teid→key). **Nothing removes the entry** — exactly as the old `HashSet` never removed either; a completed Mutation stays in `intents` forever. In-doubt-vs-completed is decided solely by the memo-first short-circuit in `execute_tool_effect` (a completed Mutation replays from `fold.memo` before `mutation_tool_effect`'s `intents.contains_key` is ever consulted), so any new reconcile branch MUST sit behind that short-circuit. Existing in-doubt/reconcile tests pass unchanged."

- [x] **`docs/superpowers/specs/2026-08-14-sp4-permission-enforcement-design.md:137-140 (and plans/2026-08-14-sp4-permission-enforcement.md:483-489)`**
  - *Claims:* §4.4: a denied call returns `{"error": "permission_denied", "tool": "<name>", "detail": "path /etc/passwd outside granted paths [/workspace]"}` into the ReAct transcript; the plan prescribes `format!("call needs {:?} which the grant {:?} does not cover", need, grant)` and a `record_denied_effect(...
  - *Verified false by:* The JSON envelope IS shipped verbatim (`record_denied_effect`, crates/orchestrator/src/executor/agent.rs:1243-1247: `"error": "permission_denied", "tool": call.name, "detail": detail`) — so I could only refute the envelope half, not the `detail` half. `rg --no-ignore -g '!target' -n 'is not available to this agent|is not permitted by its grant' cra
  - *Correction:* §4.4 first bullet becomes: "A denied call returns a structured tool-result **error value** — `{\"error\": \"permission_denied\", \"tool\": \"<name>\", \"detail\": \"the requested access for tool 'fs_write' is not permitted by its grant\"}` — placed into the ReAct transcript as that call's result. The `detail` is deliberately **TERSE**: it names the tool and the fact of denial and NOTHING else — never the offending path and never the grant/allowlist, because the denied party is the model and disclosing the allowlist invites a redirect to another granted resource (confused-deputy / prompt-injection surface). The two s1 forms are `tool '<name>' is not available to this agent` (not listed) and `the requested access for tool '<name>' is not permitted by its grant` (listed, grant does not cover)

- [x] **`docs/superpowers/specs/2026-08-14-sp4-secret-redaction-design.md:85-86`**
  - *Claims:* §4.1: "The default set (extensible; `PatternRedactor::new(patterns)` + a `PatternRedactor::default()` with the built-ins)"
  - *Verified false by:* `rg --no-ignore -g '!target' -n 'PatternRedactor::new|PatternRedactor::' crates/ docs/ --stats` → 70 matches / 69 lines / 13 files, not truncated; every single call site in `crates/` is `PatternRedactor::default()` (e.g. crates/torii/src/boot.rs:423, crates/torii/src/render.rs:38, crates/orchestrator/src/executor/mod.rs:776, human.rs:1762), and the
  - *Correction:* §4.1 becomes: "The default set is **fixed at construction**: `PatternRedactor` exposes only `Default` (its `whole`/`whole_set`/`assignment`/`secret_key` fields are private and there is no pattern-taking constructor), so the built-in shapes cannot be extended without editing `orchestrator-core`. The supported extension point is the injected **`Redactor` trait** — a deployment needing site-specific secret shapes supplies its own `impl Redactor` via `Executor::with_redactor`."

- [x] **`docs/superpowers/specs/2026-08-16-sp4-workspace-isolation-design.md:63-67`**
  - *Claims:* §4.1: "At `run`/`start` entry, when a base is wired, the executor resolves the per-run root `base/<run_id>/` and `std::fs::create_dir_all`s it"
  - *Verified false by:* `rg --no-ignore -g '!target' -n 'create_dir_all' crates/orchestrator/src/ --stats` → 9 matches / 4 files, not truncated; the only non-test creation in the executor is inside `workspace_root_for` (crates/orchestrator/src/executor/agent.rs:1280). `rg --no-ignore -g '!target' -n 'workspace_root_for|workspace_root' crates/orchestrator/src/executor/*.rs
  - *Correction:* §4.1 second bullet becomes: "When a base is wired, the executor resolves the **per-run root** `base/<run_id>/` **lazily, on the live tool-effect path** — `Executor::workspace_root_for(run)`, called from the jail pre-check in `execute_tool_effect` and from `record_tool_effect`, not from `run`/`start`. It `std::fs::create_dir_all`s then `canonicalize`s (both idempotent — safe on resume; a memo hit replays without resolving at all). **Consequence:** a run that never makes a live tool call creates no directory, so a cleanup/GC, quota or operator 'show this run's workspace' path must treat `base/<run_id>/` as possibly absent. Durable, no auto-delete (§6)."

- [x] **`docs/superpowers/specs/2026-08-14-sp4-secret-redaction-design.md:87-93`**
  - *Claims:* §4.1's census of the default `PatternRedactor` set: `sk-ant-[A-Za-z0-9_-]{20,}`, `sk-[A-Za-z0-9]{20,}`, `AKIA[0-9A-Z]{16}`, `ghp_[A-Za-z0-9]{36}`, `xox[baprs]-…`, `AIza[0-9A-Za-z_-]{35}`, assignment value class `([^\s"',]{6,})`
  - *Verified false by:* Read crates/orchestrator-core/src/redact.rs:46-73 — the shipped `whole_patterns` array is `sk-[A-Za-z0-9_-]{20,}`, `sk_live_[A-Za-z0-9]{20,}`, `rk_live_[A-Za-z0-9]{20,}`, `AKIA[0-9A-Z]{16}`, `gh[opsru]_[A-Za-z0-9]{30,}`, `github_pat_[A-Za-z0-9_]{22,}`, `xox[baprs]-[A-Za-z0-9-]{10,}`, `AIza[0-9A-Za-z_-]{30,}`, bearer, PEM, URL-userinfo; the assignme
  - *Correction:* §4.1 bullet becomes: "- **Provider key prefixes:** `sk-[A-Za-z0-9_-]{20,}` (OpenAI **and** Anthropic `sk-ant-…` — one pattern covers both), `sk_live_[A-Za-z0-9]{20,}` / `rk_live_[A-Za-z0-9]{20,}` (Stripe), `AKIA[0-9A-Z]{16}` (AWS), `gh[opsru]_[A-Za-z0-9]{30,}` (GitHub classic PAT/OAuth/server/refresh/user), `github_pat_[A-Za-z0-9_]{22,}` (GitHub fine-grained PAT), `xox[baprs]-[A-Za-z0-9-]{10,}` (Slack), `AIza[0-9A-Za-z_-]{30,}` (Google)." and the assignment form's value class becomes `([^\s"',&;]{6,})` (the `&`/`;` exclusions keep a URL query string's following parameters out of the redaction).


## Task 4: SP-DATA-1/2/3 specs — 6 items

- [x] **`docs/superpowers/specs/2026-08-18-sp-data-2-postgres-config-source-design.md:140 (and the code blocks at :142-176, plus AC7 at :242)`**
  - *Claims:* "`ConfigSource` gains one defaulted method so existing impls compile unchanged", with `reload`/`from_source` shown as two separate reads (`source.load().await?` then `source.version().await?`); AC7 says forcing Postgres `version()` to return `None` makes B fall back to the local counter.
  - *Verified false by:* `rg --no-ignore -g '!target' -n 'async fn version|async fn load_versioned|pub async fn reload|pub async fn from_source|trait ConfigSource' crates/orchestrator-core/src/registry.rs` → 277 `pub trait ConfigSource`, 285 `async fn version(..) { Ok(None) }`, **306 `async fn load_versioned(..)` — a SECOND defaulted method**, 672 `pub async fn reload`, 69
  - *Correction:* §6: "`ConfigSource` carries TWO defaulted methods: `version()` (this slice) and `load_versioned()` (added by SP-DATA-4 §6.1 to close this slice's TOCTOU carry-forward), so existing impls still compile unchanged." The `reload`/`from_source` bodies must read `let (cfg, ver) = source.load_versioned().await?;` (registry.rs:673, :692) — the two separate `load()`/`version()` reads shown here ARE the TOCTOU and are now forbidden by a guard test (registry.rs:2069). AC7 becomes: "mutation-check — the durable generation reaches the handle through `load_versioned()`, which `PostgresConfigSource` overrides (postgres.rs:835); forcing its `version()` to `None` no longer changes the handle's generation. Mutate the `config_versions` read INSIDE the override to break the fence."

- [x] **`docs/superpowers/specs/2026-08-18-sp-data-2-postgres-config-source-design.md:205-209`**
  - *Claims:* The backend template `impl ConfigSource for PostgresConfigSource { async fn load(..); async fn version(..); }` — a versioned backend implements `load` + `version` and nothing else.
  - *Verified false by:* `sed -n '280,320p' crates/orchestrator-core/src/registry.rs` → the default `load_versioned` (registry.rs:306) does `let cfg = self.load().await?; let ver = self.version().await?;` then `if ver.is_some() { return Err(OrchestratorError::RegistryLoad("ConfigSource::version() returned Some(_) through the DEFAULT load_versioned() — a versioned source MU
  - *Correction:* The `impl ConfigSource for PostgresConfigSource` block must show three methods: `load`, `version`, AND `async fn load_versioned(&self) -> Result<(RegistryConfig, Option<u64>), OrchestratorError>` overridden with ONE `REPEATABLE READ` transaction spanning the four config tables and `config_versions` (postgres.rs:835). Add: "A versioned source MUST override `load_versioned`; reaching the default with a `Some(_)` version is a hard `RegistryLoad` error (registry.rs:312), not a silent fallback."

- [x] **`docs/superpowers/specs/2026-08-18-sp-data-2-postgres-config-source-design.md:194-203 (and §5, :123-136; §9, :250)`**
  - *Claims:* `store()` "Does NOT bump the version (the caller bumps explicitly after committing a change) … SP-DATA-4's CLI grows granular edits on top", with `pub async fn store(..)` and `pub async fn bump_config_version(..)` presented as the ordinary public write API.
  - *Verified false by:* `rg --no-ignore -g '!target' -n 'pub async fn store_and_bump|pub async fn store\b|pub async fn bump_config_version|cfg\(any\(feature = \"test-support\"' crates/orchestrator-store/src/postgres.rs` → :796 `#[cfg(any(feature = "test-support", test))]` then :797 `pub async fn store`; :806 the same gate then :807 `pub async fn bump_config_version`; the 
  - *Correction:* §7: mark the pair as test-only and name the real API — "`store` / `bump_config_version` are the UN-COUPLED writers, `#[cfg(any(feature = \"test-support\", test))]`-gated (postgres.rs:796, :806) and unreachable from a production build: even a disciplined caller doing `store()` then `bump()` has a crash window that durably leaves new content under an old generation. The production write path is the coupled single-transaction `store_and_bump` (postgres.rs:668) and `store_and_bump_if(cfg, expected_generation)` (postgres.rs:743, a CAS on the generation) — `torii config push` uses `store_and_bump_if` (crates/torii/src/cmd/config.rs:163) and boot seeding uses `store_and_bump` (crates/torii/src/boot.rs:899)." Drop "the caller bumps explicitly after committing a change" and "SP-DATA-4's CLI grows g

- [x] **`docs/superpowers/specs/2026-08-18-sp-data-3-durable-scheduler-design.md:178-179`**
  - *Claims:* "the Scheduler reads it from the durable journal: on a paused drive it `journal.load(run)`s and takes the **last** `RunPaused { resume_after }` event."
  - *Verified false by:* `sed -n '175,235p' crates/orchestrator/src/scheduler.rs` → the deadline read is `async fn earliest_resume_after(&self, run: RunId, since: Seq)` at scheduler.rs:218, whose body is `self.journal.load_since(run, since)` … `.filter_map(|(_, e)| match e { JournalEvent::RunPaused { resume_after, .. } => *resume_after, _ => None }).min()` (scheduler.rs:22
  - *Correction:* "…the Scheduler reads it from the durable journal: it takes a journal watermark BEFORE the drive (`Scheduler::watermark`, scheduler.rs:185) and, on a paused drive, `journal.load_since(run, watermark)`s and takes the EARLIEST non-`None` `RunPaused { resume_after }` journaled by THIS drive (`earliest_resume_after`, scheduler.rs:218). Earliest, not last: one drive can journal several `RunPaused` events, and taking the last (then `flatten()`ing) let a deadline-less pause null out a timed gate's wake. This drive's window, not the whole journal: an earlier drive's already-past deadline would be re-adopted and re-claimed by every `tick()` — a hot loop." (Same fix needed in the SP-DATA-3 plan at :543/:561/:849.)
  - **APPLIED to the spec; the parenthetical is SKIPPED — `docs/superpowers/plans/2026-08-18-sp-data-3-durable-scheduler.md` is in no task's file list**, so no agent owned it and the disjoint-by-file partition forbade reaching into it. The drift is real and still present there: `:7` says "the Scheduler reads the pause deadline from the journal's last `RunPaused` event" and `:560` says "The last journaled `RunPaused.resume_after`". **Needs a follow-up dispatch owning that file.**

- [x] **`docs/superpowers/specs/2026-08-18-sp-data-3-durable-scheduler-design.md:47`**
  - *Claims:* Goals: "Additive: default-off ⇒ byte-identical; the `Executor` unchanged (one tiny additive `PauseInfo` field)."
  - *Verified false by:* `rg --no-ignore -g '!target' -n 'struct PauseInfo' -g '*.rs'` → 1 match, untruncated: crates/orchestrator/src/executor/mod.rs:124, and the body is exactly `pub struct PauseInfo { pub node: NodeId, pub reason: String }` (mod.rs:124-127) — no field was added. Refutation attempts that FAILED: (a) reading it as "a field OF TYPE `PauseInfo`" — that is `
  - *Correction:* "Additive: default-off ⇒ byte-identical; the `Executor` and core are entirely unchanged — no `PauseInfo`/`NodeExec` change at all (the Scheduler reads the pause deadline from the durable journal instead; see §7 and AC9)."

- [x] **`docs/superpowers/specs/2026-08-17-sp-data-1-durable-run-state-design.md:75-76`**
  - *Claims:* §4.1: "**`runs`** (or a column on the first `RunStarted`) — carries the durable **`format_version`** (see §4.3) + `run_id`, `created_at`, terminal status (for later slices' queries)."
  - *Verified false by:* `cat database/ddl/table/orchestrator/runs.sql` → `create table if not exists orchestrator.runs (run_id uuid primary key, format_version integer not null, created_at timestamptz not null default now());` — three columns, no status. `sed -n '50,60p' database/_apply_all.sql` shows the identical three-column definition (so the applied schema is not a s
  - *Correction:* "**`runs`** — `run_id uuid primary key`, `format_version integer not null`, `created_at timestamptz not null default now()` (database/ddl/table/orchestrator/runs.sql). It carries the durable `format_version` (§4.3) and nothing else; there is no status column. Run lifecycle status lives in `orchestrator.scheduled_runs.status`, added by SP-DATA-3."


## Task 5: SP-DATA-4/5 specs — 3 items

- [x] **`docs/superpowers/specs/2026-08-22-sp-data-4-torii-management-cli-design.md:384-397 (§7.3 Shutdown)`**
  - *Claims:* "**There is deliberately no second-signal fast path, and an earlier draft of this spec claimed one.** `shutdown_signal()` yields a one-shot future, so signals arriving during a tick are consumed and discarded: sending SIGTERM twice more, then SIGINT, to a worker blocked mid-`claim_due` leaves it ali
  - *Verified false by:* Searched for a rename/re-export or a #[cfg] scoping that would rescue the claim, and for an in-document amendment. Neither exists. 1) `rg --no-ignore -g '!target' -n 'shutdown_signal|watch::Receiver<u64>|watch::Sender' crates/torii/src/` → 7 hits, not truncated. Both definitions (unix and non-unix) are `fn shutdown_signal() -> Result<tokio::sync::w
  - *Correction:* Replace §7.3's second and third paragraphs with: "**A second signal abandons the in-flight tick** (added in SP-DATA-4.1 Task 4; an earlier draft of this section wrongly recorded the path as absent). `shutdown_signal()` returns a `tokio::sync::watch::Receiver<u64>` — a LEVEL incremented once per received signal, not a one-shot future — which is what lets `serve` tell a first signal from a second. A `Future` fires once by construction and `Notify` coalesces two back-to-back signals into one permit; reading the watch value as a level means even two signals landing before the loop is first polled already read `>= 2`. The FIRST signal is noted and not acted on: the in-flight tick is allowed to finish so a partial drive is not wasted. The SECOND drops the tick future at its next await point (mid

- [x] **`docs/superpowers/specs/2026-09-03-sp-data-5-budget-clamp-design.md:394-401 (§6 Accepted costs)`**
  - *Claims:* "**The pessimism assumes a Latin script.** `chars / 3` over-counts only where a token is worth three or more characters. CJK, Cyrillic and emoji run nearer 1–3 tokens PER character, so on such a prompt the estimate under-counts by a multiple, §4's residual (`actual_input − est_input`) becomes a frac
  - *Verified false by:* Checked for a surviving char-based estimator under another name and for an amendment covering §6. Neither exists. 1) `rg --no-ignore -g '!target' -n 'estimate_input_tokens_pessimistic' crates/` → 20 hits, one definition: crates/gateway/src/engine/util.rs:273 `pub fn estimate_input_tokens_pessimistic(payload: &Payload) -> u32`. Every term is a BYTE 
  - *Correction:* Replace §6:394-401 with: "**The pessimism assumes a script that tokenizes at three or more BYTES per token.** The estimate is `ceil(UTF-8 bytes / 3)` (`gateway::estimate_input_tokens_pessimistic`), so it over-counts only where a token is worth ≥ 3 bytes — Latin-script text, where a byte is a character. It was `chars / 3` when this section was first written, and that under-counted CJK threefold (3 bytes per character, tokenizing near 1 token per character); counting bytes closes most of that, since 3 bytes / 3 lands at ~1 token per CJK character, roughly the truth. Note that `bytes >= chars` only proves the new estimate beats the OLD one, which is not the property that matters: the estimate must be >= the TRUE token count. So the margin on CJK is ~0 rather than negative, and the sign can st

- [x] **`docs/superpowers/specs/2026-08-23-sp-data-5-token-budget-design.md:210-212 (§6.5, the "Amended by the follow-on clamp slice" block)`**
  - *Claims:* "a budgeted `Chat` request now carries `max_tokens = min(remaining − est_input, the chain's smallest max_output_tokens, the caller's own)`, so the provider enforces the bound."
  - *Verified false by:* Checked whether a fourth term is folded into one of the three named terms, and whether the token-budget spec amends this anywhere. Neither. 1) crates/orchestrator/src/executor/dispatch.rs:751-761 computes a TWO-term ceiling: `let out = self.gateway.min_max_output_tokens(chain).await;` `let min_win = self.gateway.min_serving_context_window(chain, es
  - *Correction:* Replace the formula sentence in §6.5's amendment block with: "…a budgeted `Chat` request now carries `max_tokens = min(remaining − est_input, the chain's smallest `max_output_tokens`, `min_serving_context_window(chain, est_input) − est_input`, the caller's own)`, so the provider enforces the bound. The third term is a WINDOW bound — a provider enforces `prompt + max_tokens <= context_window` too — and it is the smallest window that can actually SERVE the request (the fold over `{ m : m.context_window >= est }`, exactly the set `ContextWindowGate` admits), not the chain-wide minimum; an empty serving set contributes no term at all, because the gate owns that refusal. And the gate is no longer the only refusal, in TWO ways rather than one: `BelowFloor { window: None }` when the budget allowa


## Task 6: SP-6 specs — 3 items

- [x] **`docs/superpowers/specs/2026-08-27-sp-6-s2-human-gate-design.md:318-329`**
  - *Claims:* "AC12 is dev/CI-gated, and the mechanism is WEAKER than `#[ignore]` ... There is no `#[ignore]` anywhere in `crates/torii` (`rg '#\[ignore' crates/torii` returns nothing), and there could not be: `#[ignore]` is a compile-time attribute and cannot be conditioned on an environment variable. ... So AC1
  - *Verified false by:* I attacked this three ways and could not save it. (1) Ran the spec's own command verbatim: `rg --no-ignore -g '!target' -n '#\[ignore' crates/torii; echo EXIT=$?` → EXIT=0 with 5 hits in 2 files (crates/torii/build.rs:2,11,13 and crates/torii/tests/e2e_pg.rs:1761,1762). It does not return nothing. (2) The mechanism the doc calls impossible ships in
  - *Correction:* Replace lines 318-329 with: "**AC12 is dev/CI-gated, and since `171ccf5` (2026-08-28) the gate is a CONDITIONAL `#[ignore]`, not a runtime early return.** It requires Docker Postgres. `crates/torii/build.rs` emits `cargo::rustc-cfg=have_database_url` when `DATABASE_URL` is set and non-blank, and every test in `crates/torii/tests/e2e_pg.rs` — AC12's `a_human_gate_decided_in_another_process_completes_the_run` among them — carries `#[cfg_attr(not(have_database_url), ignore = \"needs a Postgres at $DATABASE_URL; see README, Postgres-backed tests\")]`. So with no database AC12 is reported **ignored, not passed**: `env -u DATABASE_URL cargo test -p sensei-torii --test e2e_pg` prints `0 passed; 0 failed; 8 ignored`. A build-time cfg is the mechanism because both static alternatives break the comm

- [x] **`docs/superpowers/specs/2026-08-27-sp-6-s3-human-as-agent-design.md:476-479`**
  - *Claims:* "**AC13 is `DATABASE_URL`-gated.** It returns early without one and is therefore **counted as passed while having exercised nothing**; the raw-stderr `SKIP` line is the only signal. Stated because s2's spec claimed the test was `#[ignore]`d, which was false — `#[ignore]` cannot be conditioned on an 
  - *Verified false by:* Same three checks, aimed at AC13's own test. (1) AC13's test carries the conditional ignore: crates/torii/tests/e2e_pg.rs:1764-1769 → `#[cfg_attr(not(have_database_url), ignore = "needs a Postgres at $DATABASE_URL; see README, Postgres-backed tests")]` then `#[tokio::test]` then `async fn a_human_backed_agent_answered_in_another_process_completes_t
  - *Correction:* Replace lines 476-479 with: "**AC13 is `DATABASE_URL`-gated, and since `171ccf5` the gate is a CONDITIONAL `#[ignore]`, not a silent early return.** `crates/torii/build.rs` turns a set, non-blank `DATABASE_URL` into `cfg(have_database_url)`, and `a_human_backed_agent_answered_in_another_process_completes_the_run` carries `#[cfg_attr(not(have_database_url), ignore = \"needs a Postgres at $DATABASE_URL; see README, Postgres-backed tests\")]`, so with no database it is reported **ignored** — `env -u DATABASE_URL cargo test -p sensei-torii --test e2e_pg` gives `0 passed; 0 failed; 8 ignored`, and the ignored count, not a raw-stderr line, is the signal. The `let Some(url) = db_url() else { return };` line survives only as a second layer for the variable-at-build-time-gone-at-run-time case. **Re

- [x] **`docs/superpowers/specs/2026-08-24-sp-6-s1-await-signal-design.md:69`**
  - *Claims:* §5 Architecture: "`executor Fold gains signals: HashMap<NodeId, serde_json::Value>` / `deadlines: HashMap<NodeId, DateTime<Utc>>`" (and §9 line 211: "`Fold` gains two maps").
  - *Verified false by:* The `deadlines` type is wrong; the "two maps" half is defensible and I am NOT confirming it. Type: `rg --no-ignore -g '!target' -n 'deadlines|signal_asks|signals:' crates/orchestrator/src/executor/mod.rs` → line 180 `signals: HashMap<NodeId, serde_json::Value>,` (spec correct) and line 214 `deadlines: HashMap<NodeId, Option<chrono::DateTime<chrono:
  - *Correction:* Line 69: replace `deadlines: HashMap<NodeId, DateTime<Utc>>` with `deadlines: HashMap<NodeId, Option<DateTime<Utc>>>`, and add the line below it inside the code block or immediately after it: "the value is itself an `Option` and both layers are load-bearing — *key absent* = this node has never begun waiting, `Some(None)` = it began waiting with NO deadline. Folded FIRST-wins **including the `None`** (review fix `5c57726`): that is what makes the deadline-less arm node-keyed idempotent rather than deadline-keyed, and it is why `Fold::deadline_for` returns `Option<Option<DateTime<Utc>>>`. Without it `run_await_signal` re-journals `SignalAwaited` on every drive, and a re-drive is not human-bounded — a dep-free sibling that pauses WITH a deadline in the same round keeps the whole run auto-wake

---

## Final verification

- [x] `cargo test --workspace` still **1760 passed / 0 failed**, real exit code checked unpiped.
  Run unpiped to a log with `echo $?` appended: `REAL_EXIT=0`; summed `test result:` lines =
  1760 passed / 0 failed / 56 ignored. Baseline UNCHANGED, as a docs-only pass requires.
- [x] `cargo clippy --workspace --all-targets -- -D warnings` and `cargo fmt --all --check` exit 0.
  `CLIPPY_EXIT=0`, `FMT_EXIT=0`.
- [x] `git diff --stat` shows **docs/ only** — a code file in the diff means an agent overreached.
  27 files, all under `docs/superpowers/`, +679/−15. `git diff --name-only | grep -v '^docs/'`
  exits 1 (no match). Nothing under `crates/` or `database/`.
- [x] Every amendment quotes the original text it supersedes.
  Verified two ways rather than by eye: (a) the pass is insertion-dominant — only **13** lines
  were deleted at all, and each is a line that was *extended* with an inline correction; a script
  diffing every deleted line against the added lines in the same file confirmed the original
  wording survives verbatim in all 13 (6 are mid-line insertions, checked by hand — the original
  words are preserved on both sides of the inserted clause). (b) Three amendments were re-derived
  from the code end-to-end (see the close-out report).

### Coverage gaps found during the pass — CLOSED by hand after the fan-out

These two files carry the same defect class this plan exists to remove, but **neither appeared in
any task's file list**, so the disjoint-by-file partition meant no agent owned them. Both were
verified against the code and amended directly, rather than left for a follow-up:

- [x] **`docs/superpowers/plans/2026-08-18-sp-data-3-durable-scheduler.md:7` and `:560`** — "the
  journal's **last** `RunPaused` event" / "The **last** journaled `RunPaused.resume_after`".
  Refuted by `Scheduler::earliest_resume_after` (`crates/orchestrator/src/scheduler.rs:218`), which
  takes `.min()` over `load_since(run, watermark)`. This is the parenthetical of the Task-4 item at
  `specs/…-sp-data-3-durable-scheduler-design.md:178-179`, which WAS fixed in the spec.
- [x] **`docs/features/orchestrator/durable-executor.md:170-172`** — "`AllGated { resume_after:
  None }` (all gates terminal) and every other gateway error **fail-fast** … never a pause-forever".
  Refuted by the second pause arm of `classify_gateway_error`
  (`crates/orchestrator/src/executor/support.rs:696-703`): `AllGated { resume_after: None,
  human_action: Some(_) }` ⇒ `Pause { resume_after: None }`. This is the M1 reversal, whose
  *plan*-side annotation the Task-1 item covered; the feature doc was never in scope. Note that
  **every one of this plan's 31 items names a file under `docs/superpowers/`** — so whatever the
  audit's own reach was, `docs/features/` received no coverage here and should get its own pass.

