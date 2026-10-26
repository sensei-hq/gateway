Analysis: rebuilding the proxy in Rust, capability-mapped to local Ollama

  1. What the current Python proxy actually is

  The whole thing is ~300 lines across main.py + llm.py + utils.py. It is a thin FastAPI wrapper over litellm, and that's the key realization:

  - main.py — FastAPI app with /chat/completions, /models, /models/available, /health, /key/new, /key/cost. Auth is a master-key gate + per-user keys backed by
    litellm.BudgetManager. Streaming via a generator that re-json.dumpses each litellm chunk.
  - llm.py — completion() does a DEFAULT_MODEL override, a budget check, then delegates everything to litellm.completion(**kwargs), wrapped in backoff retry
    decorators.
  - utils.py — env loading.

  There is almost no "model mapping" here to port. The mapping the user is describing — translate a client's model name to a suitable local model — is not
  implemented in this proxy. It's implicitly delegated to litellm's own model_map/provider routing. litellm's model_list() is even buggy (the dict literal at
  main.py:97-103 is a dangling expression, never assigned to data).

  So the task is not "port this proxy." It is: build the capability- and hardware-aware routing this proxy never had, and lean on the gateway library for everything
  around it. Roughly 80% new code, ~20% porting.

  2. What already exists to reuse (the big finding)

  The ~/Developer/gateway workspace already implements the hard parts. It's a library, no HTTP server or binary (Facade/Gateway are the entrypoints), which is
  exactly the split the user wants:

  ┌─────────────────────────────────────────────────────────────────────────────┬─────────────────────────────────────────┬─────────────────────────────────────┐
  │                                    Need                                     │             Already exists              │              Location               │
  ├────────────────────────────────────────────────────────────────────────────┼──────────────────────────────────────────┼─────────────────────────────────────┤
  │ Local Ollama as an adapter (OpenAI-compat wire: chat/embed/decide)         │ OllamaAdapter                            │ cloud-providers/src/ollama.rs       │
  ├────────────────────────────────────────────────────────────────────────────┼──────────────────────────────────────────┼─────────────────────────────────────┤
  │ Read local model catalog from ~/.ollama/models                             │ OllamaResolver (also                     │ local-engine/src/registry/          │
  │                                                                            │ Managed/External/ChainedResolver)        │                                     │
  ├────────────────────────────────────────────────────────────────────────────┼──────────────────────────────────────────┼─────────────────────────────────────┤
  │ RAM/disk fit guard (need_ram = size×1.2, need_disk = size×1.05, via        │ evaluate_fit / machine_resources         │ local-engine/src/registry/pull.rs   │
  │ sysinfo) + FitReport with actionable reason                                │                                          │                                     │
  ├────────────────────────────────────────────────────────────────────────────┼──────────────────────────────────────────┼─────────────────────────────────────┤
  │ Cold-boot/provision a local model on demand                                │ ProvisioningSupervisor / ProvisionHandle │ local-engine/src/supervisor.rs      │
  │                                                                            │  / ProvisionPlan                         │                                     │
  ├────────────────────────────────────────────────────────────────────────────┼──────────────────────────────────────────┼─────────────────────────────────────┤
  │ Capability vocabulary (TextChat, TextComplete, TextEmbed, ImageAnalyze,    │                                          │ kernel/src/types/capability.rs,     │
  │ Decision…) + Locality (Local/Cloud) + ModelConfig (capabilities, family,   │ kernel types                             │ config.rs                           │
  │ context_window, pricing, catalog)                                          │                                          │                                     │
  ├────────────────────────────────────────────────────────────────────────────┼──────────────────────────────────────────┼─────────────────────────────────────┤
  │ Capability-aware tiering — derive a tier's members by                      │ TierConfig / TierPredicate /             │ gateway/src/catalog/tiers.rs        │
  │ capability/locality/tags/cost-band                                         │ tier_members                             │                                     │
  ├────────────────────────────────────────────────────────────────────────────┼──────────────────────────────────────────┼─────────────────────────────────────┤
  │ Distribute work by intent — Fastest / Balanced / Best / Specific(name)     │ ModelHint / Purpose                      │ gateway/src/purpose.rs              │
  ├────────────────────────────────────────────────────────────────────────────┼──────────────────────────────────────────┼─────────────────────────────────────┤
  │ Routing engine: fallback chains, circuit breaker, budget/quota gates,      │ Gateway/Facade, GatewayConfig, gates/*   │ gateway/src/                        │
  │ context-window gate, resilience/retry, panels/consensus                    │                                          │                                     │
  ├────────────────────────────────────────────────────────────────────────────┼──────────────────────────────────────────┼─────────────────────────────────────┤
  │ Provider/selector preferences (sort/only/ignore/order)                     │ RoutingPreferences / CandidateRef        │ kernel/src/types/request.rs         │
  ├────────────────────────────────────────────────────────────────────────────┼──────────────────────────────────────────┼─────────────────────────────────────┤
  │ Execute entrypoints                                                        │ Gateway::execute / execute_stream /      │ gateway/src/engine/                 │
  │                                                                            │ execute_panel_addressed                  │                                     │
  └────────────────────────────────────────────────────────────────────────────┴──────────────────────────────────────────┴─────────────────────────────────────┘

  Conclusion: the Rust proxy is genuinely a light binary — an HTTP front-end plus a config generator — because routing, budgeting, fit, and resilience are all in
  gateway. This confirms the user's instinct that budgeting/controls should live in gateway, not the proxy.

  3. The pieces you actually have to build

  Four modules, most of it small:

  3a. Provider model vocabulary — "the provider map"

  The mapping target is a tier/intent, not a literal model. Two cases:

  - Fixed providers (Claude/Anthropic): the client sends a closed set of names — claude-haiku, claude-sonnet, claude-opus, claude-fable (+ dated versions). Author a
    canonical name → tier table. This is "what Claude supports."
  - Open/custom providers ("open code"): arbitrary model ids. No fixed set — resolve by heuristic (name → family/size → tier) or a default.

  The map is a set of ModelConfig entries where id = the name the client sends (e.g. claude-sonnet-4-5) and it routes through a Local-first → Cloud-fallback chain.
  So Claude's "distribute work across models" (haiku→fast, sonnet→balanced, opus→best, fable→…) becomes which chain each name points at.

  3b. Capability-aware tier → local-model resolution

  This is the genuinely novel bit, and it's where "don't map a low-capability model to opus" lives:

  - Each local model gets a capability profile derived from Ollama's own metadata — I verified this machine's /api/tags already exposes capabilities,
    parameter_size, quantization_level, context_length, families, runner. E.g. qwen3-coder:30b = 30.5B, Q4, 262k ctx, [completion,tools]; nimble = 9B, [decision];
    gemma2:2b = 2.6B.
  - Map each tier to the best-fitting local model that (a) has the required capability, (b) clears a capability floor for that tier, (c) fits RAM/disk (reuse
    evaluate_fit).
  - The floor is the guardrail: opus-tier requires the highest-capable model that fits; haiku-tier may use a small one. On this box, opus-tier →
    qwen3-coder:30b/nemotron-3.5 (fits, 30–33B), balanced → gemma4/nimble (9B), fastest → llama3.2 (3.2B). No tier is allowed to resolve to a model below its floor
    — that's how a weak model never quietly answers an opus request.
  - When no local model clears a tier's floor, the chain falls back to Cloud (real Anthropic API) — this is where Locality + fallback chains do the work for free.

  3c. Install-time provisioning — populates ~/.sensei

  An install / doctor subcommand that generates the config from this machine:
  1. read machine via sysinfo (reuse machine_resources),
  2. read the local catalog (ollama /api/tags + /api/show, or OllamaResolver walking ~/.ollama/models),
  3. evaluate_fit + derive a capability profile per candidate coding model,
  4. assemble the tier→model table respecting floors + fit,
  5. write ~/.sensei/config.json (a GatewayConfig: routers[ollama], models[] with catalog.locality=Local + the client-name aliases, per-capability chains) plus the
     provider-name map and a FitReport.

  It's essentially check_fit + a new "plan the local routing" pass. ~/.sensei/ already exists (config.json, models/, rules.md), so the config location is settled.

  3d. The HTTP front-end (the actual proxy)

  - OpenAI-compatible /v1/chat/completions + /v1/models + /v1/embeddings + /health, SSE streaming (Claude Code streams — this is mandatory).
  - Per request: parse model → look up name→tier → resolve to local model (or cloud fallback) → build InferenceRequest → gateway.execute_stream → adapt chunks back
    to OpenAI SSE.
  - Keep the master-key auth gate (it's already in the Python proxy); budget/controls come from the gateway's gates. Hot-reload ~/.sensei/config.json.

  4. Config shape in ~/.sensei/config.json

  Reuse GatewayConfig directly (the gateway is built to deserialize it):

  {
    "routers": { "ollama": { "url": "http://127.0.0.1:11434", "enabled": true } },
    "models": {
      "qwen3-coder:30b": { "id":"qwen3-coder:30b", "provider":"ollama",
        "capabilities":["text_chat","text_complete"], "family":"qwen",
        "context_window":262144, "max_output_tokens":32768,
        "catalog": { "locality":"local", "tags":["coding","reasoning","heavy"] } },
      "claude-opus-4-1":   { "id":"claude-opus-4-1", "provider":"anthropic",
        "api_model_id":"claude-opus-4-1", "capabilities":["text_chat"],
        "catalog": { "locality":"cloud" } }
    },
    "chains": {
      "opus-tier":   { "id":"opus-tier",   "capability":"text_chat",
        "models":[ {"model":"qwen3-coder:30b","router":"ollama","priority":1},
                   {"model":"claude-opus-4-1","router":"anthropic","priority":2} ],
        "fallback_triggers":["timeout","provider_error","model_unavailable"] }
    },
    "constraints": { /* quota/budget — from gateway, kept minimal */ }
  }

  The client-facing names (claude-opus-4-1, etc.) are aliases whose chain is Local-first, Cloud-fallback, and the opus-tier chain only contains models that clear
  the opus floor.

  5. Phased build & effort (rough)

  ┌─────────────────────┬────────────────────────────────────────────────────────────────────┬───────────────────────────────────────────┬────────────┬────────┐
  │        Phase        │                            Deliverable                             │                   Reuse                   │  New LOC   │ Effort │
  ├─────────────────────┼────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────┼────────────┼────────┤
  │ 0. Scaffold         │ new crate, path-dep on gateway+local-engine, cargo build green     │ all                                       │ ~50        │ 0.5d   │
  ├─────────────────────┼────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────┼────────────┼────────┤
  │ 1. Provider map     │ name→tier table (Claude fixed set + heuristic fallback) as         │ ModelHint, Capability                     │ ~200 +     │ 1d     │
  │                     │ ModelConfig aliases                                                │                                           │ tests      │        │
  ├─────────────────────┼────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────┼────────────┼────────┤
  │ 2. Capability       │ derive profile from ollama /api/tags+/api/show; capability floors  │ Capability, TierPredicate                 │ ~250 +     │ 1.5d   │
  │ profiles            │ per tier                                                           │                                           │ tests      │        │
  ├─────────────────────┼────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────┼────────────┼────────┤
  │ 3. Fit/plan         │ install/doctor: sysinfo + catalog → tier table → write             │ evaluate_fit, machine_resources,          │ ~300 +     │ 2d     │
  │                     │ ~/.sensei/config.json                                              │ OllamaResolver                            │ tests      │        │
  ├─────────────────────┼────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────┼────────────┼────────┤
  │ 4. HTTP front-end   │ OpenAI-compat /v1/* + SSE, master-key auth, hot-reload             │ Gateway::execute_stream                   │ ~350 +     │ 2.5d   │
  │                     │                                                                    │                                           │ tests      │        │
  ├─────────────────────┼────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────┼────────────┼────────┤
  │ 5. Wire-in          │ point Claude Code / open-code at it; e2e against local ollama      │ —                                         │ ~150       │ 1d     │
  └─────────────────────┴────────────────────────────────────────────────────────────────────┴───────────────────────────────────────────┴────────────┴────────┘

  Total ≈ 8–9 person-days to a working, tested MVP, because the routing/budget/fit/resilience substrate is already built. The risky/uncertain part is 3b (capability
  floors + the profile-derivation heuristic) — that's where the real judgment lives and where I'd want a test suite, not the plumbing.

  6. Decisions that are genuinely yours

  Before I write a design doc or plan, four forks materially branch the implementation:

⏺ User answered Claude's questions:
  ⎿  · Where should the Rust proxy live, given gateway is a library with no HTTP server? → New crate in gateway workspace
     · Which wire protocol should the front-end speak to clients (Claude Code, open-code, etc.)? → Anthropic-native /v1/messages
     · How should the capability-floor / tier table be authored? → Curated + auto-derive
     · What's the first slice to build and verify? → Install/provision + map only
