# torii

> **This crate is moving to the Torii product repo** — epic
> [sensei-hq/gateway#76](https://github.com/sensei-hq/gateway/issues/76), step TM-8
> ([sensei-hq/torii#26](https://github.com/sensei-hq/torii/issues/26)). Torii `docs/DECISIONS.md` §11:
> the gateway is a library; torii owns persistence. Until the move lands, everything below holds.

The operator control plane for the sensei orchestrator: submit and observe runs, intervene on the
ones waiting for a human, drive due wakes, and manage the durable registry config.

Everything below was verified against the built binary and the source, not from memory. Where the
toolkit does not yet do something, it says so rather than describing an intention.

## What you need first

| | |
|---|---|
| **A Postgres** | The orchestrator's journal, CAS, context, scheduler and config all live there. |
| **The schema** | `psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f database/_apply_all.sql` — idempotent (`create … if not exists`), so it is safe to re-run. |
| **`DATABASE_URL`** | Environment only. There is deliberately no flag: a flag would leak the password into `ps`. |
| **`TORII_FENCE_VERSION`** | Needed by `run submit` and `worker serve`. Set it **explicitly** (e.g. `v1`) and keep a fleet agreed on it — it is recorded in every run and checked on resume, so deriving it from a build version would strand every paused run on a routine deploy. |
| **`TORII_POOL_SIZE`** | Optional. Defaults are fine to start. |
| **`TORII_BACKEND`** | Optional: `postgres` (the default — everything above applies) or `memory`. `memory` keeps every store in the process — no database, no `DATABASE_URL` — for development and CI. Nothing survives the process, so a run it submits can only be observed or woken by that same process. |
| **`TORII_REGISTRY_DIR`** | With `TORII_BACKEND=memory`: the registry directory (the `agents/ skills/ tools/` layout `config push` reads) loaded at boot, since there is no database to push to. |
| **A gateway config** | `--gateway-config <file>`, JSON. Needed by `run submit` and `worker serve`. |

## The gateway config

`GatewayConfig` (`kernel::types::config`) is JSON with `routers`, `models`, `chains`, plus optional
`constraints`, `panels` and consensus workflows. The minimum that boots:

```json
{ "routers": { "ollama": { "url": "http://127.0.0.1:11434" } } }
```

`ollama` is convenient for a first boot because it registers without credentials. A real
deployment adds `models` and named `chains`.

> **Know this before you author agents.** An agent's `chain`, its per-phase `chains`, and every
> `(area, kind)` chain binding are **strings** resolved against *this* file's `chains` map. Two
> checks keep them in step: `torii config push --gateway-config <file>` refuses a registry that
> names a chain the file lacks (opt-in, at push time), and `run submit` / `worker serve` **always**
> refuse to boot on one — naming each missing chain and the agent, phase or binding that names it
> — rather than letting every run that reaches it fail with no candidates.

## The registry directory

`torii config push <dir>` reads exactly three subdirectories:

```
<dir>/agents/*.md     # frontmatter: name, area, kind, chain | chains, tools, skills,
                      #              backed_by, timeout, default_planner
                      # body = the agent's system_prompt
<dir>/skills/*.md     # frontmatter: name, description, activate_on: [kw, ...]
                      # body = the skill text composed into the prompt
<dir>/tools/*.json    # a ToolSpec: the model-facing schema + effect class
```

Flat globs — a `skills/foo/SKILL.md` one level deeper is **not** read.

`activate_on` as a list makes a skill conditional (`OnKeywords`); absent means always. A *scalar*
`activate_on` is a loud parse error rather than a silent "always", so a forgotten pair of brackets
cannot quietly disable the gate.

`backed_by: human` makes the agent a human role rather than a model one, with an optional
`timeout: 48h` SLA; a `timeout` without it is a loud parse error, because a model-backed agent has
no SLA to wait on. `default_planner: true` designates **the** `area: planning` agent that a
`PlannerRef::Select` node picks, instead of letting the alphabetically-first name win — at most one
agent may carry it, a second is refused at load, and it is refused outside `area: planning`, where
it would designate nothing. Both keys are read literally: `default_planner: yes` is a loud parse
error, never a silent "unmarked".

Per-tool `grants` are **not** agent frontmatter. They live in the registry root as
`<dir>/grants.json` (`{"<agent>": {"<tool>": <permissions>}}`), beside the optional
`<dir>/chains.json` of `(area, kind) → chain` bindings.

A shipped `tools/*.json` declares a schema the model may call. The executable side must exist too —
`torii` wires `fs_read`, `fs_write` and `shell`. A schema with no executable counterpart is a tool
the model can call and the runtime cannot serve.

## The flow

```sh
# 1. schema (idempotent)
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f database/_apply_all.sql

# 2. push your registry — replace-all, and it advances the config generation
torii config push ./registry
torii config version

# 3. run something
torii run submit --graph ./graph.json --gateway-config ./gateway.json

# 4. or serve wakes continuously
torii worker serve --gateway-config ./gateway.json
```

**`config push` is replace-all.** What is in the directory becomes the durable config; anything
absent is removed. It asks for confirmation before removing entities, and before advancing the
generation while runs are paused — `--yes` bypasses both, which is what a CI push needs, since the
interactive prompt refuses on EOF.

**Any successful push terminally kills every already-journaled paused run**, because the generation
it advances is part of the fence those runs resume against. `torii run list-paused` before pushing.

## Observing and intervening

```sh
torii run status <id>            # one run's schedule record
torii run list-paused            # everything awaiting a wake, and nodes awaiting a signal
torii run signal <...>           # deliver a decision to an AwaitSignal node
torii run gate <...>             # decide a HumanGate or a Loop's human gate
torii run agent <...>            # answer a human-backed Agent — a role a person fills
torii run wake <id>              # queue a paused run for the next worker tick
torii run cancel <id>            # cancel a non-terminal run so it is never woken
torii run prune --older-than <>  # delete terminal run records
```

Exit codes: `0` ok · `1` error, including a run that executed and failed · `2` not-found,
precondition-not-met, or a result printable but not the unqualified success you asked for. Exit 1
writes to stderr and nothing to stdout; exit 2 still prints its result. Note `2` is also clap's
usage-error code, so a script keying off it should check stderr too.

Logs go to **stderr** via `RUST_LOG` (default `info`), never stdout, so `--json` output stays clean.

## Known gaps

Stated because finding them by experiment is worse.

- **No registry content ships.** There are no built-in agents, skills or tools — you author all of
  them. `torii worker serve` refuses to boot against a registry with zero agents.
- **No `config init`, `config pull` or `config diff`.** `version` and `push` are the whole config
  surface today.
- **With nobody marked, two planner agents resolve by name order.** Mark one `area: planning`
  agent `default_planner: true` and it is chosen; leave every agent unmarked and the selector
  still takes the first by name.
- **No release artifact.** There is no published crate or container, so `torii` is built from a
  checkout.

For the design record behind these, see `docs/analysis/2026-09-14-sp-reg-1-registry-content.md`.
