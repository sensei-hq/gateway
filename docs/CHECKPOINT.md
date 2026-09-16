# Checkpoint

**Slice: SP-REG-3 — designate the planner with `default_planner`. Build DONE (`e86ced1`);
whole-slice review DONE, fixes landed. Branch `feat/sp-reg-3-default-planner`, 2 ahead of
the build commit, NOT pushed, no PR.** No spec/plan file for this slice yet.

## Done

`default_planner: true` on an `area: planning` agent makes `PlannerRef::Select` pick it over
the alphabetically-first name. The marker is read in `Executor::planner_candidates` from the
PINNED registry (inside the config fence), not snapshotted at boot.

Review: 11 raw findings from 4 lenses → 6 distinct. All 6 verified against the code, none
refuted. 3 MEDIUM fixed, 3 LOW listed below.

`df4e21a` test strength — both mutants had survived everything else. (1) `marked.len() > 1`
was exercised at 0/1/3, never at 2; `> 2` kept the workspace green (1828/0) while an
exactly-two config loads silently. (2) No test combined a marked agent with
`with_registry_handle`, so a boot-snapshot `planner_candidates` left 466 green — the new
reload test reddens it (`left "beta", right "alpha"`). No production code changed.

`5883cdb` doc truth, four surfaces. `default_planner` was in NO markdown in the repo and the
torii README still said "nothing can yet designate a default". Also DROPPED `grants` from the
frontmatter key list — `from_frontmatter` hardcodes an empty map, so authoring it did nothing
(grants live in `grants.json`). Guarded by two serde-field-scrape tests in `cmd::config`.

## Verified

`cargo test --workspace` 1831 / 0, real exit 0 (baseline 1828; ignored 7 both) · clippy
`-D warnings` exit 0 · `fmt --all --check` exit 0. `DATABASE_URL` set, PG live on 5432.

## Next

1. develop→main path: this branch is off `origin/main`; merge/PR is the caller's call.
2. Optionally close the 3 LOW findings before that PR.

## Open

LOW, unfixed by instruction: (1) the `validate` comment claims it "covers the `with_agent`
builder path" — it does not; `from_config` is `validate`'s only non-test caller. (2) The name
tie-break in `planner_candidates` is only probabilistically guarded — dropping it escaped its
two ordering tests 6 of 24 runs. (3) The `default_planner`-outside-`planning` check returns on
the first offender in `HashMap` order, so 2+ offenders name an arbitrary one, varying per load.
Untracked `crates/torii/docs/` appeared mid-session; not this slice's, left alone.
**Sensei daemon NOT running; this file is the record.**
