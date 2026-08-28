# Checkpoint

**Slice:** SP-6 s3 `human-as-Agent`, the LAST slice of SP-6, on
`feat/sp-6-s3-human-as-agent` (from `develop` at `771af25`). **All 7 tasks done; the
whole-slice review's 16 findings are ALL fixed on top (`576bc1f`).**

## Done / remaining

- **Tasks 1–7 ✅** (`fa070dd` … `f482d05`); **✅ whole-slice review fixes, red-first:**
  - `d2e8145` `parse_fm_duration` is loud, never panics — byte-index `split_at` on `48ℏ`,
    chrono's infallible `days` on `999999999999999d`; + the `parse_backing` empty/list
    `timeout:` guard (mutation-verified both arms).
  - `18d22e2` §5.5's top-level rule is a POSITION, not a caller — `drive`/`run_node` carry
    `nested`, cleared by `drive_nested`; a one-node `Subgraph` used to bypass it entirely.
    + the missing `run_consolidate` `MapBody::Agent` row (mutation-verified).
  - `9b12470` a kind-swapped Agent node fails loudly instead of hanging unanswerably —
    keyed on `Fold::prompt_for`, the mirror of `run_human_gate`'s missing-menu arm.
  - `02d6794` `AgentAnswered.actor` redacted before the durable write; `one_line(actor)`
    now guarded (mutation-verified).
  - `72fa55d` the question's two halves get two bounds: AUTHORED fails loudly at
    `MAX_HUMAN_TEXT_BYTES`, `## Context` truncates at the new `MAX_HUMAN_CONTEXT_BYTES`
    (32 KiB); + the prompt is redacted before the append.
  - `4423c83` + `576bc1f` doc/spec conformance (README, execution-graph, overview log,
    spec §5.2/§5.4/§5.5/§6/AC15/AC17).
- **⬜ Remaining:** a RE-review of these fixes (s2's found three HIGH defects introduced
  while fixing), then merge to `develop`.

## Next command — re-review the review fixes, then merge to `develop`

## Open questions

- Covered by nothing: a human-backed `AgentDefinition` through a live `PostgresConfigSource`.
- The e2e suite is `DATABASE_URL`-gated and **passes while exercising nothing** without it.

## Known-broken

None. **1590 passed / 0 failed / 7 ignored, exit 0** (`env -u DATABASE_URL cargo test
--workspace`, +7 over the 1583 baseline); clippy `-D warnings` and `fmt --check` exit 0.
`$DATABASE_URL` is REMOTE Supabase — never run the DB suite against it.
