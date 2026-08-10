---
title: Config Versioning
doctype: feature
module: catalog
status: planned
phase: 4
spec: SP-DATA
source: data-tier (torii config_versions)
---

# Config Versioning

> **Status: Planned (Phase 4 · SP-DATA).** Reuses torii's `config_versions` + `bump_config_version`.

A monotonically-increasing config version stamped whenever the catalog changes.
Consumers pin the version they assembled from; the orchestrator's durable replay
uses it as the **version-fence** (a config change must not silently mix new
config with memoized effects — see the design's determinism rule).

## Scenarios

```gherkin
Feature: Config versioning
  Scenario: A catalog change bumps the version
    Given config version is N
    When a model/chain/tier is changed and committed
    Then bump_config_version yields N+1

  Scenario: Replay refuses to resume across a version change
    Given a run journaled under config version N
    And the current config version is N+1
    When the run is resumed
    Then it halts with a determinism-violation diagnostic (no silent memoize)

  Scenario: Same version resumes cleanly
    Given a run journaled under version N and current version N
    Then resume proceeds and memoized effects are honored
```

## Notes

- The version-fence is the catalog counterpart of the durable-journal's input-hash binding — see [orchestrator/durable-journal](../orchestrator/durable-journal.md).
