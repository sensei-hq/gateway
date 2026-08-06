---
title: Expiration Tracking
doctype: feature
module: governance
status: planned
phase: 1
spec: SP-0
source: crates/gateway (proactive expiry)
---

# Expiration Tracking

> **Status: Planned — reactive `401→auth-lock` in SP-0 (Phase 1); the stateful expiration *tracking/alerts* in SP-DATA (Phase 4).** Reference: OmniRoute `providerExpiration`.

Proactive tracking of credential/quota expiry per connection, so a credential is
skipped (and an operator alerted) *before* it starts failing — the complement to
reactive [model lockout](../routing/model-lockout.md).

## Behavior

- Tracks `oauth_token | subscription | api_credits | free_tier_reset` with a status of `active | expiring_soon | expired | unknown` and an `alert_days` lead time.
- Detects expiry from responses: 401→token expired, 402→subscription expired, 429+reset-header→free-tier reset time.

## Scenarios

```gherkin
Feature: Expiration tracking
  Scenario: A credential expiring soon raises an alert
    Given an oauth_token expiring within alert_days
    Then its status is "expiring_soon" and it is surfaced to the operator

  Scenario: A 402 marks the subscription expired
    Given a provider returns HTTP 402
    Then the connection's subscription is marked expired

  Scenario: A 429 reset header records the free-tier reset time
    Given a 429 with x-ratelimit-reset in the future
    Then a free_tier_reset expiry is recorded at that time

  Scenario: An expired credential is skipped before it fails
    Given a connection whose credential is expired
    Then selection skips its models proactively
```

## Notes

- In-memory today; persisted in the data-tier when live metering lands (Phase 4).
