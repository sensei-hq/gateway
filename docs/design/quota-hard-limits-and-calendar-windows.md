# Sketch: hard/atomic quota + calendar windows (AUTH follow-ups)

- **Status:** Sketch — future work, not scheduled
- **Crate:** `gateway`
- **Builds on:** the shipped AUTH stream (`docs/design/subscription-quota-auth.md`,
  `src/engine.rs::check_quota`, `src/store.rs`). v1 chose **rolling windows (D2)**
  and **soft limits (D3)**; this sketches the two deferred hardenings. Both are
  **additive** — v1 behaviour is the default.

## Part A — Hard/atomic limits (close the TOCTOU overshoot)

**Problem.** `check_quota` reads usage, then the call runs, then it's recorded.
Between read and record, concurrent calls for the same subject can each pass the
check and overshoot the limit by ~the in-flight count. Fine for a gross guard;
wrong for a strict billing cap.

**Approach — a reservation API on `GatewayStore`, defaulted to the soft path** so
existing store impls don't break:

```rust
pub struct Reservation { pub id: Uuid, pub subject_id: Uuid /* + window keys */ }

#[async_trait]
pub trait GatewayStore {
    // … existing …
    /// Atomically check-and-hold this call's estimated deltas against every
    /// windowed limit. Returns QuotaExceeded without holding anything if any
    /// limit would be crossed. DEFAULT: falls back to `get_usage_since` + the
    /// soft check (today's behaviour) so unchanged impls still compile.
    async fn reserve(&self, subject: Uuid, deltas: &[(MeterUnit, Window, u64)], limits: &[QuotaLimit])
        -> Result<Reservation, GatewayError> { /* default: soft check, no hold */ }
    /// Finalize a reservation with the actual usage (adds output tokens + cost).
    async fn commit(&self, r: &Reservation, actual: &UsageTotals) -> Result<(), GatewayError> { Ok(()) }
    /// Roll back a held reservation (call failed / errored before commit).
    async fn release(&self, r: &Reservation) -> Result<(), GatewayError> { Ok(()) }
}
```

**Engine flow** (replaces the current check → run → record when a store opts in):
`reserve(estimate)` → dispatch → on success `commit(actual)`, on error `release`.
Reserve holds the pre-call estimate (requests=1, input tokens); commit reconciles
to actual (output tokens + `cost_usd_millis`).

**Atomicity options for a real impl:**
- **Postgres (multi-node):** a per-`(subject, unit, window_bucket)` counter row
  incremented with `UPDATE … SET used = used + $d WHERE used + $d <= $limit`
  (0 rows updated ⇒ QuotaExceeded), or a `SELECT … FOR UPDATE` / advisory lock.
- **Single-node gateway:** an in-process `Mutex`/semaphore keyed by
  `(subject, window)` around the read-modify-write — no DB round-trip.

**Orphaned reservations.** A crash between `reserve` and `commit`/`release` leaks a
hold. Needs a **TTL + sweep** (reservations expire after N minutes) or a
commit-with-idempotency-key so a retry reconciles. Design this in, don't bolt on.

**Cost:** one extra store round-trip per call (reserve), plus commit. Only for
subjects with a strict limit; the soft default stays free.

## Part B — Calendar-aligned windows (billing resets)

**Problem.** Rolling `now − period` never "resets" — a burst 25h ago still
partially counts. Billing usually wants "resets at 00:00 UTC / Monday / the 1st".

**Approach — the store needs *no* change.** `get_usage_since` already takes a
`since`; a calendar window just computes a **boundary** `since` instead of
`now − period`:

```rust
pub enum WindowKind { Rolling, Calendar }          // add to QuotaLimit (default Rolling)
// window_start(now, Window::Month, Calendar, tz) = first day of this month, 00:00 in tz
```

- `Day` → start of today; `Week` → start of this ISO week (Mon); `Month` → 1st.
- **Timezone matters** (a "day" boundary is tz-relative). Add an optional
  `tz: Option<String>` (IANA, default `UTC`) to `ConstraintsConfig` or per
  `TierConstraints`; billing periods are often per-tenant.
- Only `window_start` (in `engine.rs`) changes — a few lines + a `chrono-tz` dep
  for named zones. `Month` uses real calendar month length (28–31), not 30 days.

**Rollout:** `WindowKind` defaults to `Rolling`, `tz` defaults to `UTC`, so
existing configs and v1 behaviour are unchanged.

## Sequencing (if/when scheduled)

Calendar windows are the **cheap, high-value** one (small, no store change, no
trait break) — do first. Hard/atomic limits are the **bigger** lift (reservation
lifecycle, TTL sweep, per-impl atomicity) — do only if a tier needs a strict
(billing-grade) cap rather than a gross guard.
