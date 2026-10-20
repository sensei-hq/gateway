//! Send every case through the gateway facade, one model at a time, and record
//! exactly one [`ResultRow`] per (case, model) — `ok`, `not_applicable` or
//! `error` (spec D2/D2a, AC3). Nothing is retried and nothing is dropped.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use gateway::circuit_breaker::CircuitBreakerConfig;
use gateway::resilience::ResilienceConfig;
use gateway::types::config::GatewayConfig;
use gateway::types::decision::{DecisionAnswer, DecisionQuestion, DecisionQuestions};
use gateway::types::error::HumanAction;
use gateway::types::request::Payload;
use gateway::types::trace::Attempt;
use gateway::{
    Capability, FacadeBuilder, Gateway, GatewayError, InferenceRequest, InferenceResponse,
};
use serde::{Deserialize, Serialize};

use crate::case::{Case, Expected, Scenario};
use crate::config::{BenchConfig, ModelSpec, ROUTER_TIMEOUT, chain_id};
use crate::scenarios::Manifest;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Ok,
    NotApplicable,
    Error,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ResultRow {
    pub case_id: String,
    pub scenario: Scenario,
    pub model: String,
    pub outcome: Outcome,
    /// `Some` only when `outcome` is `Ok`.
    pub correct: Option<bool>,
    /// Probability of the answered option (noul: `max(p, 1 - p)`).
    pub top_probability: Option<f64>,
    /// Score answers: the probability-weighted level.
    pub score_value: Option<f64>,
    /// Score cases: the gold level, for MAE.
    pub expected_level: Option<usize>,
    /// The answer the decision gave (`Some` only when `outcome` is `Ok`):
    /// the chosen option key; `"yes"` / `"no"` for noul (decided at 0.5); the
    /// argmax level's key for score (as in `probabilities`).
    #[serde(default)]
    pub answer: Option<String>,
    /// The probabilities the decision returned (`Some` only when `outcome`
    /// is `Ok`): per option / per level key; noul: `{"yes": p}`.
    #[serde(default)]
    pub probabilities: Option<BTreeMap<String, f64>>,
    pub latency_ms: u64,
    /// Whether the call reached the model's server (the chain has exactly
    /// one candidate):
    /// - `true` for an answer; an `AllAttemptsFailed` whose attempt got a
    ///   reply or timed out (a timeout is a request the server was still
    ///   working on); and an `AllGated` whose `skipped` is empty — the engine
    ///   also returns `AllGated` *after* an attempt, e.g. a 429 whose lockout
    ///   makes the chain timed-gated.
    /// - `false` for a transport failure before any reply: a refused
    ///   connection, a DNS failure, or a connection dropped unanswered. The
    ///   kernel keeps only the error's text, and all of these (and a timeout)
    ///   read [`NETWORK_SEND_MARKER`]; an attempt that failed so before the
    ///   router timeout is one of these, one that lasted the whole timeout is
    ///   a timeout. Also `false` for an `AllGated` that lists the candidate in
    ///   `skipped` (a gate — cooldown, lockout, breaker, context window —
    ///   refused it before dispatch) and for every other pre-dispatch error.
    ///
    /// Caveats: a connect that hangs until the router timeout (a blackholed
    /// host) reads `true`. An adapter's own pre-send `InputRejected`
    /// (Laya head budget, images) is a recorded attempt and so reads `true`
    /// although no bytes reached the server; the error text cannot tell it
    /// apart from a server-side truncation.
    pub reached_server: bool,
    /// Reason for `NotApplicable` / `Error`, clipped to [`DETAIL_MAX_CHARS`].
    pub detail: Option<String>,
}

/// The kernel's `InputRejected` Display marker — pinned by
/// `the_input_rejected_marker_matches_the_kernel` against the real type.
pub const INPUT_REJECTED_MARKER: &str = "cannot serve this input: ";

/// How the kernel renders a `Network` error whose request got no reply
/// (`network error: ` + reqwest's `error sending request`): a refused or
/// dropped connection, a DNS failure, or a timeout alike. Pinned by
/// `the_network_send_marker_matches_the_kernel` against the real types.
pub const NETWORK_SEND_MARKER: &str = "network error: error sending request";

/// Longest `detail` kept on a row, in chars.
pub const DETAIL_MAX_CHARS: usize = 300;

/// A case still in flight after this long gets a progress line on stderr
/// (and another each further interval), so a hung server is visible.
pub const SLOW_CASE: Duration = Duration::from_secs(30);

/// A model that `check` found unreachable, so it was not run.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct NotRun {
    pub model: String,
    pub reason: String,
}

/// One model of the run's roster, as configured, so a report rendered from
/// `results.json` alone can say what each model id was. The URL is left out
/// on purpose: a results file is shared, and a hosted URL can carry a token.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct RosterEntry {
    pub id: String,
    pub router: String,
    pub api_model_id: String,
    pub context_window: u32,
}

impl From<&ModelSpec> for RosterEntry {
    fn from(m: &ModelSpec) -> Self {
        RosterEntry {
            id: m.id.clone(),
            router: m.router.clone(),
            api_model_id: m.api_model_id.clone(),
            context_window: m.context_window,
        }
    }
}

/// The whole of `results.json`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Results {
    /// Unix seconds.
    pub generated_at: u64,
    pub manifest: Option<Manifest>,
    pub not_run: Vec<NotRun>,
    /// Every selected model, NOT RUN ones included, in config order.
    /// (`default`: files written before T7 have none.)
    #[serde(default)]
    pub roster: Vec<RosterEntry>,
    /// Each hand-curated (S4) scenario run, with its file header's
    /// `labels_reviewed_by`.
    #[serde(default)]
    pub labels_reviewed: Vec<LabelReview>,
    /// Models whose every row is in `rows`, in the order they finished. A
    /// roster model in neither this nor `not_run` never finished (the run
    /// stopped early).
    #[serde(default)]
    pub completed: Vec<String>,
    /// The paths the run used, for the report's reproduce commands.
    #[serde(default)]
    pub paths: Option<RunPaths>,
    pub rows: Vec<ResultRow>,
}

/// Paths as given to the CLI: relative to the crate directory where they lie
/// under it, else absolute.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct RunPaths {
    /// `None`: the default (`bench.toml`, else the example).
    pub config: Option<String>,
    pub cases: String,
    /// The results file `run` wrote (`report --results`).
    pub results: String,
    /// Set by `report` to its `--out` before rendering.
    #[serde(default)]
    pub report: Option<String>,
}

/// The sign-off state of one hand-curated scenario's labels.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct LabelReview {
    pub scenario: Scenario,
    /// `None`: the labels are unreviewed.
    pub reviewed_by: Option<String>,
}

/// `text` with `m.url`, and every `url (…)` segment (how transport errors
/// quote the request URL), replaced by `<url>`: a URL can carry a token, and
/// reasons and details end up in `results.json` and the committed report.
pub fn redact(text: &str, m: &ModelSpec) -> String {
    let mut out = text.to_string();
    let bare = m.url.trim_end_matches('/');
    for url in [m.url.as_str(), bare] {
        if !url.is_empty() {
            out = out.replace(url, "<url>");
        }
    }
    const SEGMENT: &str = "url (";
    let mut redacted = String::with_capacity(out.len());
    let mut rest = out.as_str();
    while let Some(at) = rest.find(SEGMENT) {
        let start = at + SEGMENT.len();
        redacted.push_str(&rest[..start]);
        redacted.push_str("<url>");
        // Up to the closing paren; a clipped segment runs to the end.
        rest = rest[start..]
            .find(')')
            .map_or("", |end| &rest[start + end..]);
    }
    redacted.push_str(rest);
    redacted
}

/// What [`score`] makes of an answer to a case.
#[derive(Clone, Debug, PartialEq)]
pub struct Scored {
    pub correct: bool,
    /// What [`ResultRow::answer`] records.
    pub answer: String,
    /// What [`ResultRow::probabilities`] records.
    pub probabilities: BTreeMap<String, f64>,
    pub top_probability: f64,
    pub score_value: Option<f64>,
    pub expected_level: Option<usize>,
}

/// Classify a failed `execute` for a single-candidate, no-fallback chain.
///
/// - `AllGated` with `UseLargerContextWindow`: the gateway refused before
///   sending because the input does not fit the window → `NotApplicable`.
/// - `AllAttemptsFailed` where every attempt failed with the kernel's
///   `InputRejected` Display (`adapter '…' cannot serve this input: …`) →
///   `NotApplicable`. An empty attempt list is not evidence of a rejection.
/// - Anything else → `Error`.
///
/// The `String` is the reason, unclipped.
pub fn classify(err: &GatewayError) -> (Outcome, String) {
    match err {
        GatewayError::AllGated {
            human_action: Some(HumanAction::UseLargerContextWindow),
            ..
        } => (Outcome::NotApplicable, err.to_string()),
        GatewayError::AllAttemptsFailed {
            attempts_detail, ..
        } if !attempts_detail.is_empty()
            && attempts_detail
                .iter()
                .all(|a| a.error.as_deref().is_some_and(is_input_rejected)) =>
        {
            let reasons: Vec<&str> = attempts_detail
                .iter()
                .filter_map(|a| a.error.as_deref())
                .collect();
            (Outcome::NotApplicable, reasons.join("; "))
        }
        _ => (Outcome::Error, err.to_string()),
    }
}

/// The kernel renders `InputRejected` as `adapter '<id>' cannot serve this
/// input: <message>`. Requiring the `adapter '` prefix as well keeps a
/// `ProviderError` (`provider error from adapter …`) whose upstream body
/// happens to quote the marker from counting as a rejection.
fn is_input_rejected(error: &str) -> bool {
    error.starts_with("adapter '")
        && error
            .find(INPUT_REJECTED_MARKER)
            .is_some_and(|at| error[..at].ends_with("' "))
}

/// Score an answer against the case's expectation.
///
/// - Choice: correct iff the chosen key is the expected option;
///   `top_probability` is the chosen option's probability.
/// - Noul: decided at 0.5 (`p >= 0.5` is `true`); `top_probability` is
///   `max(p, 1 - p)`.
/// - Score: correct iff the argmax level of `probabilities` is the expected
///   level. **A tie goes to the lower level.** `score_value` is the answer's
///   probability-weighted `score`; `top_probability` is the argmax level's
///   probability; `expected_level` is the gold level.
///
/// An answer of the wrong type for the question, or one missing the
/// probability it needs, is an `Err`.
pub fn score(case: &Case, answer: &DecisionAnswer) -> anyhow::Result<Scored> {
    let id = &case.id;
    match (&case.expected, answer) {
        (
            Expected::Choice { option },
            DecisionAnswer::Choice {
                choice,
                probabilities,
                ..
            },
        ) => {
            check_probabilities(id, probabilities)?;
            let p = *probabilities.get(choice).with_context(|| {
                format!("case {id}: answer chose {choice:?} but gave it no probability")
            })?;
            Ok(Scored {
                correct: choice == option,
                answer: choice.clone(),
                probabilities: owned(probabilities),
                top_probability: p,
                score_value: None,
                expected_level: None,
            })
        }
        (Expected::Noul { value }, DecisionAnswer::Noul { noul }) => {
            let p = *noul;
            check_probability(id, "noul", p)?;
            Ok(Scored {
                correct: (p >= 0.5) == *value,
                answer: if p >= 0.5 { "yes" } else { "no" }.into(),
                probabilities: BTreeMap::from([("yes".to_string(), p)]),
                top_probability: p.max(1.0 - p),
                score_value: None,
                expected_level: None,
            })
        }
        (
            Expected::Score { level },
            DecisionAnswer::Score {
                score,
                probabilities,
                ..
            },
        ) => {
            check_probabilities(id, probabilities)?;
            let DecisionQuestion::Score { criteria, .. } = &case.question else {
                bail!("case {id}: expects a score but its question is not a score question");
            };
            // Argmax over levels; a tie goes to the LOWER level (levels are
            // visited in ascending order and only a strictly greater
            // probability replaces the incumbent).
            let mut levels = Vec::with_capacity(probabilities.len());
            for (k, p) in probabilities {
                let l: usize = k.parse().with_context(|| {
                    format!("case {id}: score probability key {k:?} is not a level index")
                })?;
                levels.push((l, *p, k));
            }
            levels.sort_by_key(|(l, _, _)| *l);
            let mut best: Option<(usize, f64, &String)> = None;
            for (l, p, k) in levels {
                if best.is_none_or(|(_, bp, _)| p > bp) {
                    best = Some((l, p, k));
                }
            }
            let (argmax, p, key) =
                best.with_context(|| format!("case {id}: score answer has no probabilities"))?;
            if argmax >= criteria.len() {
                bail!(
                    "case {id}: answer's most probable level {argmax} is beyond the question's {} levels",
                    criteria.len()
                );
            }
            Ok(Scored {
                correct: argmax == *level,
                answer: key.clone(),
                probabilities: owned(probabilities),
                top_probability: p,
                score_value: Some(*score),
                expected_level: Some(*level),
            })
        }
        (e, a) => bail!(
            "case {id}: answer type {} does not match the expected {e:?}",
            match a {
                DecisionAnswer::Choice { .. } => "choice",
                DecisionAnswer::Noul { .. } => "noul",
                DecisionAnswer::Score { .. } => "score",
            }
        ),
    }
}

fn owned<'a>(
    probabilities: impl IntoIterator<Item = (&'a String, &'a f64)>,
) -> BTreeMap<String, f64> {
    probabilities
        .into_iter()
        .map(|(k, p)| (k.clone(), *p))
        .collect()
}

/// `p` must be a number within 0..=1 (NaN is not).
fn check_probability(id: &str, what: &str, p: f64) -> anyhow::Result<()> {
    if !(0.0..=1.0).contains(&p) {
        bail!("case {id}: {what} probability {p} is not within 0..=1");
    }
    Ok(())
}

/// Every per-option / per-level probability must be a number within 0..=1.
fn check_probabilities<'a>(
    id: &str,
    probabilities: impl IntoIterator<Item = (&'a String, &'a f64)>,
) -> anyhow::Result<()> {
    for (k, p) in probabilities {
        check_probability(id, &format!("{k:?}"), *p)?;
    }
    Ok(())
}

/// The facade builder for `m`: its single-entry chain, with the circuit
/// breaker effectively OFF (`threshold: usize::MAX`). With the default
/// threshold, five consecutive real failures would open the breaker and every
/// later case would be a ~0 ms gated skip that never reaches the server.
pub fn facade_for(cfg: &BenchConfig, m: &ModelSpec) -> anyhow::Result<FacadeBuilder> {
    Ok(facade_from(cfg.gateway_config_for(m)?))
}

fn facade_from(config: GatewayConfig) -> FacadeBuilder {
    FacadeBuilder::new(config).circuit_breaker(CircuitBreakerConfig {
        threshold: usize::MAX,
        timeout: Duration::from_secs(1),
        half_open_max_requests: 1,
    })
}

/// The gateway a bench run uses for `m`: [`facade_for`] (breaker off) plus a
/// resilience policy with no transport cooldown and no synthetic lockout.
///
/// Turning the breaker off is not enough. With the default
/// [`ResilienceConfig`], one `Network`/`Timeout` failure (a cold model load
/// timing out on the warm-up is the likeliest) cools the whole router for
/// 30 s, and a 429 locks the model out for 60 s; every case in that window is
/// skipped as a 0 ms `AllGated` that never reaches the server. So:
///
/// - `cooldown_base = 0`: a transport fault cools nothing.
/// - `lockout.rate_limit_base = 0` and `lockout.quota_default = 0`: a 429 or
///   403-quota *without* `Retry-After` locks nothing; each later case goes to
///   the server and is recorded as whatever it answers.
///
/// What this cannot turn off: a real upstream `Retry-After` is honoured
/// verbatim by the gateway (never clamped), and an auth / credits failure
/// locks the model terminally. Cases skipped by either are rows with
/// `reached_server: false`. All other resilience fields keep their defaults.
pub async fn gateway_for(cfg: &BenchConfig, m: &ModelSpec) -> anyhow::Result<Gateway> {
    Ok(gateway_from(facade_for(cfg, m)?).await)
}

/// [`gateway_for`], with `m`'s router timeout set to `timeout` instead of the
/// run's per-case 120 s (`check` probes with a short one).
pub async fn gateway_with_timeout(
    cfg: &BenchConfig,
    m: &ModelSpec,
    timeout: Duration,
) -> anyhow::Result<Gateway> {
    let mut config = cfg.gateway_config_for(m)?;
    let ms = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
    for router in config.routers.values_mut() {
        router.timeout_ms = Some(ms);
    }
    Ok(gateway_from(facade_from(config)).await)
}

async fn gateway_from(facade: FacadeBuilder) -> Gateway {
    let mut resilience = ResilienceConfig::default();
    resilience.cooldown_base = Duration::ZERO;
    resilience.lockout.rate_limit_base = Duration::ZERO;
    resilience.lockout.quota_default = Duration::ZERO;
    facade.build().await.gateway.with_resilience(resilience)
}

/// The warm-up case: the one with the shortest serialized `state` (first on a
/// tie), so loading the model costs as little as possible.
pub fn warm_up_case(cases: &[Case]) -> Option<&Case> {
    cases
        .iter()
        .min_by_key(|c| serde_json::to_string(&c.state).map_or(usize::MAX, |s| s.len()))
}

/// The request for `case` on `m`'s chain, with fallback off.
pub fn request_for(case: &Case, m: &ModelSpec) -> InferenceRequest {
    InferenceRequest {
        capability: Capability::Decision,
        model: None,
        router: None,
        chain: Some(chain_id(m)),
        payload: Payload::Decision {
            state: case.state.clone(),
            questions: DecisionQuestions::from([("q".to_string(), case.question.clone())]),
            images: vec![],
            keep_alive: None,
        },
        budget: None,
        auth: None,
        panel: None,
        consensus: None,
        allow_fallback: false,
        credentials: Default::default(),
        routing: None,
    }
}

fn clip(s: &str) -> String {
    s.chars().take(DETAIL_MAX_CHARS).collect()
}

/// The one row for (`case`, `model`) from the result of `execute`, its
/// `detail` clipped to [`DETAIL_MAX_CHARS`].
pub fn row_for(
    case: &Case,
    model: &str,
    result: &Result<InferenceResponse, GatewayError>,
    latency_ms: u64,
) -> ResultRow {
    let mut row = row_unclipped(case, model, result, latency_ms, ROUTER_TIMEOUT);
    row.detail = row.detail.map(|d| clip(&d));
    row
}

/// [`row_for`] for `m` called with router timeout `router_timeout`, with `m`'s
/// URL [`redact`]ed from the detail before it is clipped (so a clip cannot cut
/// a URL past recognition). Every row a run records goes through this.
pub fn row_for_model(
    case: &Case,
    m: &ModelSpec,
    result: &Result<InferenceResponse, GatewayError>,
    latency_ms: u64,
    router_timeout: Duration,
) -> ResultRow {
    let mut row = row_unclipped(case, &m.id, result, latency_ms, router_timeout);
    row.detail = row.detail.map(|d| clip(&redact(&d, m)));
    row
}

fn row_unclipped(
    case: &Case,
    model: &str,
    result: &Result<InferenceResponse, GatewayError>,
    latency_ms: u64,
    router_timeout: Duration,
) -> ResultRow {
    let mut row = ResultRow {
        case_id: case.id.clone(),
        scenario: case.scenario,
        model: model.to_string(),
        outcome: Outcome::Error,
        correct: None,
        top_probability: None,
        score_value: None,
        expected_level: None,
        answer: None,
        probabilities: None,
        latency_ms,
        reached_server: reached_server(result, router_timeout),
        detail: None,
    };
    let answer = match result {
        Err(e) => {
            let (outcome, reason) = classify(e);
            row.outcome = outcome;
            row.detail = Some(reason);
            return row;
        }
        Ok(resp) => resp.decisions.as_ref().and_then(|d| d.get("q")),
    };
    let Some(answer) = answer else {
        row.detail = Some("no answer for q".into());
        return row;
    };
    match score(case, answer) {
        Ok(s) => {
            row.outcome = Outcome::Ok;
            row.correct = Some(s.correct);
            row.top_probability = Some(s.top_probability);
            row.score_value = s.score_value;
            row.expected_level = s.expected_level;
            row.answer = Some(s.answer);
            row.probabilities = Some(s.probabilities);
        }
        Err(e) => row.detail = Some(format!("{e:#}")),
    }
    row
}

/// See [`ResultRow::reached_server`].
fn reached_server(
    result: &Result<InferenceResponse, GatewayError>,
    router_timeout: Duration,
) -> bool {
    match result {
        Ok(_) => true,
        Err(GatewayError::AllAttemptsFailed {
            attempts_detail, ..
        }) => attempts_detail
            .iter()
            .any(|a| attempt_reached_server(a, router_timeout)),
        // Single-candidate chain: the candidate is either skipped by a gate
        // (listed in `skipped`, never dispatched) or was attempted and its
        // outcome made the chain gated (a 429 → timed lockout).
        Err(GatewayError::AllGated { skipped, .. }) => skipped.is_empty(),
        Err(_) => false,
    }
}

/// An attempt reached the server unless it failed with
/// [`NETWORK_SEND_MARKER`] before the router timeout ran out.
fn attempt_reached_server(a: &Attempt, router_timeout: Duration) -> bool {
    let unanswered = a
        .error
        .as_deref()
        .is_some_and(|e| e.starts_with(NETWORK_SEND_MARKER));
    !unanswered || u128::from(a.duration_ms) >= router_timeout.as_millis()
}

/// Run every case on every model in `model_ids`, in config order, one case at
/// a time, calling `on_model_done(model_id, rows)` with that model's rows as
/// soon as it finishes (so a caller can persist partial results). Returns every
/// row.
///
/// Each model first gets one warm-up call ([`warm_up_case`]); its result is
/// never recorded, and a failed warm-up is logged to stderr. Progress goes to
/// stderr: a line when a model starts and finishes, and a line for any case
/// still in flight after each [`SLOW_CASE`].
///
/// An id not in the config is an error naming it; an empty `model_ids` runs
/// nothing.
pub async fn run_with_progress(
    cfg: &BenchConfig,
    cases: &[Case],
    model_ids: &[String],
    mut on_model_done: impl FnMut(&str, &[ResultRow]),
) -> anyhow::Result<Vec<ResultRow>> {
    if let Some(unknown) = model_ids
        .iter()
        .find(|id| !cfg.models.iter().any(|m| &m.id == *id))
    {
        bail!("unknown model id '{unknown}': not in the bench config");
    }
    let mut rows = Vec::with_capacity(cases.len() * model_ids.len());
    for m in cfg.models.iter().filter(|m| model_ids.contains(&m.id)) {
        let started_model = Instant::now();
        eprintln!("decision-bench: {}: {} cases", m.id, cases.len());
        let gw = gateway_for(cfg, m).await?;
        if let Some(warm) = warm_up_case(cases) {
            // Loads the model; the result is never recorded.
            if let Err(e) = execute_watched(&gw, &request_for(warm, m), &m.id, "warm-up").await {
                eprintln!(
                    "decision-bench: {}: warm-up failed (not recorded): {}",
                    m.id,
                    redact(&e.to_string(), m)
                );
            }
        }
        let first = rows.len();
        for case in cases {
            let req = request_for(case, m);
            let started = Instant::now();
            let result = execute_watched(&gw, &req, &m.id, &case.id).await;
            let latency_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
            rows.push(row_for_model(case, m, &result, latency_ms, ROUTER_TIMEOUT));
        }
        let done = &rows[first..];
        let count = |o: Outcome| done.iter().filter(|r| r.outcome == o).count();
        eprintln!(
            "decision-bench: {}: done in {:.1}s — {} ok, {} not_applicable, {} error ({} never reached the server)",
            m.id,
            started_model.elapsed().as_secs_f64(),
            count(Outcome::Ok),
            count(Outcome::NotApplicable),
            count(Outcome::Error),
            done.iter().filter(|r| !r.reached_server).count(),
        );
        on_model_done(&m.id, done);
    }
    Ok(rows)
}

/// `gw.execute(req)`, with a stderr line for every [`SLOW_CASE`] it is still
/// in flight.
async fn execute_watched(
    gw: &Gateway,
    req: &InferenceRequest,
    model: &str,
    what: &str,
) -> Result<InferenceResponse, GatewayError> {
    let started = Instant::now();
    let call = gw.execute(req);
    tokio::pin!(call);
    loop {
        tokio::select! {
            result = &mut call => return result,
            _ = tokio::time::sleep(SLOW_CASE) => eprintln!(
                "decision-bench: {model}: {what} still waiting after {}s",
                started.elapsed().as_secs()
            ),
        }
    }
}

/// [`run_with_progress`] without a per-model callback.
pub async fn run(
    cfg: &BenchConfig,
    cases: &[Case],
    model_ids: &[String],
) -> anyhow::Result<Vec<ResultRow>> {
    run_with_progress(cfg, cases, model_ids, |_, _| {}).await
}

/// Write `results` as pretty JSON to `path` atomically
/// ([`crate::fetch::write_atomic`]: a unique synced temp file, then a rename).
pub fn write_results(path: &Path, results: &Results) -> anyhow::Result<()> {
    let mut json = serde_json::to_string_pretty(results)?;
    json.push('\n');
    crate::fetch::write_atomic(path, json.as_bytes())
}

/// Run `model_ids` one model at a time (config order) via
/// [`run_with_progress`], appending each model's rows to `results` and
/// rewriting `out` atomically after every model, so a crash or a failed model
/// loses at most the model in flight. `out` is first written before any model
/// runs (so NOT RUN models are on disk even if the first model fails).
/// Returns the final results; on an error the file holds every model finished
/// before it.
pub async fn run_saving(
    cfg: &BenchConfig,
    cases: &[Case],
    model_ids: &[String],
    mut results: Results,
    out: &Path,
) -> anyhow::Result<Results> {
    if let Some(unknown) = model_ids
        .iter()
        .find(|id| !cfg.models.iter().any(|m| &m.id == *id))
    {
        bail!("unknown model id '{unknown}': not in the bench config");
    }
    write_results(out, &results)?;
    for m in cfg.models.iter().filter(|m| model_ids.contains(&m.id)) {
        let rows = run_with_progress(cfg, cases, std::slice::from_ref(&m.id), |_, _| {})
            .await
            .with_context(|| format!("running model '{}'", m.id))?;
        results.rows.extend(rows);
        results.completed.push(m.id.clone());
        write_results(out, &results)?;
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use gateway::types::trace::AttemptStatus;
    use serde_json::json;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn attempt(error: &str) -> Attempt {
        Attempt {
            sequence: 1,
            adapter: "laya".into(),
            model: "english".into(),
            api_model_id: "english".into(),
            status: AttemptStatus::Failed,
            duration_ms: 3,
            tokens: None,
            cost: None,
            error: Some(error.into()),
            fallback_triggered: false,
        }
    }

    fn all_failed(errors: &[&str]) -> GatewayError {
        GatewayError::AllAttemptsFailed {
            attempts: errors.len(),
            errors: errors.join("; "),
            attempts_detail: errors.iter().map(|e| attempt(e)).collect(),
            retryable: false,
        }
    }

    fn gated(human_action: Option<HumanAction>) -> GatewayError {
        GatewayError::AllGated {
            resume_after: None,
            skipped: vec!["laya:english — over window".into()],
            human_action,
        }
    }

    fn input_rejected_text() -> String {
        GatewayError::InputRejected {
            adapter: "laya".into(),
            message: "state truncated".into(),
        }
        .to_string()
    }

    fn provider_error_text() -> String {
        GatewayError::ProviderError {
            adapter: "laya".into(),
            message: "boom".into(),
            status: Some(500),
        }
        .to_string()
    }

    fn case(question: DecisionQuestion, expected: Expected) -> Case {
        Case {
            id: "c1".into(),
            scenario: Scenario::S3Entailment,
            state: json!("some state"),
            question,
            expected,
            source: "test".into(),
        }
    }

    fn choice_case(expected: &str) -> Case {
        case(
            DecisionQuestion::Choice {
                instructions: json!("pick"),
                criteria: [("a".to_string(), None), ("b".to_string(), None)]
                    .into_iter()
                    .collect(),
            },
            Expected::Choice {
                option: expected.into(),
            },
        )
    }

    fn noul_case(value: bool) -> Case {
        case(
            DecisionQuestion::Noul {
                instructions: json!("holds?"),
                criteria: None,
            },
            Expected::Noul { value },
        )
    }

    fn score_case(level: usize) -> Case {
        case(
            DecisionQuestion::Score {
                instructions: json!("how much?"),
                criteria: vec!["low".into(), "mid".into(), "high".into()],
            },
            Expected::Score { level },
        )
    }

    fn choice(choice: &str, probs: &[(&str, f64)]) -> DecisionAnswer {
        DecisionAnswer::Choice {
            choice: choice.into(),
            probabilities: probs.iter().map(|(k, p)| (k.to_string(), *p)).collect(),
            confidence: 0.5,
        }
    }

    fn score_answer(score: f64, probs: &[f64]) -> DecisionAnswer {
        DecisionAnswer::Score {
            score,
            legend: (0..probs.len())
                .map(|i| (i.to_string(), format!("level {i}")))
                .collect(),
            probabilities: probs
                .iter()
                .enumerate()
                .map(|(i, p)| (i.to_string(), *p))
                .collect(),
            confidence: 0.5,
        }
    }

    fn spec(url: &str) -> ModelSpec {
        ModelSpec {
            id: "nimble-v3".into(),
            router: "llamacpp".into(),
            url: url.into(),
            api_model_id: "nimble-v3".into(),
            context_window: 8192,
            api_key_env: None,
        }
    }

    fn response(decisions: serde_json::Value) -> InferenceResponse {
        serde_json::from_value(json!({
            "success": true,
            "decisions": decisions,
            "attempts": [],
        }))
        .unwrap()
    }

    // --- marker pin -------------------------------------------------------

    /// A9: a URL ending in `/` is redacted both as given and with the slash
    /// trimmed (how an adapter joins it to a path).
    #[test]
    fn a_url_is_redacted_as_given_and_with_its_trailing_slash_trimmed() {
        let m = spec("http://127.0.0.1:9/?token=SECRETTOK/");
        let text = "a http://127.0.0.1:9/?token=SECRETTOK/ b http://127.0.0.1:9/?token=SECRETTOK c";
        let out = redact(text, &m);
        assert!(!out.contains("SECRETTOK"), "{out}");
        assert_eq!(out, "a <url> b <url> c");
    }

    #[test]
    fn the_input_rejected_marker_matches_the_kernel() {
        let text = GatewayError::InputRejected {
            adapter: "laya".into(),
            message: "m".into(),
        }
        .to_string();
        assert!(text.contains(INPUT_REJECTED_MARKER), "{text}");
        assert!(text.starts_with("adapter 'laya' "), "{text}");
    }

    // --- classify ---------------------------------------------------------

    #[test]
    fn a_context_window_refusal_is_not_applicable() {
        let (o, d) = classify(&gated(Some(HumanAction::UseLargerContextWindow)));
        assert_eq!(o, Outcome::NotApplicable);
        assert!(!d.is_empty());
    }

    #[test]
    fn an_all_gated_with_another_or_no_human_action_is_an_error() {
        assert_eq!(
            classify(&gated(Some(HumanAction::TopUpCredits))).0,
            Outcome::Error
        );
        assert_eq!(classify(&gated(None)).0, Outcome::Error);
    }

    #[test]
    fn an_input_rejected_attempt_is_not_applicable_with_its_reason() {
        let (o, d) = classify(&all_failed(&[&input_rejected_text()]));
        assert_eq!(o, Outcome::NotApplicable);
        assert!(d.contains("state truncated"), "{d}");
    }

    #[test]
    fn a_provider_error_attempt_is_an_error() {
        let (o, d) = classify(&all_failed(&[&provider_error_text()]));
        assert_eq!(o, Outcome::Error);
        assert!(d.contains("boom"), "{d}");
    }

    #[test]
    fn a_provider_error_beside_an_input_rejection_is_an_error() {
        let e = all_failed(&[&provider_error_text(), &input_rejected_text()]);
        assert_eq!(classify(&e).0, Outcome::Error);
    }

    #[test]
    fn a_provider_error_quoting_the_marker_is_still_an_error() {
        let quoted = GatewayError::ProviderError {
            adapter: "laya".into(),
            message: format!("upstream said: {INPUT_REJECTED_MARKER}x"),
            status: Some(400),
        }
        .to_string();
        assert_eq!(classify(&all_failed(&[&quoted])).0, Outcome::Error);
    }

    #[test]
    fn an_all_attempts_failed_with_no_attempts_is_an_error() {
        assert_eq!(classify(&all_failed(&[])).0, Outcome::Error);
    }

    #[test]
    fn timeout_and_network_errors_are_errors() {
        let t = GatewayError::Timeout {
            adapter: "laya".into(),
            model: "english".into(),
            duration_ms: 120_000,
        };
        assert_eq!(classify(&t).0, Outcome::Error);
        let network = reqwest::Client::new()
            .get("http://[::1")
            .build()
            .expect_err("an unparseable URL is a reqwest::Error");
        assert_eq!(classify(&GatewayError::Network(network)).0, Outcome::Error);
    }

    // --- score ------------------------------------------------------------

    #[test]
    fn a_choice_equal_to_the_expected_option_is_correct_with_its_probability() {
        let s = score(&choice_case("a"), &choice("a", &[("a", 0.7), ("b", 0.3)])).unwrap();
        assert_eq!(
            s,
            Scored {
                correct: true,
                answer: "a".into(),
                probabilities: BTreeMap::from([("a".into(), 0.7), ("b".into(), 0.3)]),
                top_probability: 0.7,
                score_value: None,
                expected_level: None
            }
        );
        let s = score(&choice_case("a"), &choice("b", &[("a", 0.4), ("b", 0.6)])).unwrap();
        assert!(!s.correct);
        assert_eq!(s.top_probability, 0.6);
    }

    #[test]
    fn a_choice_without_a_probability_for_its_pick_is_an_err() {
        assert!(score(&choice_case("a"), &choice("a", &[("b", 1.0)])).is_err());
    }

    #[test]
    fn noul_is_decided_at_one_half() {
        let s = score(&noul_case(true), &DecisionAnswer::Noul { noul: 0.5 }).unwrap();
        assert!(s.correct, "p = 0.5 decides true");
        let s = score(&noul_case(true), &DecisionAnswer::Noul { noul: 0.49 }).unwrap();
        assert!(!s.correct);
        let s = score(&noul_case(false), &DecisionAnswer::Noul { noul: 0.2 }).unwrap();
        assert!(s.correct);
    }

    #[test]
    fn noul_top_probability_is_the_decided_sides_probability() {
        let s = score(&noul_case(false), &DecisionAnswer::Noul { noul: 0.2 }).unwrap();
        assert!((s.top_probability - 0.8).abs() < 1e-12, "{s:?}");
        let s = score(&noul_case(true), &DecisionAnswer::Noul { noul: 0.9 }).unwrap();
        assert!((s.top_probability - 0.9).abs() < 1e-12, "{s:?}");
    }

    #[test]
    fn a_noul_outside_zero_to_one_is_an_err() {
        assert!(score(&noul_case(true), &DecisionAnswer::Noul { noul: 1.5 }).is_err());
        assert!(score(&noul_case(true), &DecisionAnswer::Noul { noul: f64::NAN }).is_err());
    }

    #[test]
    fn score_correctness_is_the_argmax_level_and_records_value_and_gold() {
        let s = score(&score_case(2), &score_answer(1.6, &[0.1, 0.2, 0.7])).unwrap();
        assert_eq!(
            s,
            Scored {
                correct: true,
                answer: "2".into(),
                probabilities: BTreeMap::from([
                    ("0".into(), 0.1),
                    ("1".into(), 0.2),
                    ("2".into(), 0.7)
                ]),
                top_probability: 0.7,
                score_value: Some(1.6),
                expected_level: Some(2)
            }
        );
        let s = score(&score_case(1), &score_answer(1.6, &[0.1, 0.2, 0.7])).unwrap();
        assert!(!s.correct);
        assert_eq!(s.expected_level, Some(1));
    }

    #[test]
    fn a_score_tie_goes_to_the_lower_level() {
        let lower = score(&score_case(0), &score_answer(1.0, &[0.4, 0.2, 0.4])).unwrap();
        assert!(lower.correct, "tie between 0 and 2 picks 0");
        let higher = score(&score_case(2), &score_answer(1.0, &[0.4, 0.2, 0.4])).unwrap();
        assert!(!higher.correct);
    }

    #[test]
    fn a_score_with_a_non_numeric_or_no_level_is_an_err() {
        let bad = DecisionAnswer::Score {
            score: 0.0,
            legend: Default::default(),
            probabilities: [("low".to_string(), 1.0)].into_iter().collect(),
            confidence: 1.0,
        };
        assert!(score(&score_case(0), &bad).is_err());
        assert!(score(&score_case(0), &score_answer(0.0, &[])).is_err());
    }

    #[test]
    fn an_answer_of_the_wrong_type_is_an_err() {
        let e = score(&choice_case("a"), &DecisionAnswer::Noul { noul: 0.9 }).unwrap_err();
        assert!(e.to_string().contains("c1"), "names the case: {e}");
        assert!(score(&noul_case(true), &choice("a", &[("a", 1.0)])).is_err());
        assert!(score(&score_case(0), &DecisionAnswer::Noul { noul: 0.9 }).is_err());
    }

    // --- rows -------------------------------------------------------------

    #[test]
    fn a_missing_q_answer_is_an_error() {
        let resp = response(json!({"other": {"type": "noul", "noul": 0.9}}));
        let row = row_for(&noul_case(true), "m", &Ok(resp), 12);
        assert_eq!(row.outcome, Outcome::Error);
        assert_eq!(row.detail.as_deref(), Some("no answer for q"));
        assert_eq!(row.correct, None);

        let mut none = response(json!({}));
        none.decisions = None;
        let row = row_for(&noul_case(true), "m", &Ok(none), 12);
        assert_eq!(row.outcome, Outcome::Error);
        assert_eq!(row.detail.as_deref(), Some("no answer for q"));
    }

    #[test]
    fn an_answered_case_is_an_ok_row() {
        let resp = response(json!({"q": {"type": "noul", "noul": 0.2}}));
        let row = row_for(&noul_case(false), "nimble", &Ok(resp), 42);
        assert_eq!(
            row,
            ResultRow {
                case_id: "c1".into(),
                scenario: Scenario::S3Entailment,
                model: "nimble".into(),
                outcome: Outcome::Ok,
                correct: Some(true),
                top_probability: Some(0.8),
                score_value: None,
                expected_level: None,
                answer: Some("no".into()),
                probabilities: Some(BTreeMap::from([("yes".into(), 0.2)])),
                latency_ms: 42,
                reached_server: true,
                detail: None,
            }
        );
    }

    /// SP-DEC-4 review A2 (spec D4/D7): a scored row records the answer and
    /// the probabilities it was chosen from; a row with no answer has neither.
    #[test]
    fn a_scored_row_carries_its_answer_and_probabilities() {
        let as_json = |row: &ResultRow| serde_json::to_value(row).unwrap();
        let choice = response(json!({"q": {
            "type": "choice", "choice": "b",
            "probabilities": {"a": 0.25, "b": 0.75}, "confidence": 0.5
        }}));
        let v = as_json(&row_for(&choice_case("a"), "m", &Ok(choice), 1));
        assert_eq!(v["answer"], "b", "{v}");
        assert_eq!(v["probabilities"], json!({"a": 0.25, "b": 0.75}), "{v}");

        let noul = response(json!({"q": {"type": "noul", "noul": 0.25}}));
        let v = as_json(&row_for(&noul_case(true), "m", &Ok(noul), 1));
        assert_eq!(v["answer"], "no", "{v}");
        assert_eq!(v["probabilities"], json!({"yes": 0.25}), "{v}");

        let score = response(json!({"q": {
            "type": "score", "score": 1.5,
            "legend": {"0": "low", "1": "mid", "2": "high"},
            "probabilities": {"0": 0.1, "1": 0.3, "2": 0.6}, "confidence": 0.5
        }}));
        let v = as_json(&row_for(&score_case(2), "m", &Ok(score), 1));
        assert_eq!(v["answer"], "2", "{v}");
        assert_eq!(
            v["probabilities"],
            json!({"0": 0.1, "1": 0.3, "2": 0.6}),
            "{v}"
        );

        let mistyped = response(json!({"q": {"type": "noul", "noul": 0.2}}));
        let v = as_json(&row_for(&choice_case("a"), "m", &Ok(mistyped), 1));
        assert_eq!(v["outcome"], "error", "{v}");
        assert_eq!(v["answer"], serde_json::Value::Null, "{v}");
        assert_eq!(v["probabilities"], serde_json::Value::Null, "{v}");
    }

    #[test]
    fn a_mistyped_answer_is_an_error_row_not_a_dropped_one() {
        let resp = response(json!({"q": {"type": "noul", "noul": 0.2}}));
        let row = row_for(&choice_case("a"), "m", &Ok(resp), 1);
        assert_eq!(row.outcome, Outcome::Error);
        assert_eq!(row.correct, None);
        assert!(row.detail.is_some());
    }

    #[test]
    fn a_failed_call_row_carries_the_class_and_a_clipped_reason() {
        let long = GatewayError::InputRejected {
            adapter: "laya".into(),
            message: "é".repeat(1000),
        }
        .to_string();
        let row = row_for(&noul_case(true), "m", &Err(all_failed(&[&long])), 5);
        assert_eq!(row.outcome, Outcome::NotApplicable);
        assert_eq!(row.correct, None);
        assert_eq!(row.top_probability, None);
        assert_eq!(row.latency_ms, 5);
        assert_eq!(row.detail.unwrap().chars().count(), DETAIL_MAX_CHARS);
    }

    #[test]
    fn the_request_pins_the_models_chain_without_fallback() {
        let m = spec("http://127.0.0.1:1");
        let c = noul_case(true);
        let req = request_for(&c, &m);
        assert_eq!(req.capability, Capability::Decision);
        assert_eq!(req.chain.as_deref(), Some("bench-nimble-v3"));
        assert!(!req.allow_fallback);
        assert!(req.model.is_none() && req.router.is_none());
        match &req.payload {
            Payload::Decision {
                state,
                questions,
                images,
                keep_alive,
            } => {
                assert_eq!(state, &c.state);
                assert_eq!(questions.len(), 1);
                assert_eq!(questions["q"], c.question);
                assert!(images.is_empty() && keep_alive.is_none());
            }
            other => panic!("not a decision payload: {other:?}"),
        }
    }

    // --- breaker / run ----------------------------------------------------

    /// The builder's breaker is not readable, so this proves it by behaviour:
    /// the default threshold (5) would open the breaker and the later calls
    /// would never reach the server.
    #[tokio::test]
    async fn every_call_reaches_the_server_after_many_failures() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500).set_body_string("down"))
            .mount(&server)
            .await;
        let m = spec(&server.uri());
        let cfg = BenchConfig {
            models: vec![m.clone()],
        };
        let gw = facade_for(&cfg, &m).unwrap().build().await.gateway;
        let req = request_for(&noul_case(true), &m);
        let mut seen = 0;
        for i in 0..20 {
            let err = gw.execute(&req).await.expect_err("server answers 500");
            let now = server.received_requests().await.unwrap().len();
            assert!(now > seen, "call {i} never reached the server: {err}");
            seen = now;
            assert_eq!(classify(&err).0, Outcome::Error, "call {i}: {err}");
        }
    }

    #[tokio::test]
    async fn run_yields_one_row_per_case_and_model_after_a_discarded_warm_up() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "model": "nimble-v3",
                "answers": {"q": {"type": "noul", "noul": 0.9}},
                "usage": {"input_tokens": 30, "output_tokens": 0}
            })))
            .mount(&server)
            .await;
        let mut a = spec(&server.uri());
        a.id = "a".into();
        let mut b = spec(&server.uri());
        b.id = "b".into();
        let cfg = BenchConfig { models: vec![a, b] };
        let mut c2 = noul_case(false);
        c2.id = "c2".into();
        let cases = vec![noul_case(true), c2];
        // Selection order is ignored: rows follow config order.
        let rows = run(&cfg, &cases, &["b".into(), "a".into()]).await.unwrap();
        let got: Vec<_> = rows
            .iter()
            .map(|r| (r.model.as_str(), r.case_id.as_str(), r.correct))
            .collect();
        assert_eq!(
            got,
            vec![
                ("a", "c1", Some(true)),
                ("a", "c2", Some(false)),
                ("b", "c1", Some(true)),
                ("b", "c2", Some(false)),
            ]
        );
        assert!(rows.iter().all(|r| r.outcome == Outcome::Ok));
        // 2 models × (1 warm-up + 2 cases).
        assert_eq!(server.received_requests().await.unwrap().len(), 6);
    }

    #[tokio::test]
    async fn an_unknown_model_id_is_an_error_naming_it() {
        let cfg = BenchConfig {
            models: vec![spec("http://127.0.0.1:1")],
        };
        let err = run(&cfg, &[noul_case(true)], &["nope".into()])
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("nope"), "{err}");
    }

    #[tokio::test]
    async fn an_empty_selection_runs_nothing() {
        let cfg = BenchConfig {
            models: vec![spec("http://127.0.0.1:1")],
        };
        assert!(run(&cfg, &[noul_case(true)], &[]).await.unwrap().is_empty());
    }

    // --- results file -----------------------------------------------------

    #[test]
    fn results_are_written_atomically_with_stable_field_names() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("results.json");
        std::fs::write(&path, "stale").unwrap();
        let results = Results {
            generated_at: 1_760_000_000,
            manifest: None,
            not_run: vec![],
            roster: vec![RosterEntry {
                id: "m".into(),
                router: "llamacpp".into(),
                api_model_id: "nimble-v3".into(),
                context_window: 8192,
            }],
            labels_reviewed: vec![LabelReview {
                scenario: Scenario::S4FixCheck,
                reviewed_by: None,
            }],
            completed: vec!["m".into()],
            paths: None,
            rows: vec![row_for(
                &noul_case(true),
                "m",
                &Err(gated(Some(HumanAction::UseLargerContextWindow))),
                7,
            )],
        };
        write_results(&path, &results).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        let v: serde_json::Value = serde_json::from_str(&text).unwrap();
        let mut keys: Vec<_> = v.as_object().unwrap().keys().cloned().collect();
        keys.sort();
        assert_eq!(
            keys,
            [
                "completed",
                "generated_at",
                "labels_reviewed",
                "manifest",
                "not_run",
                "paths",
                "roster",
                "rows"
            ]
        );
        let mut row_keys: Vec<_> = v["rows"][0].as_object().unwrap().keys().cloned().collect();
        row_keys.sort();
        assert_eq!(
            row_keys,
            [
                "answer",
                "case_id",
                "correct",
                "detail",
                "expected_level",
                "latency_ms",
                "model",
                "outcome",
                "probabilities",
                "reached_server",
                "scenario",
                "score_value",
                "top_probability"
            ]
        );
        assert_eq!(v["rows"][0]["outcome"], "not_applicable");
        let back: Results = serde_json::from_str(&text).unwrap();
        assert_eq!(back, results);
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(leftovers, vec![std::ffi::OsString::from("results.json")]);
    }

    /// Results written before T7 have no roster / labels_reviewed: they still
    /// parse, with both empty.
    #[test]
    fn a_results_file_without_roster_or_labels_still_parses() {
        let back: Results = serde_json::from_value(json!({
            "generated_at": 1,
            "manifest": null,
            "not_run": [],
            "rows": [],
        }))
        .unwrap();
        assert!(back.roster.is_empty());
        assert!(back.labels_reviewed.is_empty());
        assert!(back.completed.is_empty());
    }

    /// A2: rows written before `answer` / `probabilities` existed still parse.
    #[test]
    fn a_row_without_answer_or_probabilities_still_parses() {
        let row: ResultRow = serde_json::from_value(json!({
            "case_id": "c1", "scenario": "s3_entailment", "model": "m",
            "outcome": "ok", "correct": true, "top_probability": 0.9,
            "score_value": null, "expected_level": null, "latency_ms": 3,
            "reached_server": true, "detail": null
        }))
        .unwrap();
        assert_eq!((row.answer, row.probabilities), (None, None));
    }

    // --- T4 review ---------------------------------------------------------

    /// A listener that accepts each connection and drops it at once, so every
    /// call is a transport (`Network`) failure. Returns its URL and the count
    /// of connections it accepted.
    fn accept_and_drop() -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let accepted = std::sync::Arc::new(AtomicUsize::new(0));
        let counter = accepted.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                counter.fetch_add(1, Ordering::SeqCst);
                drop(stream);
            }
        });
        (url, accepted)
    }

    fn noul_cases(n: usize) -> Vec<Case> {
        (0..n)
            .map(|i| {
                let mut c = noul_case(true);
                c.id = format!("c{i}");
                c
            })
            .collect()
    }

    /// C1: one transport failure must not cool the router for the rest of the
    /// run — every case (and the warm-up) reaches the server.
    #[tokio::test]
    async fn a_transport_failure_does_not_cool_the_router_for_later_cases() {
        let (url, accepted) = accept_and_drop();
        let m = spec(&url);
        let cfg = BenchConfig {
            models: vec![m.clone()],
        };
        let rows = run(&cfg, &noul_cases(5), std::slice::from_ref(&m.id))
            .await
            .unwrap();
        assert_eq!(rows.len(), 5);
        for r in &rows {
            assert_eq!(r.outcome, Outcome::Error, "{r:?}");
            // Dropped unanswered: no reply, so not a result of the model (A1).
            assert!(!r.reached_server, "{r:?}");
        }
        assert_eq!(
            accepted.load(std::sync::atomic::Ordering::SeqCst),
            6,
            "warm-up + 5 cases must each reach the server; rows: {rows:?}"
        );
    }

    /// SP-DEC-4 review A1: a refused connection sends nothing, so its rows
    /// never reached the server, and the report says so instead of ranking a
    /// dead port at accuracy 0.00 with no warning.
    #[tokio::test]
    async fn a_run_against_a_closed_port_never_reaches_the_server() {
        let m = spec("http://127.0.0.1:9");
        let cfg = BenchConfig {
            models: vec![m.clone()],
        };
        let rows = run(&cfg, &noul_cases(3), std::slice::from_ref(&m.id))
            .await
            .unwrap();
        assert_eq!(rows.len(), 3);
        for r in &rows {
            assert_eq!(r.outcome, Outcome::Error, "{r:?}");
            assert!(
                !r.reached_server,
                "a refused connection reached nothing: {r:?}"
            );
        }
        let results = Results {
            generated_at: 0,
            manifest: None,
            not_run: vec![],
            roster: vec![RosterEntry::from(&m)],
            labels_reviewed: vec![],
            completed: vec![m.id.clone()],
            paths: None,
            rows,
        };
        let md = crate::report::render(&results, 7);
        let never = md
            .split("## Rows that never reached the server")
            .nth(1)
            .unwrap_or_else(|| panic!("no never-reached section:\n{md}"));
        assert!(
            never.contains("nimble-v3 on S3: 3 rows never reached the server (3 error)"),
            "{never}"
        );
    }

    /// A1: the marker is what the kernel makes of a real reqwest connect
    /// failure, so a kernel or reqwest wording change fails here, not
    /// silently in a run.
    #[tokio::test]
    async fn the_network_send_marker_matches_the_kernel() {
        let refused = reqwest::Client::new()
            .post("http://127.0.0.1:9/v1/systemone")
            .send()
            .await
            .expect_err("nothing listens on port 9");
        let text = GatewayError::Network(refused).to_string();
        assert!(text.starts_with(NETWORK_SEND_MARKER), "{text}");
    }

    /// A1: a case that runs out the router timeout was sent and the server
    /// was working on it, so it reached the server — although the kernel
    /// renders the timeout with the same text as a refused connection.
    #[tokio::test]
    async fn a_case_that_times_out_reached_the_server() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(5)))
            .mount(&server)
            .await;
        let m = spec(&server.uri());
        let cfg = BenchConfig {
            models: vec![m.clone()],
        };
        let timeout = Duration::from_millis(300);
        let gw = gateway_with_timeout(&cfg, &m, timeout).await.unwrap();
        let case = noul_case(true);
        let result = gw.execute(&request_for(&case, &m)).await;
        let row = row_for_model(&case, &m, &result, 300, timeout);
        assert_eq!(row.outcome, Outcome::Error, "{row:?}");
        assert!(
            row.detail.as_deref().unwrap().contains(NETWORK_SEND_MARKER),
            "{row:?}"
        );
        assert!(row.reached_server, "{row:?}");
    }

    /// C1: a 429 without `Retry-After` must not lock the model out for the
    /// rest of the run.
    #[tokio::test]
    async fn a_rate_limit_without_retry_after_does_not_lock_later_cases_out() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(429).set_body_string("slow down"))
            .mount(&server)
            .await;
        let m = spec(&server.uri());
        let cfg = BenchConfig {
            models: vec![m.clone()],
        };
        let rows = run(&cfg, &noul_cases(4), std::slice::from_ref(&m.id))
            .await
            .unwrap();
        assert!(rows.iter().all(|r| r.reached_server), "{rows:?}");
        assert_eq!(server.received_requests().await.unwrap().len(), 5);
    }

    /// The one lock the bench cannot turn off: a real `Retry-After` is honoured
    /// verbatim. The skipped cases are not dropped or disguised — each is an
    /// `Error` row with `reached_server: false`.
    #[tokio::test]
    async fn a_real_retry_after_still_gates_and_the_rows_say_so() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(429)
                    .insert_header("retry-after", "120")
                    .set_body_string("slow down"),
            )
            .mount(&server)
            .await;
        let m = spec(&server.uri());
        let cfg = BenchConfig {
            models: vec![m.clone()],
        };
        let rows = run(&cfg, &noul_cases(3), std::slice::from_ref(&m.id))
            .await
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            1,
            "warm-up only"
        );
        for r in &rows {
            assert_eq!(r.outcome, Outcome::Error, "{r:?}");
            assert!(!r.reached_server, "{r:?}");
        }
    }

    /// I1: a gate skip (here a cooling router) never reached the server.
    #[test]
    fn a_gated_row_is_marked_as_not_reaching_the_server() {
        let err = GatewayError::AllGated {
            resume_after: None,
            skipped: vec!["llamacpp:nimble-v3 — router cooling down".into()],
            human_action: None,
        };
        let row = row_for(&noul_case(true), "m", &Err(err), 0);
        assert_eq!(row.outcome, Outcome::Error);
        assert!(!row.reached_server);
        assert!(row.detail.unwrap().contains("cooling"));
        let na = row_for(
            &noul_case(true),
            "m",
            &Err(gated(Some(HumanAction::UseLargerContextWindow))),
            0,
        );
        assert_eq!(na.outcome, Outcome::NotApplicable);
        assert!(!na.reached_server);
    }

    /// The engine also answers `AllGated` AFTER a dispatched attempt: a 429
    /// adds a timed gate contribution, and the single candidate is then not in
    /// `skipped`. That call did reach the server.
    #[test]
    fn an_all_gated_after_an_attempt_reached_the_server() {
        let err = GatewayError::AllGated {
            resume_after: None,
            skipped: vec![],
            human_action: None,
        };
        let row = row_for(&noul_case(true), "m", &Err(err), 4);
        assert_eq!(row.outcome, Outcome::Error);
        assert!(row.reached_server);
    }

    #[test]
    fn an_attempted_call_is_marked_as_reaching_the_server() {
        let failed = row_for(
            &noul_case(true),
            "m",
            &Err(all_failed(&[&provider_error_text()])),
            3,
        );
        assert!(failed.reached_server);
        let ok = row_for(
            &noul_case(true),
            "m",
            &Ok(response(json!({"q": {"type": "noul", "noul": 0.9}}))),
            3,
        );
        assert!(ok.reached_server);
        let missing = row_for(&noul_case(true), "m", &Ok(response(json!({}))), 3);
        assert!(missing.reached_server, "the server answered, just not q");
        assert!(!row_for(&noul_case(true), "m", &Err(all_failed(&[])), 0).reached_server);
        assert!(
            !row_for(&noul_case(true), "m", &Err(GatewayError::NotConfigured), 0).reached_server
        );
    }

    /// I2: the callback sees each model once, in config order, with exactly
    /// that model's rows, and the return value is every row.
    #[tokio::test]
    async fn run_with_progress_reports_each_model_once_with_its_rows() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "model": "nimble-v3",
                "answers": {"q": {"type": "noul", "noul": 0.9}},
                "usage": {"input_tokens": 30, "output_tokens": 0}
            })))
            .mount(&server)
            .await;
        let mut a = spec(&server.uri());
        a.id = "a".into();
        let mut b = spec(&server.uri());
        b.id = "b".into();
        let cfg = BenchConfig { models: vec![a, b] };
        let mut seen: Vec<(String, Vec<String>)> = vec![];
        let rows = run_with_progress(
            &cfg,
            &noul_cases(3),
            &["a".into(), "b".into()],
            |model, rows| {
                assert!(rows.iter().all(|r| r.model == model), "{model}: {rows:?}");
                seen.push((
                    model.into(),
                    rows.iter().map(|r| r.case_id.clone()).collect(),
                ));
            },
        )
        .await
        .unwrap();
        let ids = vec!["c0".to_string(), "c1".into(), "c2".into()];
        assert_eq!(seen, vec![("a".into(), ids.clone()), ("b".into(), ids)]);
        assert_eq!(rows.len(), 6);
    }

    // --- run_saving ---------------------------------------------------------

    fn base_results() -> Results {
        Results {
            generated_at: 1,
            manifest: None,
            not_run: vec![NotRun {
                model: "down".into(),
                reason: "connection refused".into(),
            }],
            roster: vec![],
            labels_reviewed: vec![],
            completed: vec![],
            paths: None,
            rows: vec![],
        }
    }

    fn read_results(path: &Path) -> Results {
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
    }

    async fn noul_server() -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "model": "nimble-v3",
                "answers": {"q": {"type": "noul", "noul": 0.9}},
                "usage": {"input_tokens": 30, "output_tokens": 0}
            })))
            .mount(&server)
            .await;
        server
    }

    /// A run that fails on model 2 leaves model 1's rows on disk.
    #[tokio::test]
    async fn a_run_failing_on_the_second_model_keeps_the_first_models_rows() {
        let server = noul_server().await;
        let mut first = spec(&server.uri());
        first.id = "first".into();
        let mut broken = spec("");
        broken.id = "broken".into();
        let cfg = BenchConfig {
            models: vec![first, broken],
        };
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("results.json");
        let err = run_saving(
            &cfg,
            &noul_cases(3),
            &["first".into(), "broken".into()],
            base_results(),
            &out,
        )
        .await
        .unwrap_err();
        assert!(format!("{err:#}").contains("broken"), "{err:#}");
        let on_disk = read_results(&out);
        assert_eq!(on_disk.rows.len(), 3, "{on_disk:?}");
        assert!(on_disk.rows.iter().all(|r| r.model == "first"));
        // T7 review I2: the file says which models finished.
        assert_eq!(on_disk.completed, ["first"]);
        assert_eq!(on_disk.not_run, base_results().not_run);
    }

    #[tokio::test]
    async fn run_saving_writes_and_returns_every_row() {
        let server = noul_server().await;
        let mut a = spec(&server.uri());
        a.id = "a".into();
        let mut b = spec(&server.uri());
        b.id = "b".into();
        let cfg = BenchConfig { models: vec![a, b] };
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("results.json");
        let got = run_saving(
            &cfg,
            &noul_cases(2),
            &["a".into(), "b".into()],
            base_results(),
            &out,
        )
        .await
        .unwrap();
        assert_eq!(got.rows.len(), 4);
        assert_eq!(got.completed, ["a", "b"]);
        assert_eq!(read_results(&out), got);
    }

    #[tokio::test]
    async fn with_no_model_to_run_the_file_still_records_the_not_run_ones() {
        let cfg = BenchConfig { models: vec![] };
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("results.json");
        run_saving(&cfg, &noul_cases(2), &[], base_results(), &out)
            .await
            .unwrap();
        assert_eq!(read_results(&out), base_results());
    }

    // --- T7 review I1: redaction ----------------------------------------------

    const SECRET_URL: &str = "http://127.0.0.1:9/?token=SECRETTOK";

    #[test]
    fn redact_removes_the_model_url_and_every_url_segment() {
        let m = spec(SECRET_URL);
        let text = format!(
            "error sending request for url (http://127.0.0.1:9/?token=SECRETTOK/v1/systemone) \
             at {SECRET_URL} or url (http://other/?k=SECRETTOK"
        );
        let out = redact(&text, &m);
        assert!(!out.contains("SECRETTOK"), "{out}");
        assert_eq!(
            out,
            "error sending request for url (<url>) at <url> or url (<url>"
        );
        assert_eq!(redact("no urls here", &m), "no urls here");
    }

    #[tokio::test]
    async fn row_details_and_the_results_file_never_carry_the_model_url() {
        let m = spec(SECRET_URL);
        let cfg = BenchConfig {
            models: vec![m.clone()],
        };
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("results.json");
        let got = run_saving(
            &cfg,
            &noul_cases(2),
            std::slice::from_ref(&m.id),
            base_results(),
            &out,
        )
        .await
        .unwrap();
        assert!(got.rows.iter().all(|r| r.outcome == Outcome::Error));
        assert!(got.rows.iter().all(|r| r.detail.is_some()));
        let text = std::fs::read_to_string(&out).unwrap();
        assert!(!text.contains("SECRETTOK"), "{text}");
        assert!(text.contains("<url>"), "{text}");
    }

    /// M4: the warm-up is the cheapest case, not whichever comes first.
    #[test]
    fn the_warm_up_is_the_case_with_the_shortest_state() {
        let mut long = noul_case(true);
        long.id = "long".into();
        long.state = json!("x".repeat(500));
        let mut short = noul_case(true);
        short.id = "short".into();
        short.state = json!("tiny");
        let mut mid = noul_case(true);
        mid.id = "mid".into();
        mid.state = json!("x".repeat(50));
        let cases = [long, short, mid];
        assert_eq!(warm_up_case(&cases).unwrap().id, "short");
        assert!(warm_up_case(&[]).is_none());
    }

    /// M2: probabilities are range- and NaN-checked like noul.
    #[test]
    fn a_choice_or_score_probability_outside_zero_to_one_is_an_err() {
        assert!(
            score(
                &choice_case("a"),
                &choice("a", &[("a", f64::NAN), ("b", 0.1)])
            )
            .is_err()
        );
        assert!(score(&choice_case("a"), &choice("a", &[("a", 0.9), ("b", 1.5)])).is_err());
        assert!(score(&choice_case("a"), &choice("a", &[("a", 0.9), ("b", -0.1)])).is_err());
        assert!(score(&score_case(0), &score_answer(0.0, &[0.5, f64::NAN, 0.1])).is_err());
        assert!(score(&score_case(0), &score_answer(0.0, &[2.0, 0.0, 0.0])).is_err());
    }

    /// M3: an argmax level the question does not have is an Err.
    #[test]
    fn a_score_argmax_level_beyond_the_criteria_is_an_err() {
        // score_case has 3 levels (0..=2); level 3 wins here.
        let e = score(&score_case(2), &score_answer(3.0, &[0.0, 0.1, 0.2, 0.7])).unwrap_err();
        assert!(e.to_string().contains("level 3"), "{e}");
    }
}
