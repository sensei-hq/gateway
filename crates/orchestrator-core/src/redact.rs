//! Secret redaction (SP-4 slice 2). A pure, replay-stable scrub of effect outputs
//! applied before they are journaled or fed back to the agent (design §4).

use regex::Regex;
use serde_json::Value;

/// Redact secrets from an effect output. MUST be pure (replay-stable) — no I/O,
/// clock, or RNG — since the redacted value is BOTH journaled and fed to the agent,
/// so a resume must reproduce it identically.
pub trait Redactor: Send + Sync {
    fn redact(&self, value: &Value) -> Value;
}

/// The fixed, type-agnostic placeholder (discloses nothing about the secret).
const PLACEHOLDER: &str = "[REDACTED]";

/// The secret-KEY shape, shared by the two rules that need it: the in-string
/// `assignment` form (`token=…`) and the JSON-object form (`{"token": "…"}`).
/// ONE source of truth so the two can never drift apart.
const SECRET_KEY_ALTERNATION: &str = r"api[_-]?key|secret|token|password|passwd";

/// Minimum length of a string value under a secret-shaped key before it is replaced.
/// The same floor the `assignment` rule applies to its value (`{6,}`): shorter than
/// this and it is a flag or a status word, not a credential.
const MIN_KEYED_VALUE_LEN: usize = 6;

/// Pattern-based redactor: replaces substrings matching curated secret-SHAPE
/// patterns with `[REDACTED]`. Best-effort by shape (design §4.4 — misses novel
/// formats). ReDoS-safe: the `regex` crate uses finite automata (no backtracking),
/// so scanning adversarial tool output is linear-time.
pub struct PatternRedactor {
    /// Whole-match patterns → the entire match becomes the placeholder.
    whole: Vec<Regex>,
    /// A `RegexSet` over the SAME whole-pattern strings — a single linear scan to
    /// gate the per-pattern replace passes (clean input skips all the allocations).
    whole_set: regex::RegexSet,
    /// `key = value` form → only the value (capture group 3) is redacted.
    assignment: Regex,
    /// A JSON-object KEY whose string value is therefore a credential. See
    /// [`PatternRedactor::redact_keyed`].
    secret_key: Regex,
}

impl Default for PatternRedactor {
    fn default() -> Self {
        // open lower bounds are intentional — recall over precision for a security scrub
        let whole_patterns = [
            r"sk-[A-Za-z0-9_-]{20,}",
            r"sk_live_[A-Za-z0-9]{20,}",
            r"rk_live_[A-Za-z0-9]{20,}",
            r"AKIA[0-9A-Z]{16}",
            r"gh[opsru]_[A-Za-z0-9]{30,}",
            r"github_pat_[A-Za-z0-9_]{22,}",
            r"xox[baprs]-[A-Za-z0-9-]{10,}",
            r"AIza[0-9A-Za-z_-]{30,}",
            r"(?i)bearer\s+[A-Za-z0-9._-]{8,}",
            r"-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----",
            // URL userinfo: `scheme://user:password@` — a connection string carries the
            // password in the clear and matches no vendor prefix. The match runs from the
            // scheme to the LAST `@` of the userinfo, so the host/path survives (same
            // shape `torii::errors::redact_url` keeps) but nothing of the password does.
            //
            // The password class deliberately permits `:` and `@` and (all but the final
            // character) `/`, because the bug this closes twice over is a password that
            // CONTAINS a delimiter: `pw@word`, `p@ss://word`, or a base64 password with a
            // `/` in it. A class that stopped at the first `@` would leave the tail of
            // such a password in the clear, which is exactly how `redact_url` leaked
            // before `rsplit_once('@')` fixed it. The one excluded position — a `/`
            // immediately before the terminating `@` — is what keeps a legitimate
            // `https://host:port/@scope/pkg` from being swallowed.
            r"(?i)[a-z][a-z0-9+.-]*://[^/\s:@]+:[^\s]*[^/\s]@",
        ];
        let whole = whole_patterns
            .iter()
            .map(|p| Regex::new(p).expect("static redaction pattern compiles"))
            .collect();
        let whole_set =
            regex::RegexSet::new(whole_patterns).expect("static redaction patterns compile");
        let assignment = Regex::new(&format!(
            r#"(?i)({SECRET_KEY_ALTERNATION})("?\s*[=:]\s*"?)([^\s"',&;]{{6,}})"#
        ))
        .expect("static redaction pattern compiles");
        let secret_key = Regex::new(&format!("(?i)(?:{SECRET_KEY_ALTERNATION})"))
            .expect("static redaction pattern compiles");
        Self {
            whole,
            whole_set,
            assignment,
            secret_key,
        }
    }
}

impl PatternRedactor {
    /// Redact one string: the assignment form first (keep key label, redact the
    /// value), then the whole-match patterns.
    fn redact_str(&self, s: &str) -> String {
        let after_assign = self.assignment.replace_all(s, |c: &regex::Captures| {
            format!("{}{}{PLACEHOLDER}", &c[1], &c[2])
        });
        if !self.whole_set.is_match(&after_assign) {
            return after_assign.into_owned(); // common case: at most one alloc
        }
        let mut out = after_assign.into_owned();
        for re in &self.whole {
            out = re.replace_all(&out, PLACEHOLDER).into_owned();
        }
        out
    }

    /// Redact one object MEMBER, using the key as evidence about its own value.
    ///
    /// `{"token": "<secret>"}` and `"token=<secret>"` are the same disclosure; JSON just
    /// puts the two halves into separate [`Value`]s, where the in-string `assignment`
    /// rule — the ONLY rule that catches a secret with no vendor prefix — can never see
    /// them together. Recursing into the value alone therefore lets an unprefixed secret
    /// through into the journal, the CAS blob and the next model prompt, which is what
    /// the SP-6 s1 review measured (`{"password": "…"}` stored verbatim).
    ///
    /// So: secret-shaped key + a string value at least [`MIN_KEYED_VALUE_LEN`] long ⇒ the
    /// WHOLE value goes (the key already said what it is; there is no label worth
    /// keeping, unlike the in-string form). Any other key recurses exactly as before.
    ///
    /// **The key carries into an ARRAY, and must.** `{"tokens": ["<secret>"]}` is the
    /// plural an operator writes for more than one value, and its elements are bare
    /// strings with nothing for the in-string rules to match — the same disclosure, one
    /// array deep. Recursing with [`redact`](Redactor::redact) there dropped the key, so
    /// the evidence was gone by the time the element was reached.
    ///
    /// A nested OBJECT is deliberately NOT carried into: its members bring their own keys,
    /// and a subtree under `"secret"` is not wholly secret — it is still walked
    /// leaf-by-leaf rather than flattened to the placeholder.
    fn redact_keyed(&self, key: &str, value: &Value) -> Value {
        match value {
            Value::String(s)
                if s.chars().count() >= MIN_KEYED_VALUE_LEN && self.secret_key.is_match(key) =>
            {
                Value::String(PLACEHOLDER.to_string())
            }
            Value::Array(a) => Value::Array(a.iter().map(|v| self.redact_keyed(key, v)).collect()),
            other => self.redact(other),
        }
    }
}

impl Redactor for PatternRedactor {
    fn redact(&self, value: &Value) -> Value {
        match value {
            Value::String(s) => Value::String(self.redact_str(s)),
            Value::Array(a) => Value::Array(a.iter().map(|v| self.redact(v)).collect()),
            // Redact string VALUES; leave object KEYS as-is (a secret-shaped key is
            // structural, not a leaked credential value) — but DO read the key as
            // evidence about its own value (see `redact_keyed`).
            Value::Object(o) => Value::Object(
                o.iter()
                    .map(|(k, v)| (k.clone(), self.redact_keyed(k, v)))
                    .collect(),
            ),
            other => other.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn redacts(s: &str) -> bool {
        PatternRedactor::default()
            .redact(&json!(s))
            .as_str()
            .unwrap()
            .contains("[REDACTED]")
    }

    /// A one-member object with a RUNTIME key (the `json!` macro wants a literal).
    fn obj(key: &str, value: Value) -> Value {
        let mut m = serde_json::Map::new();
        m.insert(key.to_string(), value);
        Value::Object(m)
    }

    /// Test secrets are assembled at RUNTIME from harmless parts: a credential-shaped
    /// literal trips the repo's secret scanner, and this module's entire subject is
    /// credential shapes.
    fn passphrase() -> String {
        ["hunter2", "hunter2", "hunter2"].concat()
    }

    /// A bare 32-hex secret — no vendor prefix, so ONLY its key or its `=` can betray it.
    fn unprefixed_secret() -> String {
        ["9f2b7c1e", "4a8d3b5f", "6e0c2a7d", "9b4f1e8c"].concat()
    }

    #[test]
    fn scrubs_each_secret_class() {
        assert!(redacts("sk-abcdefghijklmnopqrstuvwx"), "OpenAI");
        assert!(redacts("sk-ant-abcdefghijklmnopqrstuvwx"), "Anthropic");
        assert!(redacts("AKIAIOSFODNN7EXAMPLE"), "AWS");
        assert!(
            redacts("ghp_0123456789abcdefghijklmnopqrstuvwxyz"),
            "GitHub PAT"
        );
        assert!(redacts("xoxb-1234567890-abcdefghij"), "Slack");
        assert!(redacts("AIza0123456789abcdefghijklmnopqrstuvwxy"), "Google");
        assert!(redacts("Bearer abcdef1234567890"), "bearer");
        assert!(
            redacts("-----BEGIN RSA PRIVATE KEY-----\nMIIabc\n-----END RSA PRIVATE KEY-----"),
            "PEM"
        );
    }

    #[test]
    fn assignment_form_redacts_only_the_value() {
        let out = PatternRedactor::default().redact(&json!("api_key=supersecretvalue"));
        let s = out.as_str().unwrap();
        assert!(s.contains("api_key="), "keeps the key label: {s}");
        assert!(s.contains("[REDACTED]"), "redacts the value: {s}");
        assert!(!s.contains("supersecretvalue"), "value gone: {s}");
    }

    #[test]
    fn clean_strings_are_untouched() {
        for clean in ["hello world", "a-short-id", "the quick brown fox", "1234"] {
            let out = PatternRedactor::default().redact(&json!(clean));
            assert_eq!(
                out.as_str().unwrap(),
                clean,
                "clean string changed: {clean}"
            );
        }
    }

    #[test]
    fn walks_nested_json_leaves_only() {
        let v = json!({
            "a": { "b": ["sk-abcdefghijklmnopqrstuvwx", "clean"] },
            "AKIAIOSFODNN7EXAMPLE": "AKIAIOSFODNN7EXAMPLE",
            "n": 42,
            "ok": true
        });
        let out = PatternRedactor::default().redact(&v);
        assert_eq!(out["a"]["b"][0], json!("[REDACTED]"));
        assert_eq!(out["a"]["b"][1], json!("clean"));
        assert!(
            out.get("AKIAIOSFODNN7EXAMPLE").is_some(),
            "key not rewritten"
        );
        assert_eq!(out["AKIAIOSFODNN7EXAMPLE"], json!("[REDACTED]"));
        assert_eq!(out["n"], json!(42));
        assert_eq!(out["ok"], json!(true));
    }

    #[test]
    fn is_pure_deterministic() {
        let r = PatternRedactor::default();
        let v = json!({ "t": "Bearer abcdef1234567890", "x": "clean" });
        assert_eq!(r.redact(&v), r.redact(&v));
    }

    #[test]
    fn scrubs_modern_and_added_secret_shapes() {
        // Live-key-shaped fixtures are assembled from parts so secret scanners
        // don't flag the test source itself — the redactor proves the match.
        let stripe = ["sk", "live", "1234567890abcdefghijklmno"].join("_");
        let restripe = ["rk", "live", "1234567890abcdefghijklmno"].join("_");
        let cases = [
            "sk-proj-aB3cD4eF5gH6iJ7kL8mN9oP0qR1sT2u", // OpenAI project key
            "gho_1234567890abcdefghijklmnopqrstuvwx",  // GitHub OAuth
            "ghs_1234567890abcdefghijklmnopqrstuvwx",  // GitHub app
            "github_pat_11ABCDEFG0abcdefghijklmnop",   // GitHub fine-grained PAT
            stripe.as_str(),                           // Stripe live secret key
            restripe.as_str(),                         // Stripe live restricted key
        ];
        for s in cases {
            assert!(redacts(s), "should redact {s}");
        }
    }

    // ---- SP-6 s1 review, Critical: the JSON-object form of an unprefixed secret ----

    /// The reviewer's proof pair: same key, same secret, differing only in STRUCTURE.
    /// The flat form was already redacted; the object form — the one
    /// `torii run signal --payload` documents — was journaled verbatim.
    #[test]
    fn the_structural_pair_redacts_both_ways() {
        let r = PatternRedactor::default();
        let secret = unprefixed_secret();

        let flat = r.redact(&json!(format!("token={secret}")));
        assert!(
            !flat.as_str().unwrap().contains(&secret),
            "flat form leaked: {flat}"
        );

        let structured = r.redact(&obj("token", json!(secret)));
        assert_eq!(structured["token"], json!(PLACEHOLDER));
        assert!(
            !structured.to_string().contains(&secret),
            "object form leaked: {structured}"
        );
    }

    #[test]
    fn secret_shaped_object_keys_redact_their_string_value() {
        let r = PatternRedactor::default();
        let pw = passphrase();
        for key in [
            "password",
            "passwd",
            "token",
            "secret",
            "api_key",
            "api-key",
            "apikey",
            // the alternation is a SUBSTRING match, so the real-world compound keys
            // are covered too (recall over precision, as everywhere in this module)
            "client_secret",
            "access_token",
            "db_password",
        ] {
            let out = r.redact(&obj(key, json!(pw)));
            assert_eq!(out[key], json!(PLACEHOLDER), "key {key:?} not redacted");
        }
    }

    #[test]
    fn secret_shaped_object_keys_are_case_insensitive() {
        let r = PatternRedactor::default();
        let pw = passphrase();
        for key in ["Password", "API_KEY", "Token", "SECRET", "ApiKey", "PASSWD"] {
            let out = r.redact(&obj(key, json!(pw)));
            assert_eq!(out[key], json!(PLACEHOLDER), "key {key:?} not redacted");
        }
    }

    #[test]
    fn secret_shaped_key_with_a_non_string_value_is_walked_not_mangled() {
        let r = PatternRedactor::default();
        let pw = passphrase();
        for v in [json!(42), Value::Null, json!(true), json!(1.5)] {
            assert_eq!(
                r.redact(&obj("token", v.clone())),
                obj("token", v.clone()),
                "non-string value changed: {v}"
            );
        }
        // a nested object under a secret key still recurses leaf-by-leaf rather than
        // collapsing the whole subtree to the placeholder
        let out = r.redact(&obj(
            "secret",
            json!({ "kind": "oauth", "password": pw, "expires_in": 3600 }),
        ));
        assert_eq!(out["secret"]["kind"], json!("oauth"));
        assert_eq!(out["secret"]["password"], json!(PLACEHOLDER));
        assert_eq!(out["secret"]["expires_in"], json!(3600));
        // …and so does an array (key matches, value is not a string)
        let out = r.redact(&obj("tokens", json!([{ "password": pw }, "clean"])));
        assert_eq!(out["tokens"][0]["password"], json!(PLACEHOLDER));
        assert_eq!(out["tokens"][1], json!("clean"));
    }

    /// The key is evidence about the whole SUBTREE of strings it introduces, not just
    /// about a string sitting directly under it.
    ///
    /// An array under a secret-shaped key is the plural an operator naturally writes for
    /// more than one value (`"tokens"`, `"api_keys"`), and its elements are bare strings
    /// with nothing in them for the in-string rules to match — the same disclosure the
    /// object rule exists to catch, one array deep. Recursing with `redact` there dropped
    /// the key, so the evidence was gone by the time the element was reached and an
    /// unprefixed credential went to the journal, the CAS blob and the next model prompt
    /// verbatim.
    ///
    /// A nested OBJECT still recurses on its own keys (asserted above): its members carry
    /// their own evidence and a subtree under `"secret"` is not wholly secret.
    #[test]
    fn a_secret_shaped_key_carries_into_the_array_it_introduces() {
        let r = PatternRedactor::default();
        let secret = unprefixed_secret();

        for key in ["tokens", "api_keys", "passwords"] {
            let out = r.redact(&obj(key, json!([secret, "clean"])));
            assert_eq!(
                out[key][0],
                json!(PLACEHOLDER),
                "a bare string under {key:?} leaked"
            );
            assert!(
                !out.to_string().contains(&secret),
                "the secret survived somewhere under {key:?}: {out}"
            );
            assert_eq!(
                out[key][1],
                json!("clean"),
                "a short value stays below the {MIN_KEYED_VALUE_LEN}-char floor"
            );
        }

        // Nested arrays carry it too — the evidence does not stop at the first level.
        let out = r.redact(&obj("token", json!([[secret]])));
        assert_eq!(out["token"][0][0], json!(PLACEHOLDER));

        // And a NON-secret key is unaffected: this must not become "redact every array".
        let out = r.redact(&obj("notes", json!([secret])));
        assert_eq!(
            out["notes"][0],
            json!(secret),
            "an ordinary key must not gain the keyed rule"
        );
    }

    #[test]
    fn keyed_value_under_the_length_floor_is_kept() {
        let r = PatternRedactor::default();
        for short in ["", "no", "abc", "12345"] {
            assert_eq!(
                r.redact(&obj("password", json!(short))),
                obj("password", json!(short)),
                "short value changed: {short:?}"
            );
        }
        let six = "123456";
        assert_eq!(
            r.redact(&obj("password", json!(six)))["password"],
            json!(PLACEHOLDER),
            "the floor is {MIN_KEYED_VALUE_LEN} chars — {six:?} is at it"
        );
    }

    #[test]
    fn secret_keys_are_caught_at_every_depth() {
        let r = PatternRedactor::default();
        let pw = passphrase();
        let v = json!({
            "outer": { "inner": { "password": pw } },
            "list": [ { "api_key": pw }, { "decision": "approved" } ],
            "matrix": [[{ "token": pw }]],
        });
        let out = r.redact(&v);
        assert_eq!(out["outer"]["inner"]["password"], json!(PLACEHOLDER));
        assert_eq!(out["list"][0]["api_key"], json!(PLACEHOLDER));
        assert_eq!(out["list"][1]["decision"], json!("approved"));
        assert_eq!(out["matrix"][0][0]["token"], json!(PLACEHOLDER));
        assert!(!out.to_string().contains(&pw), "leaked somewhere: {out}");
    }

    /// The overwhelmingly common signal payload. Over-redaction here would cost the
    /// operator the decision itself, so it is asserted byte-for-byte.
    #[test]
    fn innocuous_approval_payload_is_untouched() {
        let r = PatternRedactor::default();
        let plain = json!({ "decision": "approved" });
        assert_eq!(r.redact(&plain), plain);

        let realistic = json!({
            "decision": "approved",
            "reviewer": "jerry",
            "note": "looks good to me, ship it",
            "attempts": 2,
            "at": "2026-08-24T10:00:00Z",
            "labels": ["release", "reviewed"],
            "blocking": false,
        });
        assert_eq!(r.redact(&realistic), realistic);
    }

    /// The end-to-end shape the review measured landing in a model's system prompt.
    #[test]
    fn approval_payload_keeps_the_decision_and_drops_the_password() {
        let r = PatternRedactor::default();
        let pw = ["hunter2-", "Tr0ub4dor&3-", "CorrectHorse"].concat();
        let out = r.redact(&json!({ "decision": "approved", "password": pw }));
        assert_eq!(out["decision"], json!("approved"));
        assert_eq!(out["password"], json!(PLACEHOLDER));
        assert!(!out.to_string().contains("Tr0ub4dor"), "leaked: {out}");
    }

    // ---- SP-6 s1 review, M4: connection strings ----

    #[test]
    fn connection_string_userinfo_is_redacted() {
        let r = PatternRedactor::default();
        let pw = passphrase();
        for url in [
            format!("postgres://admin:{pw}@db.internal:5432/prod"),
            format!("postgresql://admin:{pw}@db.internal/prod"),
            format!("mysql://root:{pw}@127.0.0.1:3306/app"),
            format!("redis://default:{pw}@cache.internal:6379"),
            format!("https://svc-account:{pw}@api.example.com/v1/things"),
            format!("amqp://guest:{pw}@rabbit:5672/vhost"),
            format!("MONGODB://user:{pw}@mongo.internal:27017/db"),
        ] {
            let out = r.redact(&json!(url));
            let s = out.as_str().unwrap();
            assert!(!s.contains(&pw), "password survived: {s}");
            assert!(s.contains(PLACEHOLDER), "nothing redacted: {s}");
        }
        // the non-secret tail survives, same shape `torii::errors::redact_url` keeps
        let out = r.redact(&json!(format!(
            "postgres://admin:{pw}@db.internal:5432/prod"
        )));
        assert_eq!(
            out.as_str().unwrap(),
            format!("{PLACEHOLDER}db.internal:5432/prod")
        );
    }

    /// The class that made `torii::errors::redact_url` leak until it switched to
    /// `rsplit_once('@')` + `split_once("://")`: a password that CONTAINS a delimiter.
    /// A pattern that stopped at the first `@` would leave the tail in the clear.
    #[test]
    fn connection_string_password_containing_delimiters_is_fully_redacted() {
        let r = PatternRedactor::default();
        let head = "hunter2";
        let tail = "Tr0ub4dor";
        for pw in [
            format!("{head}@{tail}"),          // '@' inside the password
            format!("{head}:{tail}"),          // ':' inside the password
            format!("{head}://{tail}"),        // a whole scheme inside the password
            format!("{head}/{tail}"),          // '/' — the base64 alphabet has one
            format!("{head}+{tail}/x=="),      // base64 padding + '/'
            format!("{head}@{tail}@{head}"),   // several '@'
            format!("{head}@{tail}://{head}"), // both, interleaved
        ] {
            let url = format!("postgres://admin:{pw}@db.internal:5432/prod");
            let s = r.redact(&json!(url)).as_str().unwrap().to_string();
            assert!(
                !s.contains(head),
                "password fragment {head:?} survived: {s}"
            );
            assert!(
                !s.contains(tail),
                "password fragment {tail:?} survived: {s}"
            );
            assert!(s.contains(PLACEHOLDER), "nothing redacted: {s}");
            assert!(s.contains("db.internal"), "over-redacted the host: {s}");
        }
    }

    /// A URL with no userinfo is not a credential — leave it alone, including the
    /// `host:port/@scope/pkg` shape that a greedier password class would swallow.
    #[test]
    fn urls_without_credentials_are_untouched() {
        let r = PatternRedactor::default();
        for url in [
            "https://api.example.com/v1/things",
            "http://registry.local:4873/@scope/pkg",
            "postgres://db.internal:5432/prod",
            "https://user@github.com/org/repo.git",
            "see https://docs.example.com/a:b/c for details",
        ] {
            let out = r.redact(&json!(url));
            assert_eq!(out.as_str().unwrap(), url, "URL changed: {url}");
        }
    }

    // ---- invariants the fold-read path depends on ----

    /// `redact(redact(x)) == redact(x)`. `torii::render::redact_payload` redacts on the
    /// WRITE side and the executor redacts again on the fold-READ side, so a
    /// non-idempotent rule would make live, journaled and replayed values disagree.
    #[test]
    fn redaction_is_idempotent() {
        let r = PatternRedactor::default();
        let pw = passphrase();
        let cases = [
            obj("password", json!(pw)),
            obj("api_key", json!(PLACEHOLDER)), // the placeholder is itself long enough
            json!(format!("postgres://admin:{pw}@db.internal:5432/prod")),
            json!(format!("token={}", unprefixed_secret())),
            json!({ "a": { "b": ["Bearer abcdef1234567890", "clean"] } }),
            json!({ "decision": "approved" }),
            json!(PLACEHOLDER),
        ];
        for v in cases {
            let once = r.redact(&v);
            assert_eq!(r.redact(&once), once, "not idempotent: {v}");
        }
    }

    /// The `regex` crate is finite-automata based (no backtracking) — this pins that
    /// property for the patterns as WRITTEN, including the new greedy userinfo class.
    #[test]
    fn scanning_adversarial_input_stays_linear() {
        let r = PatternRedactor::default();
        let long = "a".repeat(200_000);
        let cases = [
            format!("postgres://admin:{long}"), // userinfo that never closes with '@'
            format!("{}{long}", "a:b@".repeat(20_000)), // many candidate '@' endings
            format!("token={long}"),            // assignment value that never terminates
            format!("Bearer {long}"),
            format!("-----BEGIN RSA PRIVATE KEY-----{long}"), // PEM that never ends
            obj("password", json!(long.clone())).to_string(),
        ];
        let start = std::time::Instant::now();
        for c in &cases {
            let _ = r.redact(&json!(c));
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "redacting adversarial input took {elapsed:?} — a pattern is not linear-time"
        );
    }

    #[test]
    fn assignment_value_stops_at_url_separators() {
        let out =
            PatternRedactor::default().redact(&serde_json::json!("token=abc123secret&next=/home"));
        let s = out.as_str().unwrap();
        assert!(
            s.contains("[REDACTED]") && !s.contains("abc123secret"),
            "value redacted: {s}"
        );
        assert!(
            s.contains("&next=/home"),
            "URL structure after the value is preserved: {s}"
        );
    }
}
