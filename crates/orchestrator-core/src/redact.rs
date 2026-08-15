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
        ];
        let whole = whole_patterns
            .iter()
            .map(|p| Regex::new(p).expect("static redaction pattern compiles"))
            .collect();
        let whole_set =
            regex::RegexSet::new(whole_patterns).expect("static redaction patterns compile");
        let assignment = Regex::new(
            r#"(?i)(api[_-]?key|secret|token|password|passwd)("?\s*[=:]\s*"?)([^\s"',&;]{6,})"#,
        )
        .expect("static redaction pattern compiles");
        Self {
            whole,
            whole_set,
            assignment,
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
}

impl Redactor for PatternRedactor {
    fn redact(&self, value: &Value) -> Value {
        match value {
            Value::String(s) => Value::String(self.redact_str(s)),
            Value::Array(a) => Value::Array(a.iter().map(|v| self.redact(v)).collect()),
            // Redact string VALUES; leave object KEYS as-is (a secret-shaped key is
            // structural, not a leaked credential value).
            Value::Object(o) => {
                Value::Object(o.iter().map(|(k, v)| (k.clone(), self.redact(v))).collect())
            }
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
