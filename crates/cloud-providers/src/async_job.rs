use std::time::Duration;

use kernel::types::config::RouterConfig;
use kernel::types::error::GatewayError;

/// Default poll interval (seconds) when the operator doesn't override it.
const DEFAULT_POLL_INTERVAL_SECS: u64 = 3;
/// Fallback max-wait ceiling (seconds) used when `RouterConfig.timeout_ms`
/// is unset. Overridable per-router via `timeout_ms`; this const is only a
/// last-resort default, never a hard cap.
const DEFAULT_MAX_WAIT_SECS: u64 = 300;
/// Optional `RouterConfig.headers` hint to override the poll interval, in
/// milliseconds, without touching `timeout_ms`.
const POLL_INTERVAL_HEADER: &str = "x-poll-interval-ms";

/// Configuration for async job polling.
pub struct JobConfig {
    /// How often to check status.
    pub poll_interval: Duration,
    /// Max total wait time before timeout.
    pub max_wait: Duration,
}

impl Default for JobConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(DEFAULT_POLL_INTERVAL_SECS),
            max_wait: Duration::from_secs(DEFAULT_MAX_WAIT_SECS),
        }
    }
}

impl JobConfig {
    /// Derive polling parameters from a [`RouterConfig`] so operators can
    /// tune long-running media jobs (video generation routinely exceeds a
    /// few minutes) without rebuilding the library.
    ///
    /// - `max_wait` follows `config.timeout_ms`, falling back to the
    ///   [`DEFAULT_MAX_WAIT_SECS`] default when `timeout_ms` is unset.
    /// - `poll_interval` stays at the [`DEFAULT_POLL_INTERVAL_SECS`]
    ///   default unless the operator sets the [`POLL_INTERVAL_HEADER`]
    ///   (`x-poll-interval-ms`) hint on the router config. Non-numeric or
    ///   zero values fall back to the default.
    pub fn from_config(config: &RouterConfig) -> Self {
        let max_wait = config
            .timeout_ms
            .map(Duration::from_millis)
            .unwrap_or_else(|| Duration::from_secs(DEFAULT_MAX_WAIT_SECS));
        let poll_interval = config
            .headers
            .get(POLL_INTERVAL_HEADER)
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|ms| *ms > 0)
            .map(Duration::from_millis)
            .unwrap_or_else(|| Duration::from_secs(DEFAULT_POLL_INTERVAL_SECS));
        Self {
            poll_interval,
            max_wait,
        }
    }
}

/// Poll a job until completion or timeout.
///
/// `check_status` returns `Ok(Some(result))` when done, `Ok(None)` when still
/// processing, or `Err` on failure.
pub async fn poll_until_complete<T, F, Fut>(
    config: &JobConfig,
    mut check_status: F,
) -> Result<T, GatewayError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Option<T>, GatewayError>>,
{
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > config.max_wait {
            return Err(GatewayError::Timeout {
                adapter: "video".into(),
                model: String::new(),
                duration_ms: start.elapsed().as_millis() as u64,
            });
        }
        match check_status().await? {
            Some(result) => return Ok(result),
            None => tokio::time::sleep(config.poll_interval).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn router_config(timeout_ms: Option<u64>, headers: HashMap<String, String>) -> RouterConfig {
        RouterConfig {
            url: "https://api.example.com".to_string(),
            api_key_env: None,
            api_key: None,
            enabled: true,
            timeout_ms,
            headers,
        }
    }

    #[test]
    fn from_config_maps_timeout_ms_into_max_wait() {
        // A long-running video job: operator bumps timeout_ms to 20 min.
        let config = router_config(Some(1_200_000), HashMap::new());
        let job = JobConfig::from_config(&config);
        assert_eq!(job.max_wait, Duration::from_millis(1_200_000));
        // Poll interval keeps the default when no hint is present.
        assert_eq!(
            job.poll_interval,
            Duration::from_secs(DEFAULT_POLL_INTERVAL_SECS)
        );
    }

    #[test]
    fn from_config_falls_back_to_default_max_wait_when_timeout_unset() {
        let config = router_config(None, HashMap::new());
        let job = JobConfig::from_config(&config);
        assert_eq!(job.max_wait, Duration::from_secs(DEFAULT_MAX_WAIT_SECS));
    }

    #[test]
    fn from_config_honours_poll_interval_header_hint() {
        let headers = HashMap::from([(POLL_INTERVAL_HEADER.to_string(), "500".to_string())]);
        let config = router_config(Some(60_000), headers);
        let job = JobConfig::from_config(&config);
        assert_eq!(job.poll_interval, Duration::from_millis(500));
        assert_eq!(job.max_wait, Duration::from_millis(60_000));
    }

    #[test]
    fn from_config_ignores_invalid_poll_interval_hint() {
        // Non-numeric and zero values fall back to the default rather
        // than producing a busy-loop or a parse panic.
        for bad in ["nonsense", "0", "-5"] {
            let headers = HashMap::from([(POLL_INTERVAL_HEADER.to_string(), bad.to_string())]);
            let config = router_config(None, headers);
            let job = JobConfig::from_config(&config);
            assert_eq!(
                job.poll_interval,
                Duration::from_secs(DEFAULT_POLL_INTERVAL_SECS),
                "input {bad:?} should fall back to default",
            );
        }
    }

    #[tokio::test]
    async fn poll_completes_immediately() {
        let config = JobConfig {
            poll_interval: Duration::from_millis(10),
            max_wait: Duration::from_secs(5),
        };

        let result = poll_until_complete(&config, || async { Ok(Some(42u32)) }).await;

        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn poll_completes_after_retries() {
        let config = JobConfig {
            poll_interval: Duration::from_millis(10),
            max_wait: Duration::from_secs(5),
        };

        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let result = poll_until_complete(&config, move || {
            let counter = counter_clone.clone();
            async move {
                let count = counter.fetch_add(1, Ordering::SeqCst);
                if count >= 3 {
                    Ok(Some("done".to_string()))
                } else {
                    Ok(None)
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), "done");
        assert!(counter.load(Ordering::SeqCst) >= 4);
    }

    #[tokio::test]
    async fn poll_times_out() {
        let config = JobConfig {
            poll_interval: Duration::from_millis(10),
            max_wait: Duration::from_millis(50),
        };

        let result: Result<String, _> = poll_until_complete(&config, || async {
            Ok::<Option<String>, GatewayError>(None)
        })
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, GatewayError::Timeout { .. }),
            "expected Timeout error, got: {err:?}",
        );
    }
}
