//! Encoding of the per-call credentials channel ([`InferenceRequest.credentials`]).
//!
//! A value is normally a provider **api_key**, injected as `RouterConfig.api_key` and sent in
//! the provider's api_key header. An **OAuth bearer** credential is marked with the
//! [`OAUTH_PREFIX`] so an OAuth-aware adapter presents it as `Authorization: Bearer <token>`
//! instead. The tenant-aware consumer applies the prefix when it fills the map; the adapter
//! recovers the bare token via [`oauth_token`]. Keeping this contract in one place stops the
//! producer (the credential vault consumer) and the consumer (the adapter) from drifting.
//!
//! [`InferenceRequest.credentials`]: crate::types::request::InferenceRequest

/// Marks a `credentials` value as an OAuth bearer token rather than a bare api_key.
pub const OAUTH_PREFIX: &str = "oauth:";

/// If `raw` is an OAuth-marked credential, return the bare bearer token; otherwise `None`
/// (it is a plain api_key). `oauth_token("oauth:abc") == Some("abc")`.
pub fn oauth_token(raw: &str) -> Option<&str> {
    raw.strip_prefix(OAUTH_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_oauth_vs_api_key() {
        assert_eq!(oauth_token("oauth:sk-oauth-123"), Some("sk-oauth-123"));
        assert_eq!(oauth_token("sk-ant-static"), None);
        // An empty token is still an (empty) oauth marker, not an api_key.
        assert_eq!(oauth_token("oauth:"), Some(""));
    }
}
