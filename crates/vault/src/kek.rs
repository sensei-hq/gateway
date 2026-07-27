//! KEK providers — where the master key-encryption key comes from.
//!
//! The [`crypto`](crate::crypto) layer takes a raw 32-byte KEK; this module decides how
//! that KEK is sourced, so the trusted process never has to hardcode it. A [`KekProvider`]
//! resolves the KEK (rarely — startup / rotation) and implementations cache it, so
//! [`KekProvider::kek`] is a cheap sync accessor even when the backing store is remote.
//!
//! Gap #1 (the epic): the strategos vault read the KEK straight from a process env var.
//! Here that stays a **dev-only** affordance — [`EnvKekProvider`] **fails closed** under
//! [`Profile::Prod`], forcing production onto a KMS/Secrets-backed provider (torii#17: a
//! Supabase-Vault-backed provider, landing behind the crate's `sqlx` feature with V3).

use zeroize::Zeroizing;

#[derive(Debug, thiserror::Error)]
pub enum KekError {
    #[error("KEK env var `{0}` is unset")]
    Unset(String),
    #[error("KEK is invalid: {0}")]
    Invalid(String),
    #[error("a raw env KEK is refused under the prod profile — configure a KMS/Vault-backed KEK")]
    EnvKekInProd,
    #[error("KEK backend error: {0}")]
    Backend(String),
}

/// Deployment profile. `Prod` forbids a raw KEK in process env (fail-closed).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    Dev,
    Prod,
}

/// Source of the master KEK. Resolved rarely (startup / rotation); implementations cache,
/// so `kek()` is a sync accessor. `Send + Sync` + object-safe so it can back a shared
/// `Vault` as `dyn KekProvider` or a generic parameter.
pub trait KekProvider: Send + Sync {
    /// The master KEK (32 bytes), in [`Zeroizing`]. Never logged; callers pass it straight
    /// to the envelope crypto and drop it.
    fn kek(&self) -> Result<Zeroizing<[u8; 32]>, KekError>;
}

/// A KEK supplied directly as bytes — for tests, and for callers that resolve the key via
/// their own mechanism (e.g. an at-startup KMS `Decrypt`) and just hand the bytes in.
pub struct StaticKekProvider(Zeroizing<[u8; 32]>);

impl StaticKekProvider {
    pub fn new(kek: [u8; 32]) -> Self {
        Self(Zeroizing::new(kek))
    }
}

impl KekProvider for StaticKekProvider {
    fn kek(&self) -> Result<Zeroizing<[u8; 32]>, KekError> {
        Ok(self.0.clone())
    }
}

/// Dev-only: a base64-encoded 32-byte KEK, from an env var or supplied string. **Fails
/// closed under [`Profile::Prod`]** — production must use a KMS/Vault-backed provider.
pub struct EnvKekProvider(Zeroizing<[u8; 32]>);

impl EnvKekProvider {
    /// Read a base64 32-byte KEK from environment variable `var`. The caller supplies the
    /// var name (no hardcoded ops) and the deployment `profile`.
    pub fn from_env(var: &str, profile: Profile) -> Result<Self, KekError> {
        let raw = std::env::var(var).map_err(|_| KekError::Unset(var.to_string()))?;
        Self::from_base64(raw.trim(), profile)
    }

    /// Decode a base64 32-byte KEK. Refuses under `Profile::Prod` before touching the
    /// bytes, so a prod misconfiguration fails loudly rather than running on a dev KEK.
    pub fn from_base64(b64: &str, profile: Profile) -> Result<Self, KekError> {
        if profile == Profile::Prod {
            return Err(KekError::EnvKekInProd);
        }
        // `Zeroizing` so the intermediate decoded copy is wiped on drop.
        let bytes = Zeroizing::new(
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64)
                .map_err(|e| KekError::Invalid(format!("base64: {e}")))?,
        );
        let arr: [u8; 32] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| KekError::Invalid(format!("expected 32 bytes, got {}", bytes.len())))?;
        Ok(Self(Zeroizing::new(arr)))
    }
}

impl KekProvider for EnvKekProvider {
    fn kek(&self) -> Result<Zeroizing<[u8; 32]>, KekError> {
        Ok(self.0.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;

    fn b64(bytes: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    #[test]
    fn env_kek_round_trips_in_dev() {
        let raw = [3u8; 32];
        let p = EnvKekProvider::from_base64(&b64(&raw), Profile::Dev).unwrap();
        assert_eq!(*p.kek().unwrap(), raw);
    }

    #[test]
    fn env_kek_refused_in_prod_even_when_valid() {
        // The gap-closer: a well-formed dev KEK must NOT load under the prod profile.
        let raw = [3u8; 32];
        assert!(matches!(
            EnvKekProvider::from_base64(&b64(&raw), Profile::Prod),
            Err(KekError::EnvKekInProd)
        ));
    }

    #[test]
    fn env_kek_rejects_wrong_length() {
        assert!(matches!(
            EnvKekProvider::from_base64(&b64(&[1u8; 16]), Profile::Dev),
            Err(KekError::Invalid(_))
        ));
    }

    #[test]
    fn env_kek_rejects_bad_base64() {
        assert!(matches!(
            EnvKekProvider::from_base64("not base64!!!", Profile::Dev),
            Err(KekError::Invalid(_))
        ));
    }

    #[test]
    fn static_provider_returns_its_bytes() {
        let p = StaticKekProvider::new([9u8; 32]);
        assert_eq!(*p.kek().unwrap(), [9u8; 32]);
    }

    #[test]
    fn provider_kek_seals_and_unseals_a_dek() {
        // Ties V2 → V1: a KEK from a provider drives the envelope round-trip.
        use crate::crypto::{generate_dek, seal_dek, unseal_dek};
        let provider = StaticKekProvider::new([5u8; 32]);
        let kek = provider.kek().unwrap();
        let dek = *generate_dek();
        let sealed = seal_dek(&kek, &dek).unwrap();
        assert_eq!(*unseal_dek(&kek, &sealed).unwrap(), dek);
    }
}
