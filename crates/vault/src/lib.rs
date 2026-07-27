//! `sensei-vault` — the shared, security-critical BYOK credential vault used by both the
//! sensei daemon and the strategos gateway (epic sensei-hq/gateway#38). It extracts the
//! vault that shipped inline in the strategos gateway and closes its three gaps — a
//! KMS-backed KEK (V2), `tenant‖router` AAD binding (V1), and key rotation (V4) — once,
//! for both consumers.
//!
//! **Invariants (non-negotiable):**
//! - Decrypt only in the trusted process (daemon / gateway) — never a Worker or client.
//! - Backing storage is RLS deny-all + `service_role`-only; keys are never returned or logged.
//! - Any credential-bearing type redacts its value in `Debug`.
//! - Key material and plaintext live in [`zeroize::Zeroizing`].
//! - Tamper, short blob, AAD mismatch, and a missing/invalid KEK all **fail closed**.
//!
//! V1 ships the AEAD envelope ([`crypto`]); V2 the [`kek`] providers; V3 the [`store`]
//! seam + the [`vault`] orchestrator; V4 rotation + the AAD re-seal migration
//! ([`Vault::rotate_dek`], [`Vault::rotate_kek`], [`Vault::reseal_without_aad`]). Next:
//! `TenantKeyCache` (V5).

pub mod crypto;
pub mod kek;
pub mod store;
pub mod vault;

#[cfg(feature = "sqlx")]
pub mod postgres;

pub use kek::{KekProvider, Profile};
pub use store::{DekBlob, StoredCredential, VaultStore};
pub use vault::{Vault, VaultError};
