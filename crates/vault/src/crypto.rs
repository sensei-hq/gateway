//! AEAD envelope crypto for the credential vault — AES-256-GCM via the vetted `aes-gcm`
//! crate (RustCrypto), never hand-rolled.
//!
//! Two layers:
//! - A per-tenant **DEK** is sealed under the master **KEK** ([`seal_dek`] / [`unseal_dek`]).
//! - A provider credential is sealed under the tenant DEK **bound to `aad`** — the caller
//!   passes `tenant_id ‖ router_id` ([`seal_credential`] / [`unseal_credential`]). The AAD
//!   binding is the point: a sealed blob copied to a different `(tenant, router)` row fails
//!   AEAD authentication, so a DB-write actor cannot relocate ciphertext across tenants.
//!
//! Blob layout (both DEK and credential): `[12B IV][16B tag][ciphertext]`. `aes-gcm` expects
//! `ciphertext ‖ tag`, so we reassemble on decrypt. Key material and plaintext live in
//! [`Zeroizing`]; a fresh random 96-bit nonce is drawn per seal. Short blob, tamper, wrong
//! key, and AAD mismatch all **fail closed** with [`CryptoError::Decrypt`] / `TooShort`.
//!
//! Functions take raw 32-byte keys so this layer is agnostic to how the KEK is sourced
//! (env vs KMS) — that is the `KekProvider` seam in V2.

use aes_gcm::aead::{Aead, AeadCore, AeadInPlace, KeyInit, OsRng, Payload};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use zeroize::Zeroizing;

const IV_LEN: usize = 12;
const TAG_LEN: usize = 16;

#[derive(Debug, thiserror::Error)]
pub enum CryptoError {
    #[error("ciphertext too short ({0} bytes; need > {1})")]
    TooShort(usize, usize),
    #[error("AEAD decryption failed (tampered ciphertext, wrong key, or AAD mismatch)")]
    Decrypt,
}

/// Seal `plaintext` under `key` with `aad` → `[IV][tag][ct]`, drawing a fresh random nonce.
fn seal_gcm(key: &[u8; 32], aad: &[u8], plaintext: &[u8]) -> Result<Vec<u8>, CryptoError> {
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let mut buf = plaintext.to_vec();
    let tag = cipher
        .encrypt_in_place_detached(&nonce, aad, &mut buf)
        .map_err(|_| CryptoError::Decrypt)?;
    let mut out = Vec::with_capacity(IV_LEN + TAG_LEN + buf.len());
    out.extend_from_slice(nonce.as_slice());
    out.extend_from_slice(&tag);
    out.extend_from_slice(&buf);
    Ok(out)
}

/// Open an `[IV][tag][ct]` blob under `key` with `aad`. Fails closed on a short blob, a
/// tampered blob, the wrong key, or an AAD mismatch.
fn open_gcm(key: &[u8; 32], aad: &[u8], blob: &[u8]) -> Result<Vec<u8>, CryptoError> {
    if blob.len() <= IV_LEN + TAG_LEN {
        return Err(CryptoError::TooShort(blob.len(), IV_LEN + TAG_LEN));
    }
    let (iv, rest) = blob.split_at(IV_LEN);
    let (tag, ct) = rest.split_at(TAG_LEN);
    // aes-gcm wants the tag appended to the ciphertext.
    let mut ct_tag = Vec::with_capacity(ct.len() + TAG_LEN);
    ct_tag.extend_from_slice(ct);
    ct_tag.extend_from_slice(tag);
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    cipher
        .decrypt(Nonce::from_slice(iv), Payload { msg: &ct_tag, aad })
        .map_err(|_| CryptoError::Decrypt)
}

/// Generate a fresh random 32-byte data-encryption key from the OS CSPRNG.
/// Held in [`Zeroizing`] so it is wiped from memory when the caller drops it.
pub fn generate_dek() -> Zeroizing<[u8; 32]> {
    let key = Aes256Gcm::generate_key(&mut OsRng);
    let mut dek = Zeroizing::new([0u8; 32]);
    dek.copy_from_slice(key.as_slice());
    dek
}

/// Seal a tenant DEK under the master KEK → `[IV][tag][ct]`. No AAD: unlike a credential,
/// the DEK is keyed by `(tenant)` alone and isn't relocatable across credential rows.
pub fn seal_dek(kek: &[u8; 32], dek: &[u8; 32]) -> Result<Vec<u8>, CryptoError> {
    seal_gcm(kek, b"", dek)
}

/// Unseal a tenant DEK (sealed under the KEK) → the 32-byte data key, in [`Zeroizing`].
pub fn unseal_dek(kek: &[u8; 32], blob: &[u8]) -> Result<Zeroizing<[u8; 32]>, CryptoError> {
    let dek = Zeroizing::new(open_gcm(kek, b"", blob)?);
    let arr: [u8; 32] = dek
        .as_slice()
        .try_into()
        .map_err(|_| CryptoError::Decrypt)?;
    Ok(Zeroizing::new(arr))
}

/// Seal a provider credential under the tenant DEK, **bound to `aad`** (`tenant_id ‖
/// router_id`) → `[IV][tag][ct]`.
pub fn seal_credential(
    dek: &[u8; 32],
    aad: &[u8],
    plaintext: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    seal_gcm(dek, aad, plaintext)
}

/// Unseal a provider credential under the tenant DEK with `aad` → the UTF-8 secret, in
/// [`Zeroizing`]. A blob relocated to a different `(tenant, router)` fails the AAD check.
pub fn unseal_credential(
    dek: &[u8; 32],
    aad: &[u8],
    blob: &[u8],
) -> Result<Zeroizing<String>, CryptoError> {
    let pt = open_gcm(dek, aad, blob)?;
    let secret = String::from_utf8(pt).map_err(|_| CryptoError::Decrypt)?;
    Ok(Zeroizing::new(secret))
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEK: [u8; 32] = [11u8; 32];
    const KEK: [u8; 32] = [7u8; 32];
    const AAD: &[u8] = b"tenant-A|router-openai";

    #[test]
    fn credential_round_trips_with_aad() {
        let blob = seal_credential(&DEK, AAD, b"sk-ant-secret").unwrap();
        assert_eq!(
            *unseal_credential(&DEK, AAD, &blob).unwrap(),
            "sk-ant-secret"
        );
    }

    #[test]
    fn dek_round_trips_under_kek() {
        let dek = *generate_dek();
        let blob = seal_dek(&KEK, &dek).unwrap();
        assert_eq!(*unseal_dek(&KEK, &blob).unwrap(), dek);
    }

    #[test]
    fn fresh_nonce_per_seal_yields_distinct_ciphertext() {
        // Same key + AAD + plaintext must still produce different blobs (random nonce),
        // otherwise equal secrets would be observable as equal ciphertext.
        let a = seal_credential(&DEK, AAD, b"same").unwrap();
        let b = seal_credential(&DEK, AAD, b"same").unwrap();
        assert_ne!(a, b);
        assert_eq!(*unseal_credential(&DEK, AAD, &a).unwrap(), "same");
        assert_eq!(*unseal_credential(&DEK, AAD, &b).unwrap(), "same");
    }

    #[test]
    fn bit_flip_fails_closed() {
        let mut blob = seal_credential(&DEK, AAD, b"secret").unwrap();
        let last = blob.len() - 1;
        blob[last] ^= 0x01;
        assert!(matches!(
            unseal_credential(&DEK, AAD, &blob),
            Err(CryptoError::Decrypt)
        ));
    }

    #[test]
    fn aad_mismatch_rejected() {
        // The core AAD-binding guarantee: a blob sealed for (tenant A, router X) must NOT
        // unseal under a different AAD — this is what stops a DB-write actor relocating
        // ciphertext across tenant/router rows.
        let blob = seal_credential(&DEK, b"tenant-A|router-openai", b"sk").unwrap();
        assert!(matches!(
            unseal_credential(&DEK, b"tenant-B|router-openai", &blob),
            Err(CryptoError::Decrypt)
        ));
    }

    #[test]
    fn wrong_key_fails_closed() {
        let blob = seal_credential(&DEK, AAD, b"secret").unwrap();
        assert!(matches!(
            unseal_credential(&[0u8; 32], AAD, &blob),
            Err(CryptoError::Decrypt)
        ));
    }

    #[test]
    fn short_blob_rejected() {
        assert!(matches!(
            unseal_credential(&DEK, AAD, b"tiny"),
            Err(CryptoError::TooShort(..))
        ));
    }

    #[test]
    fn dek_tamper_fails_closed() {
        let mut blob = seal_dek(&KEK, &[1u8; 32]).unwrap();
        let last = blob.len() - 1;
        blob[last] ^= 0x01;
        assert!(matches!(unseal_dek(&KEK, &blob), Err(CryptoError::Decrypt)));
    }
}
