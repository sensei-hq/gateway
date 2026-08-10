//! Content-addressed storage (CAS) vocabulary (§7.4). A large effect output or
//! context value is stored once, keyed by the sha256 of its bytes, and the
//! journal/blackboard carry a lightweight [`ContentRef`] (digest + size +
//! summary) instead of the inline blob — so the fold reads refs without ever
//! deserializing the payload, and identical content deduplicates to one digest.

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::error::OrchestratorError;

/// The sha256 hex digest of a blob's bytes — its content address.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Digest(pub String);

/// A content-addressed reference: the digest, the blob's byte size, and an
/// optional human summary. Carried by the journal/blackboard in place of a large
/// inline value; the bytes are fetched lazily via [`ContentStore::get`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentRef {
    pub digest: Digest,
    pub size: usize,
    pub summary: Option<String>,
}

/// Compute the content address of `bytes`: `sha256` hex.
pub fn digest_of(bytes: &[u8]) -> Digest {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Digest(format!("{:x}", hasher.finalize()))
}

/// The content-addressed store seam. Slice 3 ships an in-memory implementation;
/// a persistent CAS implements the same trait in a later layer.
///
/// `put` is idempotent — identical content yields the same [`Digest`] and stores
/// one copy. `get` is strict — a digest miss is a loud error, never an empty value.
#[async_trait::async_trait]
pub trait ContentStore: Send + Sync {
    async fn put(&self, bytes: &[u8]) -> Result<Digest, OrchestratorError>;
    async fn get(&self, digest: &Digest) -> Result<Vec<u8>, OrchestratorError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_is_stable_and_content_addressed() {
        assert_eq!(
            digest_of(b"hello"),
            digest_of(b"hello"),
            "same bytes → same digest"
        );
        assert_ne!(
            digest_of(b"hello"),
            digest_of(b"world"),
            "different bytes → different digest"
        );
    }
}
