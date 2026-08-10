use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// The side-effect class of an effect. Only `Pure` is exercised in slice 1;
/// `Observation`/`Mutation` are reserved for later SP-1 slices.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum EffectClass {
    Pure,
    Observation,
    Mutation,
}

/// A structural, iteration-aware effect identifier.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct EffectId(pub String);

/// Compute a structural effect id: `sha256(parent_path ‖ loop_iteration ‖ local_index)`,
/// hex-encoded. Deterministic in the structural coordinates and independent of
/// wall-clock or execution order, so a resumed run recomputes the same id.
pub fn effect_id(parent_path: &str, loop_iteration: u64, local_index: usize) -> EffectId {
    let mut hasher = Sha256::new();
    hasher.update(format!("{parent_path}|{loop_iteration}|{local_index}").as_bytes());
    EffectId(format!("{:x}", hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use crate::effect_id;

    #[test]
    fn effect_id_is_structural_and_stable() {
        let a = effect_id("", 0, 0);
        let b = effect_id("", 0, 0);
        let c = effect_id("", 0, 1);
        assert_eq!(a, b); // same structural coords → same id (deterministic)
        assert_ne!(a, c); // different local_index → different id
        assert_ne!(effect_id("p", 0, 0), a); // parent_path matters
        assert_ne!(effect_id("", 1, 0), a); // loop_iteration matters
    }
}
