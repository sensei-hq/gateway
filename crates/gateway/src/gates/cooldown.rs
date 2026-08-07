use super::RouterHealthRead;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// In-memory per-router cooldown state (read by the gate, written by the sink).
/// Arc-backed + Clone so a read reference and an owned sink copy share one map.
#[derive(Clone, Default)]
pub struct ConnectionCooldownStore {
    cooling: Arc<Mutex<HashMap<String, Instant>>>,
}
impl ConnectionCooldownStore {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn start(&self, router: &str, until: Instant) {
        self.cooling
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(router.to_string(), until);
    }
}
impl RouterHealthRead for ConnectionCooldownStore {
    fn cooling_until(&self, router: &str) -> Option<Instant> {
        self.cooling
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(router)
            .copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};
    #[test]
    fn store_reports_cooling_until_set() {
        let s = ConnectionCooldownStore::new();
        assert!(s.cooling_until("r").is_none()); // unknown → not cooling
        let until = Instant::now() + Duration::from_secs(60);
        s.start("r", until);
        assert_eq!(s.cooling_until("r"), Some(until)); // recorded
    }
}
