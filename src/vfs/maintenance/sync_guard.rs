use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

type PathGuard = Arc<Mutex<()>>;
type UserPathGuards = DashMap<Arc<str>, PathGuard>;
/// Synchronization task concurrency manager
/// Prevents redundant synchronization of the same directory triggered by multiple protocols or users
#[derive(Debug)]
pub struct SyncGuardManager {
    guards: DashMap<Arc<str>, UserPathGuards>,
}
impl SyncGuardManager {
    pub fn new() -> Self {
        Self {
            guards: DashMap::new(),
        }
    }
    /// Get synchronization lock for a specific path
    pub fn get_guard(&self, user_id: &str, path: &str) -> PathGuard {
        if let Some(user_guards) = self.guards.get(user_id)
            && let Some(existing) = user_guards.get(path)
        {
            return Arc::clone(existing.value());
        }

        let user_key: Arc<str> = Arc::from(user_id);
        let path_key: Arc<str> = Arc::from(path);
        let user_guards = self.guards.entry(user_key).or_default();
        Arc::clone(
            user_guards
                .entry(path_key)
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .value(),
        )
    }
    ///( Arc )
    /// Prune unused guards (guards are automatically recycled via Arc, this is for explicit memory management)
    pub fn prune(&self) {
        self.guards.retain(|_, user_guards| {
            user_guards.retain(|_, guard| Arc::strong_count(guard) > 1);
            !user_guards.is_empty()
        });
    }
}
impl Default for SyncGuardManager {
    fn default() -> Self {
        Self::new()
    }
}
