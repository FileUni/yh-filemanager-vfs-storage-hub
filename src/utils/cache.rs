use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use yh_fast_kv_storage_hub::api::{del, get_json, set_json};
// Cache item structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheItem<T> {
    pub data: T,
    pub timestamp: u64,
}
// VFS cache helper
#[derive(Debug, Clone)]
pub struct VfsCache {
    prefix: String,
    enabled: bool,
}
impl VfsCache {
    pub fn new(user_id: &str, enabled: bool) -> Self {
        Self { prefix: format!("vfs:{}", user_id), enabled }
    }
    // Generate cache key
    fn make_key(&self, operation: &str, path: &str) -> String {
        format!("{}:{}:{}", self.prefix, operation, path)
    }
    // Get current timestamp
    fn now_timestamp() -> u64 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_secs(),
            Err(err) => {
                yh_console_log::yhlog("warn", &format!("System time before UNIX_EPOCH: {}", err));
                0
            }
        }
    }
    // Get data from cache
    pub async fn get<T>(&self, operation: &str, path: &str, ttl: u64) -> Option<T>
    where
        T: DeserializeOwned,
    {
        if !self.enabled {
            return None;
        }
        let key = self.make_key(operation, path);
        match get_json::<CacheItem<T>>(&key).await {
            Ok(Some(item)) => {
                let age = Self::now_timestamp().saturating_sub(item.timestamp);
                if age <= ttl { Some(item.data) } else { None }
            }
            _ => None,
        }
    }
    // Store data in cache
    pub async fn set<T>(&self, operation: &str, path: &str, data: T, ttl: Option<u64>)
    where
        T: Serialize,
    {
        if !self.enabled {
            return;
        }
        let key = self.make_key(operation, path);
        let item = CacheItem { data, timestamp: Self::now_timestamp() };
        let _ = set_json(&key, &item, ttl).await;
    }
    // Invalidate specific cache
    pub async fn invalidate(&self, operation: &str, path: &str) {
        if !self.enabled {
            return;
        }
        let key = self.make_key(operation, path);
        let _ = del(&key).await;
    }
    // Recursively invalidate cache for current path and parent directory
    // For file changes, usually need to invalidate the 'ls' cache of parent directory
    pub async fn invalidate_parent_ls(&self, path: &str) {
        if !self.enabled {
            return;
        }
        let path = path.trim_matches('/');
        let parent = if let Some((parent, _)) = path.rsplit_once('/') {
            parent
        } else if !path.is_empty() {
            "" // Root
        } else {
            return;
        };
        // Normalize parent directory path to start with /
        let parent_key = if parent.is_empty() { "/".to_string() } else { format!("/{}", parent) };
        self.invalidate("ls", &parent_key).await;
    }
}
