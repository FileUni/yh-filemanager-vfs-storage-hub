use super::CachePathPolicy;
use crate::config::VfsReadCacheConfig;
use crate::vfs::VfsFileInfo;
use crate::vfs::global_vfs_metrics;
use bytes::Bytes;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadCacheBackend {
    Memory,
    LocalDir,
}

#[derive(Debug, Clone)]
enum ReadCacheBlob {
    Memory(Bytes),
    Disk(PathBuf),
}

#[derive(Debug, Clone)]
struct ReadCacheEntry {
    key: Arc<str>,
    info: VfsFileInfo,
    size: u64,
    last_accessed_at: i64,
    expires_at: i64,
    blob: ReadCacheBlob,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReadCacheDiskMeta {
    key: String,
    info: VfsFileInfo,
    size: u64,
    created_at: i64,
    expires_at: i64,
}

#[derive(Debug)]
pub struct ReadCacheManager {
    enabled: bool,
    backend: ReadCacheBackend,
    root_dir: PathBuf,
    capacity_bytes: u64,
    max_file_size_bytes: u64,
    ttl_secs: u64,
    cleanup_interval_secs: u64,
    policy: CachePathPolicy,
    entries: DashMap<String, ReadCacheEntry>,
    accounted_bytes: AtomicU64,
    last_cleanup_ts: AtomicI64,
}

fn now_ts() -> i64 {
    chrono::Utc::now().timestamp()
}

fn cache_hash(key: &str) -> String {
    format!("{:x}", Sha256::digest(key.as_bytes()))
}

fn data_path(root_dir: &Path, key: &str) -> PathBuf {
    root_dir.join(format!("{}.bin", cache_hash(key)))
}

fn meta_path(root_dir: &Path, key: &str) -> PathBuf {
    root_dir.join(format!("{}.json", cache_hash(key)))
}

impl ReadCacheManager {
    pub async fn new(pool_name: &str, config: &VfsReadCacheConfig) -> anyhow::Result<Arc<Self>> {
        let backend = match config.get_backend() {
            "memory" => ReadCacheBackend::Memory,
            _ => ReadCacheBackend::LocalDir,
        };
        let root_dir = PathBuf::from(config.get_local_dir()).join(pool_name);
        if matches!(backend, ReadCacheBackend::LocalDir) {
            tokio::fs::create_dir_all(&root_dir).await?;
        }
        let manager = Arc::new(Self {
            enabled: config.is_enabled(),
            backend,
            root_dir,
            capacity_bytes: config.get_capacity_bytes(),
            max_file_size_bytes: config.get_max_file_size_bytes(),
            ttl_secs: config.get_ttl_secs(),
            cleanup_interval_secs: config.get_ttl_secs().saturating_div(4).clamp(1, 60),
            policy: CachePathPolicy::new(
                config.is_cache_thumbnail_paths(),
                &config.get_skip_extensions(),
            ),
            entries: DashMap::new(),
            accounted_bytes: AtomicU64::new(0),
            last_cleanup_ts: AtomicI64::new(0),
        });
        manager.load_existing_entries().await;
        Ok(manager)
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn should_cache(&self, path: &str, size: u64) -> bool {
        self.enabled
            && self.policy.allows(path)
            && size > 0
            && size <= self.max_file_size_bytes
            && size <= self.capacity_bytes
    }

    pub async fn get(&self, key: &str) -> Option<(Bytes, VfsFileInfo)> {
        if !self.enabled || !self.policy.allows(key) {
            global_vfs_metrics().record_read_cache_miss();
            return None;
        }
        let now = now_ts();
        let entry = {
            let Some(mut entry) = self.entries.get_mut(key) else {
                global_vfs_metrics().record_read_cache_miss();
                return None;
            };
            if entry.expires_at <= now {
                drop(entry);
                self.remove(key).await;
                global_vfs_metrics().record_read_cache_miss();
                return None;
            }
            entry.last_accessed_at = now;
            entry.clone()
        };
        match &entry.blob {
            ReadCacheBlob::Memory(bytes) => {
                global_vfs_metrics().record_read_cache_hit();
                Some((bytes.clone(), entry.info.clone()))
            }
            ReadCacheBlob::Disk(path) => match tokio::fs::read(path).await {
                Ok(data) => {
                    global_vfs_metrics().record_read_cache_hit();
                    Some((Bytes::from(data), entry.info.clone()))
                }
                Err(err) => {
                    yh_console_log::yhlog(
                        "warn",
                        &format!("Read cache disk read failed for '{}': {}", key, err),
                    );
                    self.remove(key).await;
                    global_vfs_metrics().record_read_cache_miss();
                    None
                }
            },
        }
    }

    pub async fn put(&self, key: &str, data: Bytes, info: VfsFileInfo) {
        if !self.should_cache(key, data.len() as u64) {
            return;
        }
        self.remove(key).await;
        self.cleanup_expired_if_due(false).await;
        if !self.ensure_capacity(data.len() as u64).await {
            return;
        }
        let created_at = now_ts();
        let expires_at = created_at + self.ttl_secs as i64;
        let blob = match self.backend {
            ReadCacheBackend::Memory => ReadCacheBlob::Memory(data.clone()),
            ReadCacheBackend::LocalDir => {
                let data_path = data_path(&self.root_dir, key);
                let meta_path = meta_path(&self.root_dir, key);
                let meta = ReadCacheDiskMeta {
                    key: key.to_string(),
                    info: info.clone(),
                    size: data.len() as u64,
                    created_at,
                    expires_at,
                };
                if tokio::fs::write(&data_path, &data).await.is_err()
                    || self.write_meta(&meta_path, &meta).await.is_err()
                {
                    let _ = tokio::fs::remove_file(&data_path).await;
                    let _ = tokio::fs::remove_file(&meta_path).await;
                    return;
                }
                ReadCacheBlob::Disk(data_path)
            }
        };
        let entry = ReadCacheEntry {
            key: Arc::from(key),
            info,
            size: data.len() as u64,
            last_accessed_at: created_at,
            expires_at,
            blob,
        };
        self.accounted_bytes.fetch_add(entry.size, Ordering::SeqCst);
        self.entries.insert(key.to_string(), entry);
        global_vfs_metrics().record_read_cache_put(data.len() as u64);
    }

    pub async fn remove(&self, key: &str) {
        let Some((_, entry)) = self.entries.remove(key) else {
            return;
        };
        self.accounted_bytes.fetch_sub(entry.size, Ordering::SeqCst);
        if let ReadCacheBlob::Disk(path) = entry.blob {
            let _ = tokio::fs::remove_file(path).await;
            let _ = tokio::fs::remove_file(meta_path(&self.root_dir, &entry.key)).await;
        }
    }

    async fn load_existing_entries(&self) {
        if !self.enabled || !matches!(self.backend, ReadCacheBackend::LocalDir) {
            return;
        }
        let Ok(mut dir) = tokio::fs::read_dir(&self.root_dir).await else {
            return;
        };
        while let Ok(Some(entry)) = dir.next_entry().await {
            let path = entry.path();
            if path.extension().and_then(|v| v.to_str()) != Some("json") {
                continue;
            }
            let Ok(raw) = tokio::fs::read_to_string(&path).await else {
                continue;
            };
            let Ok(meta) = serde_json::from_str::<ReadCacheDiskMeta>(&raw) else {
                continue;
            };
            if meta.expires_at <= now_ts() {
                let _ = tokio::fs::remove_file(&path).await;
                let _ = tokio::fs::remove_file(data_path(&self.root_dir, &meta.key)).await;
                continue;
            }
            let data_path = data_path(&self.root_dir, &meta.key);
            if tokio::fs::metadata(&data_path).await.is_err() {
                let _ = tokio::fs::remove_file(&path).await;
                continue;
            }
            self.accounted_bytes.fetch_add(meta.size, Ordering::SeqCst);
            self.entries.insert(
                meta.key.clone(),
                ReadCacheEntry {
                    key: Arc::from(meta.key.as_str()),
                    info: meta.info,
                    size: meta.size,
                    last_accessed_at: meta.created_at,
                    expires_at: meta.expires_at,
                    blob: ReadCacheBlob::Disk(data_path),
                },
            );
        }
        self.cleanup_expired_if_due(true).await;
    }

    async fn cleanup_expired_if_due(&self, force: bool) {
        let now = now_ts();
        let last = self.last_cleanup_ts.load(Ordering::Relaxed);
        if !force && now.saturating_sub(last) < self.cleanup_interval_secs as i64 {
            return;
        }
        self.last_cleanup_ts.store(now, Ordering::Relaxed);
        let expired_keys: Vec<String> = self
            .entries
            .iter()
            .filter_map(|entry| (entry.value().expires_at <= now).then(|| entry.key().clone()))
            .collect();
        for key in expired_keys {
            self.remove(&key).await;
        }
    }

    async fn ensure_capacity(&self, required: u64) -> bool {
        self.cleanup_expired_if_due(true).await;
        loop {
            let used = self.accounted_bytes.load(Ordering::SeqCst);
            if used.saturating_add(required) <= self.capacity_bytes {
                return true;
            }
            let oldest_key = self
                .entries
                .iter()
                .min_by_key(|entry| entry.value().last_accessed_at)
                .map(|entry| entry.key().clone());
            let Some(key) = oldest_key else {
                return false;
            };
            self.remove(&key).await;
        }
    }

    async fn write_meta(&self, path: &Path, meta: &ReadCacheDiskMeta) -> anyhow::Result<()> {
        let raw = serde_json::to_vec(meta)?;
        tokio::fs::write(path, raw).await?;
        Ok(())
    }
}
