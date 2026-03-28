use super::{CachePathPolicy, read_cache::ReadCacheManager};
use crate::business::entities::file_index;
use crate::business::services::{FileIndexService, user_settings::UserSettingsService};
use crate::config::VfsWriteCacheConfig;
use crate::vfs::{VfsFileInfo, VfsJournalEvent, global_vfs_metrics};
use bytes::Bytes;
use dashmap::DashMap;
use opendal::{Metadata, Operator};
use sea_orm::ActiveValue::Set;
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Mutex, Semaphore};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteCacheBackend {
    Memory,
    LocalDir,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingWriteState {
    Pending,
    Flushing,
    Abnormal,
}

#[derive(Debug, Clone)]
enum PendingWriteBlob {
    Memory(Bytes),
    Disk(PathBuf),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PendingWriteDiskMeta {
    user_id: String,
    logical_path: String,
    physical_path: String,
    size: u64,
    modified_at: i64,
    deadline_at: i64,
    generation: u64,
    abnormal: bool,
}

#[derive(Debug)]
struct PendingWriteMutable {
    blob: PendingWriteBlob,
    size: u64,
    modified_at: i64,
    deadline_at: i64,
    next_retry_at: i64,
    generation: u64,
    retry_count: u32,
    state: PendingWriteState,
    abnormal_logged: bool,
    last_error: Option<String>,
}

#[derive(Debug)]
struct PendingWriteRecord {
    user_id: Arc<str>,
    logical_path: Arc<str>,
    physical_path: Arc<str>,
    parent_physical_path: Arc<str>,
    inner: Mutex<PendingWriteMutable>,
}

#[derive(Debug, Clone)]
struct DirectoryIndexSyncTask {
    user_id: Arc<str>,
    logical_parent_path: Arc<str>,
    physical_parent_path: Arc<str>,
    next_sync_at: i64,
    retry_count: u32,
}

#[derive(Debug, Clone)]
struct UserQuotaSyncTask {
    user_id: Arc<str>,
    delta: i64,
    next_sync_at: i64,
    retry_count: u32,
}

#[derive(Debug, Clone)]
pub struct PendingWriteInfo {
    pub physical_path: Arc<str>,
    pub size: u64,
    pub modified: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug)]
pub struct WriteCacheManager {
    enabled: bool,
    backend: WriteCacheBackend,
    queue_dir: PathBuf,
    abnormal_dir: PathBuf,
    capacity_bytes: u64,
    max_file_size_bytes: u64,
    flush_concurrency: usize,
    flush_interval_ms: u64,
    flush_deadline_secs: u64,
    policy: CachePathPolicy,
    accounted_bytes: AtomicU64,
    entries: DashMap<String, Arc<PendingWriteRecord>>,
    dirty_paths: DashMap<String, ()>,
    dirty_dirs: DashMap<String, ()>,
    index_sync_dirs: DashMap<String, DirectoryIndexSyncTask>,
    quota_sync_users: DashMap<String, UserQuotaSyncTask>,
    primary: Arc<Operator>,
    db: Arc<DatabaseConnection>,
    pool_name: Arc<str>,
    backend_type: Arc<str>,
    read_cache: Option<Arc<ReadCacheManager>>,
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

fn file_name_from_path(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

fn normalize_entry_path(path: &str) -> &str {
    if path == "/" {
        return path;
    }
    path.strip_suffix('/').unwrap_or(path)
}

fn to_vfs_file_info(path: &str, meta: &Metadata) -> VfsFileInfo {
    VfsFileInfo {
        name: file_name_from_path(path).into(),
        path: path.into(),
        is_dir: meta.is_dir(),
        size: meta.content_length(),
        modified: meta
            .last_modified()
            .map(|t| std::time::SystemTime::from(t).into()),
        favorite_color: 0,
        has_active_share: None,
        has_active_direct: None,
        trashed_at: None,
        original_path: None,
    }
}

fn parent_path(path: &str) -> Arc<str> {
    let parent = Path::new(path)
        .parent()
        .map(|v| v.to_string_lossy().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "/".to_string());
    Arc::from(parent)
}

fn file_info_from_pending(path: &str, size: u64, modified_at: i64) -> VfsFileInfo {
    let name = Path::new(path)
        .file_name()
        .map(|v| v.to_string_lossy().to_string())
        .unwrap_or_else(|| path.to_string());
    VfsFileInfo {
        name: Arc::from(name),
        path: Arc::from(path),
        is_dir: false,
        size,
        modified: chrono::DateTime::<chrono::Utc>::from_timestamp(modified_at, 0),
        favorite_color: 0,
        has_active_share: None,
        has_active_direct: None,
        trashed_at: None,
        original_path: None,
    }
}

impl WriteCacheManager {
    pub async fn new(
        pool_name: &str,
        primary: Arc<Operator>,
        db: Arc<DatabaseConnection>,
        backend_type: String,
        read_cache: Option<Arc<ReadCacheManager>>,
        config: &VfsWriteCacheConfig,
    ) -> anyhow::Result<Arc<Self>> {
        let backend = match config.get_backend() {
            "memory" => WriteCacheBackend::Memory,
            _ => WriteCacheBackend::LocalDir,
        };
        let queue_dir = PathBuf::from(config.get_local_dir())
            .join(pool_name)
            .join("queue");
        let abnormal_dir = PathBuf::from(config.get_abnormal_spill_dir()).join(pool_name);
        tokio::fs::create_dir_all(&abnormal_dir).await?;
        if matches!(backend, WriteCacheBackend::LocalDir) {
            tokio::fs::create_dir_all(&queue_dir).await?;
        }
        let manager = Arc::new(Self {
            enabled: config.is_enabled(),
            backend,
            queue_dir,
            abnormal_dir,
            capacity_bytes: config.get_capacity_bytes(),
            max_file_size_bytes: config.get_max_file_size_bytes(),
            flush_concurrency: config.get_flush_concurrency(),
            flush_interval_ms: config.get_flush_interval_ms(),
            flush_deadline_secs: config.get_flush_deadline_secs(),
            policy: CachePathPolicy::new(
                config.is_cache_thumbnail_paths(),
                &config.get_skip_extensions(),
            ),
            accounted_bytes: AtomicU64::new(0),
            entries: DashMap::new(),
            dirty_paths: DashMap::new(),
            dirty_dirs: DashMap::new(),
            index_sync_dirs: DashMap::new(),
            quota_sync_users: DashMap::new(),
            primary,
            db,
            pool_name: Arc::from(pool_name),
            backend_type: Arc::from(backend_type),
            read_cache,
        });
        manager.load_existing_entries().await;
        if manager.enabled {
            manager.start_worker();
        }
        Ok(manager)
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn max_file_size_bytes(&self) -> u64 {
        self.max_file_size_bytes
    }

    pub fn should_cache(&self, logical_path: &str, size: usize) -> bool {
        self.enabled
            && self.policy.allows(logical_path)
            && size > 0
            && (size as u64) <= self.max_file_size_bytes
            && (size as u64) <= self.capacity_bytes
    }

    pub async fn enqueue(
        &self,
        user_id: &str,
        logical_path: &str,
        physical_path: &str,
        data: Bytes,
    ) -> anyhow::Result<Option<VfsFileInfo>> {
        if !self.should_cache(logical_path, data.len()) {
            global_vfs_metrics().record_write_cache_bypass();
            return Ok(None);
        }
        if let Some(read_cache) = &self.read_cache {
            read_cache.remove(physical_path).await;
        }
        if !self.can_admit(physical_path, data.len() as u64).await {
            global_vfs_metrics().record_write_cache_bypass();
            return Ok(None);
        }
        let modified_at = now_ts();
        let deadline_at = modified_at + self.flush_deadline_secs as i64;
        if let Some(existing) = self
            .entries
            .get(physical_path)
            .map(|v| Arc::clone(v.value()))
        {
            self.update_existing_entry(&existing, physical_path, data, modified_at, deadline_at)
                .await?;
            self.mark_dirty(logical_path, physical_path, user_id);
            let size = existing.inner.lock().await.size;
            global_vfs_metrics().record_write_cache_enqueue();
            return Ok(Some(file_info_from_pending(
                physical_path,
                size,
                modified_at,
            )));
        }
        let blob = self
            .store_blob(
                &PendingWriteDiskMeta {
                    user_id: user_id.to_string(),
                    logical_path: logical_path.to_string(),
                    physical_path: physical_path.to_string(),
                    size: data.len() as u64,
                    modified_at,
                    deadline_at,
                    generation: 1,
                    abnormal: false,
                },
                &data,
            )
            .await?;
        let record = Arc::new(PendingWriteRecord {
            user_id: Arc::from(user_id),
            logical_path: Arc::from(logical_path),
            physical_path: Arc::from(physical_path),
            parent_physical_path: parent_path(physical_path),
            inner: Mutex::new(PendingWriteMutable {
                blob,
                size: data.len() as u64,
                modified_at,
                deadline_at,
                next_retry_at: modified_at,
                generation: 1,
                retry_count: 0,
                state: PendingWriteState::Pending,
                abnormal_logged: false,
                last_error: None,
            }),
        });
        let guard = record.inner.lock().await;
        self.accounted_bytes
            .fetch_add(self.accounted_bytes_for_blob(&guard), Ordering::SeqCst);
        drop(guard);
        self.entries.insert(physical_path.to_string(), record);
        self.mark_dirty(logical_path, physical_path, user_id);
        global_vfs_metrics().record_write_cache_enqueue();
        Ok(Some(file_info_from_pending(
            physical_path,
            data.len() as u64,
            modified_at,
        )))
    }

    pub async fn pending_stat(&self, physical_path: &str) -> Option<VfsFileInfo> {
        let entry = self
            .entries
            .get(physical_path)
            .map(|v| Arc::clone(v.value()))?;
        let inner = entry.inner.lock().await;
        Some(file_info_from_pending(
            physical_path,
            inner.size,
            inner.modified_at,
        ))
    }

    pub async fn pending_read(&self, physical_path: &str) -> Option<(Bytes, VfsFileInfo)> {
        let entry = self
            .entries
            .get(physical_path)
            .map(|v| Arc::clone(v.value()))?;
        let (blob, size, modified_at) = {
            let inner = entry.inner.lock().await;
            (inner.blob.clone(), inner.size, inner.modified_at)
        };
        let data = self.read_blob_bytes(&blob).await.ok()?;
        global_vfs_metrics().record_write_cache_pending_read();
        Some((
            data,
            file_info_from_pending(physical_path, size, modified_at),
        ))
    }

    pub async fn pending_children(&self, parent_physical_path: &str) -> Vec<VfsFileInfo> {
        let mut out = Vec::new();
        let entries: Vec<Arc<PendingWriteRecord>> = self
            .entries
            .iter()
            .filter(|entry| entry.value().parent_physical_path.as_ref() == parent_physical_path)
            .map(|entry| Arc::clone(entry.value()))
            .collect();
        for entry in entries {
            let inner = entry.inner.lock().await;
            out.push(file_info_from_pending(
                entry.physical_path.as_ref(),
                inner.size,
                inner.modified_at,
            ));
        }
        out
    }

    pub fn is_dirty_path(&self, physical_path: &str) -> bool {
        self.dirty_paths.contains_key(physical_path)
    }

    pub fn is_dirty_dir(&self, parent_physical_path: &str) -> bool {
        self.dirty_dirs.contains_key(parent_physical_path)
    }

    pub fn pending_quota_delta(&self, user_id: &str) -> i64 {
        self.quota_sync_users
            .get(user_id)
            .map(|entry| entry.value().delta)
            .unwrap_or(0)
    }

    pub async fn flush_path(&self, physical_path: &str) -> crate::vfs::VfsResult<()> {
        let Some(entry) = self
            .entries
            .get(physical_path)
            .map(|v| Arc::clone(v.value()))
        else {
            return Ok(());
        };
        self.flush_entry(entry, true).await
    }

    fn start_worker(self: &Arc<Self>) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            let semaphore = Arc::new(Semaphore::new(this.flush_concurrency));
            let mut interval =
                tokio::time::interval(std::time::Duration::from_millis(this.flush_interval_ms));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                let entries: Vec<Arc<PendingWriteRecord>> = this
                    .entries
                    .iter()
                    .map(|entry| Arc::clone(entry.value()))
                    .collect();
                for entry in entries {
                    if !this.is_due_for_background_flush(&entry).await {
                        continue;
                    }
                    let Ok(permit) = Arc::clone(&semaphore).try_acquire_owned() else {
                        break;
                    };
                    let this_clone = Arc::clone(&this);
                    tokio::spawn(async move {
                        let _permit = permit;
                        let _ = this_clone.flush_entry(entry, false).await;
                    });
                }
                let due_dir_keys: Vec<String> = this
                    .index_sync_dirs
                    .iter()
                    .filter_map(|entry| {
                        (entry.value().next_sync_at <= now_ts()).then(|| entry.key().clone())
                    })
                    .collect();
                for task_key in due_dir_keys {
                    let Some((_, task)) = this.index_sync_dirs.remove(&task_key) else {
                        continue;
                    };
                    let this_clone = Arc::clone(&this);
                    tokio::spawn(async move {
                        let _ = this_clone.sync_directory_index(task_key, task).await;
                    });
                }
                let due_quota_keys: Vec<String> = this
                    .quota_sync_users
                    .iter()
                    .filter_map(|entry| {
                        (entry.value().next_sync_at <= now_ts()).then(|| entry.key().clone())
                    })
                    .collect();
                for user_key in due_quota_keys {
                    let Some((_, task)) = this.quota_sync_users.remove(&user_key) else {
                        continue;
                    };
                    let this_clone = Arc::clone(&this);
                    tokio::spawn(async move {
                        let _ = this_clone.sync_user_quota(user_key, task).await;
                    });
                }
            }
        });
    }

    async fn flush_entry(
        &self,
        entry: Arc<PendingWriteRecord>,
        force: bool,
    ) -> crate::vfs::VfsResult<()> {
        let (blob, generation, size, modified_at, deadline_at) = {
            let mut inner = entry.inner.lock().await;
            if inner.state == PendingWriteState::Flushing {
                return Ok(());
            }
            if !force && inner.next_retry_at > now_ts() {
                return Ok(());
            }
            inner.state = PendingWriteState::Flushing;
            (
                inner.blob.clone(),
                inner.generation,
                inner.size,
                inner.modified_at,
                inner.deadline_at,
            )
        };
        let data = self
            .read_blob_bytes(&blob)
            .await
            .map_err(|err| crate::vfs::VfsError::Internal(err.to_string()))?;
        let previous_size = match self.primary.stat(entry.physical_path.as_ref()).await {
            Ok(meta) => meta.content_length() as i64,
            Err(_) => 0,
        };
        let write_result = self
            .primary
            .write(entry.physical_path.as_ref(), data.clone())
            .await;
        if let Err(err) = write_result {
            let mut inner = entry.inner.lock().await;
            inner.retry_count = inner.retry_count.saturating_add(1);
            inner.last_error = Some(err.to_string());
            inner.next_retry_at =
                now_ts() + self.retry_delay_secs(inner.retry_count, inner.abnormal_logged);
            if now_ts() > deadline_at {
                let promoted = self.promote_to_abnormal(&entry, &mut inner, &data).await;
                if promoted && !inner.abnormal_logged {
                    inner.abnormal_logged = true;
                    inner.state = PendingWriteState::Abnormal;
                    inner.next_retry_at = now_ts() + self.retry_delay_secs(inner.retry_count, true);
                    drop(inner);
                    global_vfs_metrics().record_write_cache_abnormal_spill();
                    self.log_abnormal_journal(&entry, &err.to_string()).await;
                    global_vfs_metrics().record_write_cache_flush_failure();
                    return Err(crate::vfs::VfsError::Internal(err.to_string()));
                }
            }
            inner.state = if inner.abnormal_logged {
                PendingWriteState::Abnormal
            } else {
                PendingWriteState::Pending
            };
            global_vfs_metrics().record_write_cache_flush_failure();
            return Err(crate::vfs::VfsError::Internal(err.to_string()));
        }

        let info = match self.primary.stat(entry.physical_path.as_ref()).await {
            Ok(meta) => VfsFileInfo {
                name: std::path::Path::new(entry.physical_path.as_ref())
                    .file_name()
                    .map(|v| v.to_string_lossy().to_string())
                    .unwrap_or_else(|| entry.physical_path.to_string())
                    .into(),
                path: Arc::clone(&entry.physical_path),
                is_dir: meta.is_dir(),
                size: meta.content_length(),
                modified: meta
                    .last_modified()
                    .map(|ts| std::time::SystemTime::from(ts).into()),
                favorite_color: 0,
                has_active_share: None,
                has_active_direct: None,
                trashed_at: None,
                original_path: None,
            },
            Err(_) => file_info_from_pending(entry.physical_path.as_ref(), size, modified_at),
        };

        {
            let mut inner = entry.inner.lock().await;
            if inner.generation != generation {
                inner.state = PendingWriteState::Pending;
                inner.next_retry_at = now_ts();
                return Ok(());
            }
            let accounted = self.accounted_bytes_for_blob(&inner);
            self.accounted_bytes.fetch_sub(accounted, Ordering::SeqCst);
            if let PendingWriteBlob::Disk(path) = &inner.blob {
                let _ = tokio::fs::remove_file(path).await;
                let _ = tokio::fs::remove_file(self.meta_path_for_blob(path)).await;
            }
        }
        self.entries.remove(entry.physical_path.as_ref());

        if let Some(read_cache) = &self.read_cache {
            read_cache
                .put(entry.physical_path.as_ref(), data, info.clone())
                .await;
        }
        let delta = info.size as i64 - previous_size;
        self.schedule_index_sync(&entry);
        self.schedule_quota_sync(entry.user_id.as_ref(), entry.logical_path.as_ref(), delta);
        global_vfs_metrics().record_write_cache_flush_success();
        Ok(())
    }

    async fn can_admit(&self, physical_path: &str, new_size: u64) -> bool {
        let existing_accounted = if let Some(entry) = self.entries.get(physical_path) {
            let guard = entry.value().inner.lock().await;
            let accounted = self.accounted_bytes_for_blob(&guard);
            drop(guard);
            accounted
        } else {
            0
        };
        self.accounted_bytes
            .load(Ordering::SeqCst)
            .saturating_sub(existing_accounted)
            .saturating_add(new_size)
            <= self.capacity_bytes
    }

    async fn update_existing_entry(
        &self,
        entry: &Arc<PendingWriteRecord>,
        physical_path: &str,
        data: Bytes,
        modified_at: i64,
        deadline_at: i64,
    ) -> anyhow::Result<()> {
        let mut inner = entry.inner.lock().await;
        let previous_accounted = self.accounted_bytes_for_blob(&inner);
        inner.generation = inner.generation.saturating_add(1);
        inner.size = data.len() as u64;
        inner.modified_at = modified_at;
        inner.deadline_at = deadline_at;
        inner.next_retry_at = modified_at;
        inner.state = if inner.abnormal_logged {
            PendingWriteState::Abnormal
        } else {
            PendingWriteState::Pending
        };
        inner.last_error = None;
        match (&self.backend, &inner.blob) {
            (WriteCacheBackend::Memory, PendingWriteBlob::Memory(_)) => {
                inner.blob = PendingWriteBlob::Memory(data);
            }
            (_, PendingWriteBlob::Disk(path)) => {
                let abnormal = path.starts_with(&self.abnormal_dir);
                tokio::fs::write(path, &data).await?;
                self.write_disk_meta(
                    path,
                    &PendingWriteDiskMeta {
                        user_id: entry.user_id.to_string(),
                        logical_path: entry.logical_path.to_string(),
                        physical_path: physical_path.to_string(),
                        size: data.len() as u64,
                        modified_at,
                        deadline_at,
                        generation: inner.generation,
                        abnormal,
                    },
                )
                .await?;
            }
            (WriteCacheBackend::LocalDir, PendingWriteBlob::Memory(_)) => {
                let blob = self
                    .store_blob(
                        &PendingWriteDiskMeta {
                            user_id: entry.user_id.to_string(),
                            logical_path: entry.logical_path.to_string(),
                            physical_path: physical_path.to_string(),
                            size: data.len() as u64,
                            modified_at,
                            deadline_at,
                            generation: inner.generation,
                            abnormal: false,
                        },
                        &data,
                    )
                    .await?;
                inner.blob = blob;
            }
        }
        let new_accounted = self.accounted_bytes_for_blob(&inner);
        if new_accounted >= previous_accounted {
            self.accounted_bytes
                .fetch_add(new_accounted - previous_accounted, Ordering::SeqCst);
        } else {
            self.accounted_bytes
                .fetch_sub(previous_accounted - new_accounted, Ordering::SeqCst);
        }
        Ok(())
    }

    async fn store_blob(
        &self,
        meta: &PendingWriteDiskMeta,
        data: &Bytes,
    ) -> anyhow::Result<PendingWriteBlob> {
        match self.backend {
            WriteCacheBackend::Memory if !meta.abnormal => Ok(PendingWriteBlob::Memory(data.clone())),
            _ => {
                let base_dir = if meta.abnormal {
                    &self.abnormal_dir
                } else {
                    &self.queue_dir
                };
                tokio::fs::create_dir_all(base_dir).await?;
                let file_path = data_path(base_dir, &meta.physical_path);
                tokio::fs::write(&file_path, data).await?;
                self.write_disk_meta(&file_path, meta).await?;
                Ok(PendingWriteBlob::Disk(file_path))
            }
        }
    }

    async fn read_blob_bytes(&self, blob: &PendingWriteBlob) -> anyhow::Result<Bytes> {
        match blob {
            PendingWriteBlob::Memory(data) => Ok(data.clone()),
            PendingWriteBlob::Disk(path) => Ok(Bytes::from(tokio::fs::read(path).await?)),
        }
    }

    async fn promote_to_abnormal(
        &self,
        entry: &Arc<PendingWriteRecord>,
        inner: &mut PendingWriteMutable,
        data: &Bytes,
    ) -> bool {
        if matches!(inner.blob, PendingWriteBlob::Disk(ref path) if path.starts_with(&self.abnormal_dir))
        {
            inner.state = PendingWriteState::Abnormal;
            return true;
        }
        let abnormal_path = data_path(&self.abnormal_dir, entry.physical_path.as_ref());
        let write_ok = tokio::fs::create_dir_all(&self.abnormal_dir).await.is_ok()
            && tokio::fs::write(&abnormal_path, data).await.is_ok()
            && self
                .write_disk_meta(
                    &abnormal_path,
                    &PendingWriteDiskMeta {
                        user_id: entry.user_id.to_string(),
                        logical_path: entry.logical_path.to_string(),
                        physical_path: entry.physical_path.to_string(),
                        size: inner.size,
                        modified_at: inner.modified_at,
                        deadline_at: inner.deadline_at,
                        generation: inner.generation,
                        abnormal: true,
                    },
                )
                .await
                .is_ok();
        if !write_ok {
            return false;
        }
        if let PendingWriteBlob::Disk(path) = &inner.blob {
            let _ = tokio::fs::remove_file(path).await;
            let _ = tokio::fs::remove_file(self.meta_path_for_blob(path)).await;
        }
        let previous_accounted = self.accounted_bytes_for_blob(inner);
        inner.blob = PendingWriteBlob::Disk(abnormal_path);
        inner.state = PendingWriteState::Abnormal;
        let current_accounted = self.accounted_bytes_for_blob(inner);
        if current_accounted >= previous_accounted {
            self.accounted_bytes
                .fetch_add(current_accounted - previous_accounted, Ordering::SeqCst);
        } else {
            self.accounted_bytes
                .fetch_sub(previous_accounted - current_accounted, Ordering::SeqCst);
        }
        true
    }

    async fn load_existing_entries(&self) {
        if !self.enabled {
            return;
        }
        if matches!(self.backend, WriteCacheBackend::LocalDir) {
            self.load_entries_from_dir(&self.queue_dir, false).await;
        }
        self.load_entries_from_dir(&self.abnormal_dir, true).await;
    }

    async fn load_entries_from_dir(&self, dir: &Path, abnormal: bool) {
        let Ok(mut read_dir) = tokio::fs::read_dir(dir).await else {
            return;
        };
        while let Ok(Some(entry)) = read_dir.next_entry().await {
            let path = entry.path();
            if path.extension().and_then(|v| v.to_str()) != Some("json") {
                continue;
            }
            let Ok(raw) = tokio::fs::read_to_string(&path).await else {
                continue;
            };
            let Ok(meta) = serde_json::from_str::<PendingWriteDiskMeta>(&raw) else {
                continue;
            };
            let blob_path = data_path(dir, &meta.physical_path);
            if tokio::fs::metadata(&blob_path).await.is_err() {
                let _ = tokio::fs::remove_file(&path).await;
                continue;
            }
            let record = Arc::new(PendingWriteRecord {
                user_id: Arc::from(meta.user_id.as_str()),
                logical_path: Arc::from(meta.logical_path.as_str()),
                physical_path: Arc::from(meta.physical_path.as_str()),
                parent_physical_path: parent_path(&meta.physical_path),
                inner: Mutex::new(PendingWriteMutable {
                    blob: PendingWriteBlob::Disk(blob_path),
                    size: meta.size,
                    modified_at: meta.modified_at,
                    deadline_at: meta.deadline_at,
                    next_retry_at: now_ts(),
                    generation: meta.generation,
                    retry_count: 0,
                    state: if abnormal {
                        PendingWriteState::Abnormal
                    } else {
                        PendingWriteState::Pending
                    },
                    abnormal_logged: abnormal,
                    last_error: None,
                }),
            });
            let guard = record.inner.lock().await;
            self.accounted_bytes
                .fetch_add(self.accounted_bytes_for_blob(&guard), Ordering::SeqCst);
            drop(guard);
            self.entries.insert(meta.physical_path.clone(), record);
            self.mark_dirty(&meta.logical_path, &meta.physical_path, &meta.user_id);
        }
    }

    fn accounted_bytes_for_blob(&self, inner: &PendingWriteMutable) -> u64 {
        match (&self.backend, &inner.blob) {
            (WriteCacheBackend::Memory, PendingWriteBlob::Memory(_)) => inner.size,
            (WriteCacheBackend::LocalDir, PendingWriteBlob::Disk(_)) => inner.size,
            _ => 0,
        }
    }

    async fn write_disk_meta(
        &self,
        file_path: &Path,
        meta: &PendingWriteDiskMeta,
    ) -> anyhow::Result<()> {
        let raw = serde_json::to_vec(meta)?;
        tokio::fs::write(self.meta_path_for_blob(file_path), raw).await?;
        Ok(())
    }

    fn meta_path_for_blob(&self, file_path: &Path) -> PathBuf {
        file_path.with_extension("json")
    }

    async fn log_abnormal_journal(&self, entry: &PendingWriteRecord, error: &str) {
        if let Some(recorder) = crate::vfs::get_global_cache_journal_recorder() {
            recorder
                .log_event(VfsJournalEvent {
                    user_id: entry.user_id.as_ref(),
                    action: "FILE:WRITE_CACHE_DEADLINE_EXCEEDED",
                    src: entry.logical_path.as_ref(),
                    dst: Some(entry.physical_path.as_ref()),
                    success: false,
                    error: Some(error.to_string()),
                })
                .await;
        }
    }

    fn mark_dirty(&self, logical_path: &str, physical_path: &str, user_id: &str) {
        self.dirty_paths.insert(physical_path.to_string(), ());
        self.dirty_dirs
            .insert(parent_path(physical_path).to_string(), ());
        let logical_parent_path = parent_path(logical_path);
        let physical_parent_path = parent_path(physical_path);
        let task_key = format!("{}\n{}", user_id, logical_parent_path);
        self.index_sync_dirs.insert(
            task_key,
            DirectoryIndexSyncTask {
                user_id: Arc::from(user_id),
                logical_parent_path,
                physical_parent_path,
                next_sync_at: now_ts() + 1,
                retry_count: 0,
            },
        );
    }

    fn schedule_index_sync(&self, entry: &PendingWriteRecord) {
        if !self.policy.allows(entry.logical_path.as_ref()) {
            return;
        }
        let logical_parent_path = parent_path(entry.logical_path.as_ref());
        let physical_parent_path = parent_path(entry.physical_path.as_ref());
        let task_key = format!("{}\n{}", entry.user_id, logical_parent_path);
        let next_sync_at = now_ts() + 1;
        self.index_sync_dirs
            .entry(task_key)
            .and_modify(|task| {
                task.next_sync_at = next_sync_at;
                task.retry_count = 0;
            })
            .or_insert_with(|| DirectoryIndexSyncTask {
                user_id: Arc::clone(&entry.user_id),
                logical_parent_path,
                physical_parent_path,
                next_sync_at,
                retry_count: 0,
            });
        global_vfs_metrics().record_index_sync_spawned();
    }

    fn schedule_quota_sync(&self, user_id: &str, logical_path: &str, delta: i64) {
        if delta == 0
            || logical_path.contains("/.thumbs")
            || logical_path.contains("/.thumbs_cache")
        {
            return;
        }
        let task_key = user_id.to_string();
        if let Some(mut task) = self.quota_sync_users.get_mut(&task_key) {
            task.delta += delta;
            task.next_sync_at = now_ts() + 1;
            task.retry_count = 0;
            let should_remove = task.delta == 0;
            drop(task);
            if should_remove {
                self.quota_sync_users.remove(&task_key);
            }
            global_vfs_metrics().record_quota_sync_scheduled();
            return;
        }
        self.quota_sync_users.insert(
            task_key,
            UserQuotaSyncTask {
                user_id: Arc::from(user_id),
                delta,
                next_sync_at: now_ts() + 1,
                retry_count: 0,
            },
        );
        global_vfs_metrics().record_quota_sync_scheduled();
    }

    async fn sync_directory_index(
        &self,
        task_key: String,
        task: DirectoryIndexSyncTask,
    ) -> anyhow::Result<()> {
        let entries = self
            .list_directory_entries(task.physical_parent_path.as_ref())
            .await?;
        let now = chrono::Utc::now();
        let models: Vec<file_index::ActiveModel> = entries
            .into_iter()
            .filter_map(|info| {
                let logical_path =
                    self.physical_to_logical_path(task.user_id.as_ref(), info.path.as_ref())?;
                if !self.policy.allows(&logical_path) {
                    return None;
                }
                Some(file_index::ActiveModel {
                    id: Set(uuid::Uuid::now_v7().to_string()),
                    user_id: Set(task.user_id.to_string()),
                    parent_path: Set(task.logical_parent_path.to_string()),
                    name: Set(info.name.to_string()),
                    path: Set(logical_path.clone()),
                    is_dir: Set(info.is_dir),
                    storage_id: Set(Some(self.pool_name.to_string())),
                    backend_type: Set(Some(self.backend_type.to_string())),
                    backend_key: Set(Some(info.path.to_string())),
                    size: Set(info.size as i64),
                    file_updated_at: Set(info.modified.map(|dt| dt.into())),
                    favorite_color: Set(0),
                    row_created_at: Set(now.into()),
                    row_updated_at: Set(now.into()),
                    ..Default::default()
                })
            })
            .collect();
        let index_service = FileIndexService::new(Arc::clone(&self.db));
        let chunk_size = crate::config::get_vfs_hub_config()
            .await
            .get_file_index()
            .get_effective_max_files_per_refresh() as usize;
        let rows = models.len() as u64;
        let chunk_count = rows.div_ceil(chunk_size.max(1) as u64);
        if let Err(err) = index_service
            .sync_directory_optimized(
                task.user_id.as_ref(),
                task.logical_parent_path.as_ref(),
                models,
                chunk_size,
            )
            .await
        {
            global_vfs_metrics().record_index_sync_failed();
            if !self.index_sync_dirs.contains_key(&task_key) {
                self.index_sync_dirs.insert(
                    task_key,
                    DirectoryIndexSyncTask {
                        next_sync_at: now_ts()
                            + self.retry_delay_secs(task.retry_count.saturating_add(1), false),
                        retry_count: task.retry_count.saturating_add(1),
                        ..task
                    },
                );
            }
            return Err(err.into());
        }
        global_vfs_metrics().record_index_sync_completed(rows, chunk_count);
        if !self.index_sync_dirs.contains_key(&task_key) {
            self.clear_dirty_markers(task.physical_parent_path.as_ref());
        }
        Ok(())
    }

    async fn sync_user_quota(
        &self,
        user_key: String,
        task: UserQuotaSyncTask,
    ) -> anyhow::Result<()> {
        if task.delta == 0 {
            return Ok(());
        }
        if let Err(err) =
            UserSettingsService::update_storage_used(&self.db, task.user_id.as_ref(), task.delta)
                .await
        {
            global_vfs_metrics().record_quota_sync_failed();
            self.quota_sync_users
                .entry(user_key)
                .and_modify(|current| {
                    current.delta += task.delta;
                    current.next_sync_at = now_ts()
                        + self.retry_delay_secs(current.retry_count.saturating_add(1), false);
                    current.retry_count = current.retry_count.saturating_add(1);
                })
                .or_insert(UserQuotaSyncTask {
                    user_id: Arc::clone(&task.user_id),
                    delta: task.delta,
                    next_sync_at: now_ts()
                        + self.retry_delay_secs(task.retry_count.saturating_add(1), false),
                    retry_count: task.retry_count.saturating_add(1),
                });
            return Err(err.into());
        }
        global_vfs_metrics().record_quota_sync_success();
        Ok(())
    }

    async fn list_directory_entries(
        &self,
        physical_parent_path: &str,
    ) -> anyhow::Result<Vec<VfsFileInfo>> {
        use futures::StreamExt;
        let list_path = if !physical_parent_path.is_empty() && !physical_parent_path.ends_with('/')
        {
            format!("{}/", physical_parent_path)
        } else {
            physical_parent_path.to_string()
        };
        let mut lister = self.primary.lister(&list_path).await?;
        let mut result = Vec::new();
        while let Some(entry_res) = lister.next().await {
            let entry = entry_res?;
            let meta = entry.metadata();
            let entry_path = normalize_entry_path(entry.path()).to_string();
            let info = if meta.last_modified().is_none()
                || (meta.is_file() && meta.content_length() == 0)
            {
                match self.primary.stat(entry.path()).await {
                    Ok(stat) => to_vfs_file_info(&entry_path, &stat),
                    Err(_) => to_vfs_file_info(&entry_path, meta),
                }
            } else {
                to_vfs_file_info(&entry_path, meta)
            };
            result.push(info);
        }
        Ok(result)
    }

    fn physical_to_logical_path(&self, user_id: &str, physical_path: &str) -> Option<String> {
        if physical_path == user_id {
            return Some("/".to_string());
        }
        let prefix = format!("{}/", user_id);
        let stripped = physical_path.strip_prefix(&prefix)?;
        Some(
            format!("/{}", stripped.trim_start_matches('/'))
                .trim_end_matches('/')
                .to_string(),
        )
    }

    fn clear_dirty_markers(&self, physical_parent_path: &str) {
        self.dirty_dirs.remove(physical_parent_path);
        let keys: Vec<String> = self
            .dirty_paths
            .iter()
            .filter_map(|entry| {
                let path = entry.key();
                let parent = Path::new(path)
                    .parent()
                    .map(|v| v.to_string_lossy().to_string())
                    .filter(|v| !v.is_empty())
                    .unwrap_or_else(|| "/".to_string());
                (parent == physical_parent_path).then(|| path.clone())
            })
            .collect();
        for key in keys {
            self.dirty_paths.remove(&key);
        }
    }

    fn retry_delay_secs(&self, retry_count: u32, abnormal_logged: bool) -> i64 {
        let base = (self.flush_interval_ms / 1000).max(1) as i64;
        let exp = 1_i64 << retry_count.saturating_sub(1).min(6);
        let delay = base.saturating_mul(exp);
        if abnormal_logged {
            delay.clamp(5, 300)
        } else {
            delay.clamp(1, 60)
        }
    }

    async fn is_due_for_background_flush(&self, entry: &Arc<PendingWriteRecord>) -> bool {
        let inner = entry.inner.lock().await;
        inner.state != PendingWriteState::Flushing && inner.next_retry_at <= now_ts()
    }
}
