use super::read_cache::ReadCacheManager;
use crate::business::services::{FileIndexService, user_settings::UserSettingsService};
use crate::config::VfsWriteCacheConfig;
use crate::vfs::{VfsFileInfo, VfsJournalEvent};
use bytes::Bytes;
use dashmap::DashMap;
use opendal::Operator;
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
    accounted_bytes: AtomicU64,
    entries: DashMap<String, Arc<PendingWriteRecord>>,
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
            accounted_bytes: AtomicU64::new(0),
            entries: DashMap::new(),
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

    pub fn should_cache(&self, size: usize) -> bool {
        self.enabled
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
        if !self.should_cache(data.len()) {
            return Ok(None);
        }
        if let Some(read_cache) = &self.read_cache {
            read_cache.remove(physical_path).await;
        }
        if !self.can_admit(physical_path, data.len() as u64).await {
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
            let size = existing.inner.lock().await.size;
            return Ok(Some(file_info_from_pending(
                physical_path,
                size,
                modified_at,
            )));
        }
        let blob = self
            .store_blob(
                physical_path,
                user_id,
                logical_path,
                modified_at,
                deadline_at,
                1,
                false,
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
        let inner = entry.inner.lock().await;
        let data = self.read_blob_bytes(&inner.blob).await.ok()?;
        Some((
            data,
            file_info_from_pending(physical_path, inner.size, inner.modified_at),
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

    pub async fn flush_path(&self, physical_path: &str) -> crate::vfs::VfsResult<()> {
        let Some(entry) = self
            .entries
            .get(physical_path)
            .map(|v| Arc::clone(v.value()))
        else {
            return Ok(());
        };
        self.flush_entry(entry).await
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
                    let Ok(permit) = Arc::clone(&semaphore).try_acquire_owned() else {
                        break;
                    };
                    let this_clone = Arc::clone(&this);
                    tokio::spawn(async move {
                        let _permit = permit;
                        let _ = this_clone.flush_entry(entry).await;
                    });
                }
            }
        });
    }

    async fn flush_entry(&self, entry: Arc<PendingWriteRecord>) -> crate::vfs::VfsResult<()> {
        let (blob, generation, size, modified_at, deadline_at) = {
            let mut inner = entry.inner.lock().await;
            if inner.state == PendingWriteState::Flushing {
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
            if now_ts() > deadline_at {
                let promoted = self.promote_to_abnormal(&entry, &mut inner, &data).await;
                if promoted && !inner.abnormal_logged {
                    inner.abnormal_logged = true;
                    inner.state = PendingWriteState::Abnormal;
                    drop(inner);
                    self.log_abnormal_journal(&entry, &err.to_string()).await;
                    return Err(crate::vfs::VfsError::Internal(err.to_string()));
                }
            }
            inner.state = if inner.abnormal_logged {
                PendingWriteState::Abnormal
            } else {
                PendingWriteState::Pending
            };
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
        if !entry.logical_path.starts_with("/.thumbs")
            && !entry.logical_path.starts_with("/.thumbs_cache")
        {
            let index_service = FileIndexService::new(Arc::clone(&self.db));
            let _ = index_service
                .upsert_file_with_location(
                    entry.user_id.as_ref(),
                    entry.logical_path.as_ref(),
                    &VfsFileInfo {
                        path: Arc::clone(&entry.logical_path),
                        ..info.clone()
                    },
                    Some(self.pool_name.as_ref()),
                    Some(self.backend_type.as_ref()),
                    Some(entry.physical_path.as_ref()),
                )
                .await;
            if delta != 0 {
                let _ = UserSettingsService::update_storage_used(
                    self.db.as_ref(),
                    entry.user_id.as_ref(),
                    delta,
                )
                .await;
            }
        }
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
                        physical_path,
                        entry.user_id.as_ref(),
                        entry.logical_path.as_ref(),
                        modified_at,
                        deadline_at,
                        inner.generation,
                        false,
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
        physical_path: &str,
        user_id: &str,
        logical_path: &str,
        modified_at: i64,
        deadline_at: i64,
        generation: u64,
        abnormal: bool,
        data: &Bytes,
    ) -> anyhow::Result<PendingWriteBlob> {
        match self.backend {
            WriteCacheBackend::Memory if !abnormal => Ok(PendingWriteBlob::Memory(data.clone())),
            _ => {
                let base_dir = if abnormal {
                    &self.abnormal_dir
                } else {
                    &self.queue_dir
                };
                tokio::fs::create_dir_all(base_dir).await?;
                let file_path = data_path(base_dir, physical_path);
                tokio::fs::write(&file_path, data).await?;
                self.write_disk_meta(
                    &file_path,
                    &PendingWriteDiskMeta {
                        user_id: user_id.to_string(),
                        logical_path: logical_path.to_string(),
                        physical_path: physical_path.to_string(),
                        size: data.len() as u64,
                        modified_at,
                        deadline_at,
                        generation,
                        abnormal,
                    },
                )
                .await?;
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
}
