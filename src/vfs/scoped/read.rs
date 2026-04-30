use super::ScopedVfsStorageEngine;
use crate::vfs::error::{VfsError, VfsResult};
use crate::vfs::{
    LOGICAL_TEMP_PREFIX, VfsFileInfo, VfsMetadata, VfsPaginationParams, VfsStorage,
    global_vfs_metrics,
};
use bytes::Bytes;
use dashmap::DashMap;
use futures::Stream;
use futures::StreamExt;
use futures::stream::BoxStream;
use std::cmp::Ordering;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::io::ReaderStream;

static INDEX_SYNC_SEMAPHORE: once_cell::sync::OnceCell<Arc<Semaphore>> =
    once_cell::sync::OnceCell::new();
static INDEX_SYNC_INFLIGHT: once_cell::sync::OnceCell<DashMap<String, ()>> =
    once_cell::sync::OnceCell::new();
static INDEX_SYNC_LAST_DONE: once_cell::sync::OnceCell<DashMap<String, i64>> =
    once_cell::sync::OnceCell::new();
const INDEX_SYNC_DEBOUNCE_SECS: i64 = 3;
const INDEX_SYNC_TIMEOUT: Duration = Duration::from_secs(24 * 3600);

#[inline]
fn list_stream_page_size() -> u64 {
    match yh_config_infra::utils::current_hardware_profile() {
        "low_memory" => 64,
        "throughput" => 512,
        _ => 128,
    }
}

enum ListStreamSource {
    Materialized(Vec<VfsFileInfo>),
    Indexed { user_id: String, normalized: String },
}

#[inline]
fn file_name_from_path(path: &str) -> &str {
    if let Some((_, name)) = path.rsplit_once('/') {
        name
    } else {
        path
    }
}

impl ScopedVfsStorageEngine {
    #[inline]
    pub(super) fn clear_index_sync_inflight(sync_key: &str) {
        if let Some(inflight) = INDEX_SYNC_INFLIGHT.get() {
            inflight.remove(sync_key);
        }
    }

    pub(super) fn clear_index_sync_debounce(user_id: &str, path: &str) {
        let sync_key = format!("{}\n{}", user_id, path);
        if let Some(last_done) = INDEX_SYNC_LAST_DONE.get() {
            last_done.remove(&sync_key);
        }
        Self::clear_index_sync_inflight(&sync_key);
    }

    async fn acquire_index_sync_permit() -> VfsResult<OwnedSemaphorePermit> {
        let semaphore = if let Some(existing) = INDEX_SYNC_SEMAPHORE.get() {
            Arc::clone(existing)
        } else {
            let cfg = crate::config::get_vfs_hub_config().await;
            let permits = cfg.get_file_index().get_effective_max_concurrent_refresh() as usize;
            let created = Arc::new(Semaphore::new(permits));
            let _ = INDEX_SYNC_SEMAPHORE.set(Arc::clone(&created));
            created
        };
        semaphore
            .acquire_owned()
            .await
            .map_err(|e| VfsError::Internal(format!("Acquire index sync permit failed: {}", e)))
    }

    pub(super) async fn read_impl(&self, path: &str) -> VfsResult<(Bytes, VfsFileInfo)> {
        let normalized = self.validate_file_operation(path).await?;
        if self.is_temp_path(&normalized) {
            let info = self.stat_impl(&normalized).await?;
            let rel = self.get_relative_path(&normalized, LOGICAL_TEMP_PREFIX);
            let data =
                tokio::fs::read(self.temp_manager.get_user_temp_dir(&self.user_id).join(rel))
                    .await
                    .map_err(VfsError::Io)?;
            Ok((Bytes::from(data), info))
        } else {
            if let Some(plan) = self.get_protected_plan(&normalized).await? {
                return self.protected_read_all_impl(&normalized, &plan).await;
            }
            if let Some(info) = self.pending_stat(&normalized).await {
                let (data, _) = self
                    .pool
                    .read(&self.get_physical_path(&normalized).await?)
                    .await?;
                return Ok((data, info));
            }
            let info = self.stat_impl(&normalized).await?;
            let data = self
                .pool
                .read(&self.get_physical_path(&normalized).await?)
                .await?
                .0;
            Ok((data, info))
        }
    }
    /// Executes index sync decision
    async fn execute_sync_decision(
        &self,
        normalized_path: &str,
    ) -> VfsResult<Option<Vec<VfsFileInfo>>> {
        if self.get_protected_plan(normalized_path).await?.is_some() {
            return Ok(None);
        }
        let vfs_cfg = crate::config::get_vfs_hub_config().await;
        let mode = vfs_cfg.get_file_index().get_vfs_sync_index_mode();
        match mode {
            1 => {
                let sync_key = format!("{}\n{}", self.user_id, normalized_path);
                let now = chrono::Utc::now().timestamp();
                let last_done = INDEX_SYNC_LAST_DONE.get_or_init(DashMap::new);
                if let Some(last) = last_done.get(&sync_key)
                    && now.saturating_sub(*last.value()) < INDEX_SYNC_DEBOUNCE_SECS
                {
                    global_vfs_metrics().record_index_sync_skipped_debounce();
                    return Ok(None);
                }
                let inflight = INDEX_SYNC_INFLIGHT.get_or_init(DashMap::new);
                match inflight.entry(sync_key.clone()) {
                    dashmap::mapref::entry::Entry::Occupied(_) => {
                        global_vfs_metrics().record_index_sync_skipped_inflight();
                        return Ok(None);
                    }
                    dashmap::mapref::entry::Entry::Vacant(entry) => {
                        entry.insert(());
                    }
                }
                global_vfs_metrics().record_index_sync_spawned();
                let self_clone = self.clone_for_async();
                let path_clone = normalized_path.to_string();
                tokio::spawn(async move {
                    let permit = Self::acquire_index_sync_permit().await;
                    if let Err(err) = permit {
                        yh_console_log::yhlog(
                            "error",
                            &format!("Background sync permit acquire failed: {}", err),
                        );
                        Self::clear_index_sync_inflight(&sync_key);
                        return;
                    }
                    let _permit = permit;
                    match tokio::time::timeout(
                        INDEX_SYNC_TIMEOUT,
                        self_clone.sync_index_internal(&path_clone, false),
                    )
                    .await
                    {
                        Ok(Ok(_)) => {}
                        Ok(Err(e)) => {
                            global_vfs_metrics().record_index_sync_failed();
                            yh_console_log::yhlog(
                                "error",
                                &format!("Background sync failed: {}", e),
                            );
                        }
                        Err(_) => {
                            global_vfs_metrics().record_index_sync_failed();
                            yh_console_log::yhlog(
                                "error",
                                &format!(
                                    "Background sync timed out after {:?}: {}",
                                    INDEX_SYNC_TIMEOUT, path_clone
                                ),
                            );
                        }
                    }
                    if let Some(last_done) = INDEX_SYNC_LAST_DONE.get() {
                        last_done.insert(sync_key.clone(), chrono::Utc::now().timestamp());
                    }
                    Self::clear_index_sync_inflight(&sync_key);
                });
                Ok(None)
            }
            2 => {
                let _permit = Self::acquire_index_sync_permit().await?;
                let fresh = self.sync_index_impl(normalized_path).await?;
                Ok(Some(fresh))
            }
            _ => Ok(None),
        }
    }
    pub(super) async fn list_impl(&self, path: &str) -> VfsResult<Vec<VfsFileInfo>> {
        let normalized = self.validate_file_operation(path).await?;
        let normalized = if normalized == "/" {
            normalized
        } else {
            normalized.trim_end_matches('/').to_string()
        };
        if self.is_temp_path(&normalized) {
            let rel = self.get_relative_path(&normalized, LOGICAL_TEMP_PREFIX);
            let mut entries =
                tokio::fs::read_dir(self.temp_manager.get_user_temp_dir(&self.user_id).join(rel))
                    .await
                    .map_err(VfsError::Io)?;
            let mut results = Vec::new();
            while let Some(entry) = entries.next_entry().await.map_err(VfsError::Io)? {
                let meta = entry.metadata().await.map_err(VfsError::Io)?;
                let file_name = entry.file_name();
                let file_name_str = file_name.to_string_lossy();
                results.push(VfsFileInfo {
                    name: file_name_str.as_ref().into(),
                    path: format!("{}/{}", normalized.trim_end_matches('/'), file_name_str).into(),
                    is_dir: meta.is_dir(),
                    size: meta.len(),
                    modified: Some(chrono::Utc::now()),
                    favorite_color: 0,
                    has_active_share: None,
                    has_active_direct: None,
                    trashed_at: None,
                    original_path: None,
                });
            }
            return Ok(results);
        }
        if self.get_protected_plan(&normalized).await?.is_some() {
            let db_entries = self
                .index_service
                .list_files(&self.user_id, &normalized)
                .await
                .map_err(|e| VfsError::Internal(e.to_string()))?;
            return Ok(db_entries
                .into_iter()
                .filter(|e| !self.is_hidden_storage_path(&e.path))
                .map(|e| VfsFileInfo {
                    name: e.name.into(),
                    path: e.path.into(),
                    is_dir: e.is_dir,
                    size: e.size as u64,
                    modified: e.file_updated_at.map(|t| t.into()),
                    favorite_color: e.favorite_color,
                    has_active_share: None,
                    has_active_direct: None,
                    trashed_at: e.file_trashed_at.map(|t| t.into()),
                    original_path: e.original_path.map(|p| p.into()),
                })
                .collect());
        }
        let physical = self.get_physical_path(&normalized).await?;
        let use_index = !self.pool.is_dirty_dir(&physical);
        if use_index {
            let _ = self.execute_sync_decision(&normalized).await?;
        }
        let mut results = if use_index {
            let db_entries = self
                .index_service
                .list_files(&self.user_id, &normalized)
                .await
                .map_err(|e| VfsError::Internal(e.to_string()))?;
            {
                let names: Vec<&str> = db_entries.iter().map(|e| e.name.as_str()).collect();
                yh_console_log::yhlog(
                    "info",
                    &format!(
                        "list_impl INDEX user_id={} path={} db_entries={:?}",
                        self.user_id, normalized, names
                    ),
                );
            }
            {
                let names: Vec<&str> = db_entries.iter().map(|e| e.name.as_str()).collect();
                yh_console_log::yhlog(
                    "info",
                    &format!(
                        "DEBUG list_impl index user_id={} path={} entries={:?}",
                        self.user_id, normalized, names
                    ),
                );
            }
            if !db_entries.is_empty() {
                let norm_path = if normalized == "/" {
                    "/"
                } else {
                    normalized.trim_end_matches('/')
                };
                let norm_path_slash = format!("{}/", norm_path);
                db_entries
                    .into_iter()
                    .map(|e| VfsFileInfo {
                        name: e.name.into(),
                        path: e.path.into(),
                        is_dir: e.is_dir,
                        size: e.size as u64,
                        modified: e.file_updated_at.map(|t| t.into()),
                        favorite_color: e.favorite_color,
                        has_active_share: None,
                        has_active_direct: None,
                        trashed_at: e.file_trashed_at.map(|t| t.into()),
                        original_path: e.original_path.map(|p| p.into()),
                    })
                    .filter(|translated| {
                        if self.is_hidden_storage_path(translated.path.as_ref()) {
                            return false;
                        }
                        let trans_path = translated.path.as_ref();
                        if trans_path == norm_path
                            || (norm_path != "/" && trans_path == norm_path_slash)
                        {
                            return false;
                        }
                        let parent = if let Some((p, _)) = trans_path.rsplit_once('/') {
                            if p.is_empty() { "/" } else { p }
                        } else {
                            return false;
                        };
                        parent == norm_path
                    })
                    .collect()
            } else {
                let physical_entries = self.pool.list(&physical).await?;
                let norm_path = if normalized == "/" {
                    "/"
                } else {
                    normalized.trim_end_matches('/')
                };
                let norm_path_slash = format!("{}/", norm_path);
                physical_entries
                    .into_iter()
                    .map(|e| self.translate_file_info(e, false))
                    .filter(|translated| {
                        if self.is_hidden_storage_path(translated.path.as_ref()) {
                            return false;
                        }
                        let trans_path = translated.path.as_ref();
                        if trans_path == norm_path
                            || (norm_path != "/" && trans_path == norm_path_slash)
                        {
                            return false;
                        }
                        let parent = if let Some((p, _)) = trans_path.rsplit_once('/') {
                            if p.is_empty() { "/" } else { p }
                        } else {
                            return false;
                        };
                        parent == norm_path
                    })
                    .collect()
            }
        } else {
            let physical_entries = self.pool.list(&physical).await?;
            let norm_path = if normalized == "/" {
                "/"
            } else {
                normalized.trim_end_matches('/')
            };
            let norm_path_slash = format!("{}/", norm_path);
            physical_entries
                .into_iter()
                .map(|e| self.translate_file_info(e, false))
                .filter(|translated| {
                    if self.is_hidden_storage_path(translated.path.as_ref()) {
                        return false;
                    }
                    let trans_path = translated.path.as_ref();
                    if trans_path == norm_path
                        || (norm_path != "/" && trans_path == norm_path_slash)
                    {
                        return false;
                    }
                    let parent = if let Some((p, _)) = trans_path.rsplit_once('/') {
                        if p.is_empty() { "/" } else { p }
                    } else {
                        return false;
                    };
                    parent == norm_path
                })
                .collect()
        };
        let pending_entries = self.pending_children(&normalized).await?;
        if pending_entries.is_empty() {
            return Ok(results);
        }
        let mut merged = std::collections::HashMap::<String, VfsFileInfo>::new();
        for entry in results.drain(..) {
            merged.insert(entry.path.to_string(), entry);
        }
        for entry in pending_entries {
            merged.insert(entry.path.to_string(), entry);
        }
        let mut merged_vec: Vec<VfsFileInfo> = merged.into_values().collect();
        merged_vec.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(merged_vec)
    }
    pub(super) fn list_stream_impl(
        &self,
        path: &str,
    ) -> BoxStream<'static, VfsResult<VfsFileInfo>> {
        use futures::StreamExt;
        let self_clone = self.clone_for_async();
        let index_service = Arc::clone(&self.index_service);
        let path_clone = path.to_string();
        Box::pin(
            futures::stream::once(async move {
                let normalized = self_clone.validate_file_operation(&path_clone).await?;
                let normalized = if normalized == "/" {
                    normalized
                } else {
                    normalized.trim_end_matches('/').to_string()
                };
                if self_clone.is_temp_path(&normalized) {
                    let entries = self_clone.list_impl(&normalized).await?;
                    return Ok(ListStreamSource::Materialized(entries));
                }
                if self_clone.get_protected_plan(&normalized).await?.is_some() {
                    let entries = self_clone.list_impl(&normalized).await?;
                    return Ok(ListStreamSource::Materialized(entries));
                }
                let physical = self_clone.get_physical_path(&normalized).await?;
                if !self_clone.pending_children(&normalized).await?.is_empty()
                    || self_clone.pool.is_dirty_dir(&physical)
                {
                    let entries = self_clone.list_impl(&normalized).await?;
                    return Ok(ListStreamSource::Materialized(entries));
                }
                if let Some(fresh) = self_clone.execute_sync_decision(&normalized).await? {
                    return Ok(ListStreamSource::Materialized(fresh));
                }
                Ok(ListStreamSource::Indexed {
                    user_id: self_clone.user_id.to_string(),
                    normalized,
                })
            })
            .flat_map(move |res| match res {
                Ok(ListStreamSource::Materialized(entries)) => {
                    futures::stream::iter(entries.into_iter().map(Ok)).boxed()
                }
                Ok(ListStreamSource::Indexed {
                    user_id,
                    normalized,
                }) => {
                    let index_service = Arc::clone(&index_service);
                    index_service
                        .list_files_stream_with_page_size(
                            user_id,
                            normalized,
                            list_stream_page_size(),
                        )
                        .map(|res| {
                            res.map(|e| VfsFileInfo {
                                name: e.name.into(),
                                path: e.path.into(),
                                is_dir: e.is_dir,
                                size: e.size as u64,
                                modified: e.file_updated_at.map(|t| t.into()),
                                favorite_color: e.favorite_color,
                                has_active_share: None,
                                has_active_direct: None,
                                trashed_at: e.file_trashed_at.map(|t| t.into()),
                                original_path: e.original_path.map(|p| p.into()),
                            })
                            .map_err(|e| VfsError::Internal(e.to_string()))
                        })
                        .boxed()
                }
                Err(e) => futures::stream::once(async { Err(e) }).boxed(),
            }),
        )
    }
    pub(super) async fn list_recursive_impl(&self, path: &str) -> VfsResult<Vec<VfsFileInfo>> {
        let normalized = self.validate_file_operation(path).await?;
        if self.is_temp_path(&normalized) {
            return self.list_impl(&normalized).await;
        }
        if self.get_protected_plan(&normalized).await?.is_some() {
            let entries = self
                .index_service
                .list_files_recursive(&self.user_id, &normalized)
                .await
                .map_err(|e| VfsError::Internal(e.to_string()))?;
            return Ok(entries
                .into_iter()
                .map(|e| VfsFileInfo {
                    name: e.name.into(),
                    path: e.path.into(),
                    is_dir: e.is_dir,
                    size: e.size as u64,
                    modified: e.file_updated_at.map(|t| t.into()),
                    favorite_color: e.favorite_color,
                    has_active_share: None,
                    has_active_direct: None,
                    trashed_at: e.file_trashed_at.map(|t| t.into()),
                    original_path: e.original_path.map(|p| p.into()),
                })
                .collect());
        }

        let physical = self.get_physical_path(&normalized).await?;
        if !self.pool.is_dirty_dir(&physical) {
            let entries = self
                .index_service
                .list_files_recursive(&self.user_id, &normalized)
                .await
                .map_err(|e| VfsError::Internal(e.to_string()))?;
            if !entries.is_empty() {
                return Ok(entries
                    .into_iter()
                    .map(|e| VfsFileInfo {
                        name: e.name.into(),
                        path: e.path.into(),
                        is_dir: e.is_dir,
                        size: e.size as u64,
                        modified: e.file_updated_at.map(|t| t.into()),
                        favorite_color: e.favorite_color,
                        has_active_share: None,
                        has_active_direct: None,
                        trashed_at: e.file_trashed_at.map(|t| t.into()),
                        original_path: e.original_path.map(|p| p.into()),
                    })
                    .collect());
            }
        }

        let norm_path = if normalized == "/" {
            "/".to_string()
        } else {
            normalized.trim_end_matches('/').to_string()
        };
        let norm_path_slash = format!("{}/", norm_path);
        Ok(self
            .pool
            .list_recursive(&physical)
            .await?
            .into_iter()
            .map(|entry| self.translate_file_info(entry, false))
            .filter(|entry| {
                let logical_path = entry.path.as_ref();
                !self.is_hidden_storage_path(logical_path)
                    && logical_path != norm_path
                    && logical_path != norm_path_slash
            })
            .collect())
    }
    pub(super) async fn exists_impl(&self, path: &str) -> VfsResult<bool> {
        let normalized = self.validate_file_operation(path).await?;
        if self.pending_stat(&normalized).await.is_some() {
            return Ok(true);
        }
        if self.is_temp_path(&normalized) {
            let rel = self.get_relative_path(&normalized, LOGICAL_TEMP_PREFIX);
            Ok(self
                .temp_manager
                .get_user_temp_dir(&self.user_id)
                .join(rel)
                .exists())
        } else {
            let physical = self.get_physical_path(&normalized).await?;
            if self.get_index_metadata(&normalized).await?.is_some() {
                return self.pool.exists(&physical).await;
            }
            if let Some(plan) = self.get_protected_plan(&normalized).await?
                && normalized == plan.root
            {
                return Ok(true);
            }
            self.pool.exists(&physical).await
        }
    }
    pub(super) async fn stat_impl(&self, path: &str) -> VfsResult<VfsFileInfo> {
        let normalized = self.validate_file_operation(path).await?;
        if self.is_temp_path(&normalized) {
            let rel = self.get_relative_path(&normalized, LOGICAL_TEMP_PREFIX);
            let local_path = self.temp_manager.get_user_temp_dir(&self.user_id).join(rel);
            let meta = tokio::fs::metadata(&local_path)
                .await
                .map_err(VfsError::Io)?;
            return Ok(VfsFileInfo {
                name: file_name_from_path(&normalized).into(),
                path: normalized.into(),
                is_dir: meta.is_dir(),
                size: meta.len(),
                modified: meta.modified().ok().map(Into::into),
                favorite_color: 0,
                has_active_share: None,
                has_active_direct: None,
                trashed_at: None,
                original_path: None,
            });
        }
        if let Some(info) = self.pending_stat(&normalized).await {
            return Ok(info);
        }
        if let Some(plan) = self.get_protected_plan(&normalized).await? {
            if let Some(e) = self.get_index_metadata(&normalized).await? {
                return Ok(VfsFileInfo {
                    name: e.name.into(),
                    path: e.path.into(),
                    is_dir: e.is_dir,
                    size: e.size as u64,
                    modified: e.file_updated_at.map(|t| t.into()),
                    favorite_color: e.favorite_color,
                    has_active_share: None,
                    has_active_direct: None,
                    trashed_at: e.file_trashed_at.map(|t| t.into()),
                    original_path: e.original_path.map(|p| p.into()),
                });
            }
            if normalized != plan.root {
                return Err(VfsError::NotFound(normalized));
            }
        }
        let physical = self.get_physical_path(&normalized).await?;
        if !self.pool.is_dirty_path(&physical)
            && let Ok(Some(e)) = self
                .index_service
                .get_file_metadata(&self.user_id, &normalized)
                .await
        {
            let physical_exists = self.pool.exists(&physical).await?;
            yh_console_log::yhlog(
                "info",
                &format!(
                    "VFS stat index probe user_id={} logical_path={} physical_path={} physical_exists={} is_dir={}",
                    self.user_id, normalized, physical, physical_exists, e.is_dir
                ),
            );
            if physical_exists {
                return Ok(VfsFileInfo {
                    name: e.name.into(),
                    path: e.path.into(),
                    is_dir: e.is_dir,
                    size: e.size as u64,
                    modified: e.file_updated_at.map(|t| t.into()),
                    favorite_color: e.favorite_color,
                    has_active_share: None,
                    has_active_direct: None,
                    trashed_at: e.file_trashed_at.map(|t| t.into()),
                    original_path: e.original_path.map(|p| p.into()),
                });
            }
            return Err(VfsError::NotFound(normalized));
        }
        let meta = self.pool.stat(&physical).await?;
        let info = VfsFileInfo {
            name: file_name_from_path(&normalized).into(),
            path: normalized.into(),
            is_dir: meta.is_dir,
            size: meta.size,
            modified: meta.modified,
            favorite_color: 0,
            has_active_share: None,
            has_active_direct: None,
            trashed_at: None,
            original_path: None,
        };
        Ok(info)
    }
    pub(super) async fn list_files_paginated_impl(
        &self,
        parent_path: &str,
        page: i64,
        page_size: i64,
    ) -> VfsResult<(Vec<VfsFileInfo>, i64)> {
        let normalized = self.validate_file_operation(parent_path).await?;
        if self.get_protected_plan(&normalized).await?.is_some() {
            let (entries, total) = self
                .index_service
                .list_files_paginated(&self.user_id, &normalized, page, page_size)
                .await
                .map_err(|e| VfsError::Internal(e.to_string()))?;
            let files = entries
                .into_iter()
                .filter(|e| !self.is_hidden_storage_path(&e.path))
                .map(|e| VfsFileInfo {
                    name: e.name.into(),
                    path: e.path.into(),
                    is_dir: e.is_dir,
                    size: e.size as u64,
                    modified: e.file_updated_at.map(|t| t.into()),
                    favorite_color: e.favorite_color,
                    has_active_share: None,
                    has_active_direct: None,
                    trashed_at: e.file_trashed_at.map(|t| t.into()),
                    original_path: e.original_path.map(|p| p.into()),
                })
                .collect();
            return Ok((files, total));
        }
        let physical = self.get_physical_path(&normalized).await?;
        if !self.pending_children(&normalized).await?.is_empty()
            || self.pool.is_dirty_dir(&physical)
        {
            let all = self.list_impl(&normalized).await?;
            let total = all.len() as i64;
            let offset = ((page - 1) * page_size).max(0) as usize;
            let files = all
                .into_iter()
                .skip(offset)
                .take(page_size.max(0) as usize)
                .collect();
            return Ok((files, total));
        }

        // Keep behavior consistent with `list_impl`: allow optional index refresh.
        // This helps self-heal when index is missing/outdated.
        let _ = self.execute_sync_decision(&normalized).await?;

        let (entries, total) = self
            .index_service
            .list_files_paginated(&self.user_id, &normalized, page, page_size)
            .await
            .map_err(|e| VfsError::Internal(e.to_string()))?;

        if total > 0 {
            let files = entries
                .into_iter()
                .filter(|e| !self.is_hidden_storage_path(&e.path))
                .map(|e| VfsFileInfo {
                    name: e.name.into(),
                    path: e.path.into(),
                    is_dir: e.is_dir,
                    size: e.size as u64,
                    modified: e.file_updated_at.map(|t| t.into()),
                    favorite_color: e.favorite_color,
                    has_active_share: None,
                    has_active_direct: None,
                    trashed_at: e.file_trashed_at.map(|t| t.into()),
                    original_path: e.original_path.map(|p| p.into()),
                })
                .collect();
            return Ok((files, total));
        }

        // Fallback: index might be empty (e.g. missing unique constraint / first run).
        // Use physical listing (via `list_impl`) + in-memory pagination.
        let all = self.list_impl(&normalized).await?;
        let total = all.len() as i64;
        let offset = ((page - 1) * page_size).max(0) as usize;
        let files = all
            .into_iter()
            .skip(offset)
            .take(page_size.max(0) as usize)
            .collect();
        Ok((files, total))
    }
    pub(super) async fn list_files_paginated_with_sort_impl(
        &self,
        parent_path: &str,
        params: VfsPaginationParams<'_>,
    ) -> VfsResult<(Vec<VfsFileInfo>, i64)> {
        let normalized = self.validate_file_operation(parent_path).await?;
        if self.get_protected_plan(&normalized).await?.is_some() {
            let (entries, total) = self
                .index_service
                .list_files_paginated_with_sort(&self.user_id, &normalized, params)
                .await
                .map_err(|e| VfsError::Internal(e.to_string()))?;
            let files = entries
                .into_iter()
                .map(|e| VfsFileInfo {
                    name: e.name.into(),
                    path: e.path.into(),
                    is_dir: e.is_dir,
                    size: e.size as u64,
                    modified: e.file_updated_at.map(|t| t.into()),
                    favorite_color: e.favorite_color,
                    has_active_share: None,
                    has_active_direct: None,
                    trashed_at: e.file_trashed_at.map(|t| t.into()),
                    original_path: e.original_path.map(|p| p.into()),
                })
                .collect();
            return Ok((files, total));
        }
        let params_for_fallback = params.clone();
        let physical = self.get_physical_path(&normalized).await?;
        if !self.pending_children(&normalized).await?.is_empty()
            || self.pool.is_dirty_dir(&physical)
        {
            let all = self.list_impl(&normalized).await?;
            return Self::paginate_with_sort_in_memory(all, params_for_fallback);
        }

        // Optional self-heal sync (background or blocking depending on config).
        // If sync returns a fresh listing (mode=2), use it directly.
        if let Some(fresh) = self.execute_sync_decision(&normalized).await? {
            return Self::paginate_with_sort_in_memory(fresh, params);
        }

        match self
            .index_service
            .list_files_paginated_with_sort(&self.user_id, &normalized, params)
            .await
        {
            Ok((entries, total)) => {
                if total > 0 {
                    let files = entries
                        .into_iter()
                        .map(|e| VfsFileInfo {
                            name: e.name.into(),
                            path: e.path.into(),
                            is_dir: e.is_dir,
                            size: e.size as u64,
                            modified: e.file_updated_at.map(|t| t.into()),
                            favorite_color: e.favorite_color,
                            has_active_share: None,
                            has_active_direct: None,
                            trashed_at: e.file_trashed_at.map(|t| t.into()),
                            original_path: e.original_path.map(|p| p.into()),
                        })
                        .collect();
                    return Ok((files, total));
                }

                // Fallback to physical listing when index is empty.
                let all = self.list_impl(&normalized).await?;
                Self::paginate_with_sort_in_memory(all, params_for_fallback)
            }
            Err(e) => {
                // If DB query fails, do not fail browsing. Use physical listing.
                yh_console_log::yhlog(
                    "warn",
                    &format!(
                        "VFS list_files_paginated_with_sort index query failed, fallback to physical list: user_id={} parent_path={} err={}",
                        self.user_id, normalized, e
                    ),
                );
                let all = self.list_impl(&normalized).await?;
                Self::paginate_with_sort_in_memory(all, params_for_fallback)
            }
        }
    }

    fn paginate_with_sort_in_memory(
        mut all: Vec<VfsFileInfo>,
        params: VfsPaginationParams<'_>,
    ) -> VfsResult<(Vec<VfsFileInfo>, i64)> {
        // Search filter
        if let Some(kw) = params.keyword
            && !kw.is_empty()
        {
            let kw_lower = kw.to_lowercase();
            all.retain(|e| {
                e.name.to_lowercase().contains(&kw_lower)
                    || e.path.to_lowercase().contains(&kw_lower)
            });
        }

        // Sorting (keep directories first, consistent with DB behavior).
        let sort_field = params.sort_by.unwrap_or("name");
        let order = params.order.unwrap_or("asc");
        let is_desc = order == "desc";

        all.sort_by(|a, b| {
            let dir_cmp = b.is_dir.cmp(&a.is_dir);
            if dir_cmp != Ordering::Equal {
                return dir_cmp;
            }
            let cmp = match sort_field {
                "size" => a.size.cmp(&b.size),
                "modified" => a.modified.cmp(&b.modified),
                "path" => a.path.cmp(&b.path),
                "created_at" => a.modified.cmp(&b.modified),
                _ => a.name.cmp(&b.name),
            };
            if is_desc { cmp.reverse() } else { cmp }
        });

        let total = all.len() as i64;
        let offset = ((params.page - 1) * params.page_size).max(0) as usize;
        let files = all
            .into_iter()
            .skip(offset)
            .take(params.page_size.max(0) as usize)
            .collect();
        Ok((files, total))
    }
    pub(super) async fn search_files_paginated_impl(
        &self,
        keyword: &str,
        page: i64,
        page_size: i64,
    ) -> VfsResult<(Vec<VfsFileInfo>, i64)> {
        let (entries, total) = self
            .index_service
            .search_files_paginated(&self.user_id, keyword, page, page_size)
            .await
            .map_err(|e| VfsError::Internal(e.to_string()))?;
        let files = entries
            .into_iter()
            .map(|e| VfsFileInfo {
                name: e.name.into(),
                path: e.path.into(),
                is_dir: e.is_dir,
                size: e.size as u64,
                modified: e.file_updated_at.map(|t| t.into()),
                favorite_color: e.favorite_color,
                has_active_share: None,
                has_active_direct: None,
                trashed_at: e.file_trashed_at.map(|t| t.into()),
                original_path: e.original_path.map(|p| p.into()),
            })
            .collect();
        Ok((files, total))
    }
    pub(super) async fn list_recycle_bin_paginated_impl(
        &self,
        page: i64,
        page_size: i64,
    ) -> VfsResult<(Vec<VfsFileInfo>, i64)> {
        let (entries, total) = self
            .index_service
            .list_trash_paginated(&self.user_id, page, page_size)
            .await
            .map_err(|e| VfsError::Internal(e.to_string()))?;
        let files = entries
            .into_iter()
            .map(|e| VfsFileInfo {
                name: e.name.into(),
                path: e.path.into(),
                is_dir: e.is_dir,
                size: e.size as u64,
                modified: e.file_updated_at.map(|t| t.into()),
                favorite_color: e.favorite_color,
                has_active_share: None,
                has_active_direct: None,
                trashed_at: e.file_trashed_at.map(|t| t.into()),
                original_path: e.original_path.map(|p| p.into()),
            })
            .collect();
        Ok((files, total))
    }
    pub(super) async fn list_recycle_bin_paginated_with_sort_impl(
        &self,
        params: VfsPaginationParams<'_>,
    ) -> VfsResult<(Vec<VfsFileInfo>, i64)> {
        match self
            .index_service
            .list_trash_paginated_with_sort(&self.user_id, params)
            .await
        {
            Ok((entries, total)) => {
                let files = entries
                    .into_iter()
                    .map(|e| VfsFileInfo {
                        name: e.name.into(),
                        path: e.path.into(),
                        is_dir: e.is_dir,
                        size: e.size as u64,
                        modified: e.file_updated_at.map(|t| t.into()),
                        favorite_color: e.favorite_color,
                        has_active_share: None,
                        has_active_direct: None,
                        trashed_at: e.file_trashed_at.map(|t| t.into()),
                        original_path: e.original_path.map(|p| p.into()),
                    })
                    .collect();
                Ok((files, total))
            }
            Err(e) => Err(VfsError::Internal(e.to_string())),
        }
    }
    pub(super) async fn read_stream_range_impl(
        &self,
        path: &str,
        range: std::ops::Range<u64>,
    ) -> VfsResult<(
        Pin<Box<dyn Stream<Item = VfsResult<Bytes>> + Send + Sync>>,
        VfsFileInfo,
    )> {
        let normalized = self.validate_file_operation(path).await?;
        if self.is_temp_path(&normalized) {
            let info = self.stat_impl(&normalized).await?;
            let rel = self.get_relative_path(&normalized, LOGICAL_TEMP_PREFIX);
            let local_path = self.temp_manager.get_user_temp_dir(&self.user_id).join(rel);
            let mut file = tokio::fs::File::open(local_path)
                .await
                .map_err(VfsError::Io)?;
            if range.start > 0 {
                file.seek(std::io::SeekFrom::Start(range.start))
                    .await
                    .map_err(VfsError::Io)?;
            }
            let remaining = info.size.saturating_sub(range.start);
            let length = if range.end == u64::MAX {
                remaining
            } else {
                remaining.min(range.end.saturating_sub(range.start))
            };
            let stream: Pin<Box<dyn Stream<Item = VfsResult<Bytes>> + Send + Sync>> = Box::pin(
                ReaderStream::new(file.take(length)).map(|item| item.map_err(VfsError::Io)),
            );
            return Ok((stream, info));
        }
        if let Some(plan) = self.get_protected_plan(&normalized).await? {
            return self
                .protected_read_stream_range_impl(&normalized, range, &plan)
                .await;
        }
        let info = if let Some(info) = self.pending_stat(&normalized).await {
            info
        } else {
            self.stat_impl(&normalized).await?
        };
        let (stream, _) = self
            .pool
            .read_stream_range(&self.get_physical_path(&normalized).await?, range)
            .await?;
        Ok((stream, info))
    }
    pub(super) async fn metadata_impl(&self, path: &str) -> VfsResult<VfsMetadata> {
        let normalized = self.validate_file_operation(path).await?;
        let info = self.stat_impl(&normalized).await?;
        Ok(VfsMetadata {
            path: info.path,
            is_dir: info.is_dir,
            size: info.size,
            modified: info.modified,
            content_type: None,
            etag: None,
        })
    }
    pub(super) async fn read_range_impl(
        &self,
        path: &str,
        start: u64,
        end: u64,
    ) -> VfsResult<(Bytes, VfsFileInfo)> {
        let normalized = self.validate_file_operation(path).await?;
        if self.is_temp_path(&normalized) {
            let info = self.stat_impl(&normalized).await?;
            let rel = self.get_relative_path(&normalized, LOGICAL_TEMP_PREFIX);
            let local_path = self.temp_manager.get_user_temp_dir(&self.user_id).join(rel);
            let mut file = tokio::fs::File::open(local_path)
                .await
                .map_err(VfsError::Io)?;
            if start > 0 {
                file.seek(std::io::SeekFrom::Start(start))
                    .await
                    .map_err(VfsError::Io)?;
            }
            let length = if end <= start {
                0
            } else {
                info.size.saturating_sub(start).min(end - start)
            };
            let mut buffer = Vec::with_capacity(length as usize);
            let mut limited = file.take(length);
            limited
                .read_to_end(&mut buffer)
                .await
                .map_err(VfsError::Io)?;
            return Ok((Bytes::from(buffer), info));
        }
        if let Some(plan) = self.get_protected_plan(&normalized).await? {
            return self
                .protected_read_range_impl(&normalized, start, end, &plan)
                .await;
        }
        let info = if let Some(info) = self.pending_stat(&normalized).await {
            info
        } else {
            self.stat_impl(&normalized).await?
        };
        let data = self
            .pool
            .read_range(&self.get_physical_path(&normalized).await?, start, end)
            .await?
            .0;
        Ok((data, info))
    }
}
