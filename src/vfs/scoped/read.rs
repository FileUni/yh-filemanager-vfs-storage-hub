// VFS Scoped Read Implementation
use super::ScopedVfsStorageEngine;
use crate::vfs::error::{VfsError, VfsResult};
use crate::vfs::{LOGICAL_TEMP_PREFIX, VfsFileInfo, VfsMetadata, VfsPaginationParams, VfsStorage};
use bytes::Bytes;
use futures::Stream;
use futures::stream::BoxStream;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

static INDEX_SYNC_SEMAPHORE: once_cell::sync::OnceCell<Arc<Semaphore>> = once_cell::sync::OnceCell::new();

#[inline]
fn file_name_from_path(path: &str) -> &str {
    if let Some((_, name)) = path.rsplit_once('/') { name } else { path }
}

impl ScopedVfsStorageEngine {
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
        semaphore.acquire_owned().await.map_err(|e| VfsError::Internal(format!("Acquire index sync permit failed: {}", e)))
    }

    pub(super) async fn read_impl(&self, path: &str) -> VfsResult<(Bytes, VfsFileInfo)> {
        let normalized = self.validate_file_operation(path).await?;
        let info = self.stat_impl(&normalized).await?;
        if self.is_temp_path(&normalized) {
            let rel = self.get_relative_path(&normalized, LOGICAL_TEMP_PREFIX);
            let data = tokio::fs::read(self.temp_manager.get_user_temp_dir(&self.user_id).join(rel)).await.map_err(VfsError::Io)?;
            Ok((Bytes::from(data), info))
        } else {
            let data = self.pool.read(&self.get_physical_path(&normalized).await?).await?.0;
            Ok((data, info))
        }
    }
    /// Executes index sync decision
    async fn execute_sync_decision(&self, normalized_path: &str) -> VfsResult<Option<Vec<VfsFileInfo>>> {
        let vfs_cfg = crate::config::get_vfs_hub_config().await;
        let mode = vfs_cfg.get_file_index().get_vfs_sync_index_mode();
        match mode {
            1 => {
                let self_clone = self.clone_for_async();
                let path_clone = normalized_path.to_string();
                tokio::spawn(async move {
                    let permit = Self::acquire_index_sync_permit().await;
                    if let Err(err) = permit {
                        yh_console_log::yhlog("error", &format!("Background sync permit acquire failed: {}", err));
                        return;
                    }
                    let _permit = permit;
                    if let Err(e) = self_clone.sync_index_impl(&path_clone).await {
                        yh_console_log::yhlog("error", &format!("Background sync failed: {}", e));
                    }
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
        if self.is_temp_path(&normalized) {
            let rel = self.get_relative_path(&normalized, LOGICAL_TEMP_PREFIX);
            let mut entries = tokio::fs::read_dir(self.temp_manager.get_user_temp_dir(&self.user_id).join(rel)).await.map_err(VfsError::Io)?;
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
        let _ = self.execute_sync_decision(&normalized).await?;
        let db_entries = self.index_service.list_files(&self.user_id, &normalized).await.map_err(|e| VfsError::Internal(e.to_string()))?;
        if !db_entries.is_empty() {
            return Ok(db_entries
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
        let physical_entries = self.pool.list(&physical).await?;
        Ok(physical_entries
            .into_iter()
            .map(|e| {
                let file_name = file_name_from_path(&e.path);
                VfsFileInfo {
                name: file_name.into(),
                path: format!(
                    "{}/{}",
                    normalized.trim_end_matches('/'),
                    file_name
                )
                .into(),
                is_dir: e.is_dir,
                size: e.size,
                modified: e.modified,
                favorite_color: 0,
                has_active_share: None,
                has_active_direct: None,
                trashed_at: None,
                original_path: None,
            }})
            .collect())
    }
    pub(super) fn list_stream_impl(&self, path: &str) -> BoxStream<'static, VfsResult<VfsFileInfo>> {
        use futures::StreamExt;
        let self_clone = self.clone_for_async();
        let path_clone = path.to_string();
        Box::pin(futures::stream::once(async move { self_clone.list_impl(&path_clone).await }).flat_map(|res| match res {
            Ok(entries) => futures::stream::iter(entries.into_iter().map(Ok)).boxed(),
            Err(e) => futures::stream::once(async { Err(e) }).boxed(),
        }))
    }
    pub(super) async fn list_recursive_impl(&self, path: &str) -> VfsResult<Vec<VfsFileInfo>> {
        let entries = self.index_service.list_files_recursive(&self.user_id, path).await.map_err(|e| VfsError::Internal(e.to_string()))?;
        Ok(entries
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
            .collect())
    }
    pub(super) async fn exists_impl(&self, path: &str) -> VfsResult<bool> {
        let normalized = self.validate_file_operation(path).await?;
        if self.is_temp_path(&normalized) {
            let rel = self.get_relative_path(&normalized, LOGICAL_TEMP_PREFIX);
            Ok(self.temp_manager.get_user_temp_dir(&self.user_id).join(rel).exists())
        } else {
            self.pool.exists(&self.get_physical_path(&normalized).await?).await
        }
    }
    pub(super) async fn stat_impl(&self, path: &str) -> VfsResult<VfsFileInfo> {
        let normalized = self.validate_file_operation(path).await?;
        if let Ok(Some(e)) = self.index_service.get_file_metadata(&self.user_id, &normalized).await {
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
        let physical = self.get_physical_path(&normalized).await?;
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
    pub(super) async fn list_files_paginated_impl(&self, parent_path: &str, page: i64, page_size: i64) -> VfsResult<(Vec<VfsFileInfo>, i64)> {
        let normalized = self.validate_file_operation(parent_path).await?;
        let (entries, total) = self.index_service.list_files_paginated(&self.user_id, &normalized, page, page_size).await.map_err(|e| VfsError::Internal(e.to_string()))?;
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
    pub(super) async fn list_files_paginated_with_sort_impl(&self, parent_path: &str, params: VfsPaginationParams<'_>) -> VfsResult<(Vec<VfsFileInfo>, i64)> {
        let normalized = self.validate_file_operation(parent_path).await?;
        match self.index_service.list_files_paginated_with_sort(&self.user_id, &normalized, params).await {
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
    pub(super) async fn search_files_paginated_impl(&self, keyword: &str, page: i64, page_size: i64) -> VfsResult<(Vec<VfsFileInfo>, i64)> {
        let (entries, total) = self.index_service.search_files_paginated(&self.user_id, keyword, page, page_size).await.map_err(|e| VfsError::Internal(e.to_string()))?;
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
    pub(super) async fn list_recycle_bin_paginated_impl(&self, page: i64, page_size: i64) -> VfsResult<(Vec<VfsFileInfo>, i64)> {
        let (entries, total) = self.index_service.list_trash_paginated(&self.user_id, page, page_size).await.map_err(|e| VfsError::Internal(e.to_string()))?;
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
    pub(super) async fn list_recycle_bin_paginated_with_sort_impl(&self, params: VfsPaginationParams<'_>) -> VfsResult<(Vec<VfsFileInfo>, i64)> {
        match self.index_service.list_trash_paginated_with_sort(&self.user_id, params).await {
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
    pub(super) async fn read_stream_range_impl(&self, path: &str, range: std::ops::Range<u64>) -> VfsResult<(Pin<Box<dyn Stream<Item = VfsResult<Bytes>> + Send + Sync>>, VfsFileInfo)> {
        let normalized = self.validate_file_operation(path).await?;
        let info = self.stat_impl(&normalized).await?;
        let (stream, _) = self.pool.read_stream_range(&self.get_physical_path(&normalized).await?, range).await?;
        Ok((stream, info))
    }
    pub(super) async fn metadata_impl(&self, path: &str) -> VfsResult<VfsMetadata> {
        let physical = self.get_physical_path(path).await?;
        self.pool.metadata(&physical).await
    }
    pub(super) async fn read_range_impl(&self, path: &str, start: u64, end: u64) -> VfsResult<(Bytes, VfsFileInfo)> {
        let normalized = self.validate_file_operation(path).await?;
        let info = self.stat_impl(&normalized).await?;
        let data = self.pool.read_range(&self.get_physical_path(&normalized).await?, start, end).await?.0;
        Ok((data, info))
    }
}
