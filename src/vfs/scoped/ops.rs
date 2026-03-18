use super::ScopedVfsStorageEngine;
use crate::vfs::error::{VfsError, VfsResult};
use crate::vfs::types::VfsBatchError;
use crate::vfs::{VfsBatchResult, VfsFileInfo, VfsStorage};
use futures::future::BoxFuture;
use std::sync::Arc;
impl ScopedVfsStorageEngine {
    pub(super) async fn set_favorite_impl(&self, path: &str, color: i32) -> VfsResult<VfsFileInfo> {
        let info = self.stat_impl(path).await?;
        let _ = self
            .index_service
            .set_favorite_color(&self.user_id, &info.path, color)
            .await;
        self.stat_impl(path).await
    }
    pub(super) async fn list_favorites_impl(
        &self,
        color_filter: Option<i32>,
    ) -> VfsResult<Vec<VfsFileInfo>> {
        let list = self
            .index_service
            .list_favorites(&self.user_id, color_filter)
            .await
            .map_err(|e| VfsError::Internal(e.to_string()))?;
        Ok(list
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
    /// Favorites list pagination implementation
    pub(super) async fn list_favorites_paginated_impl(
        &self,
        params: crate::vfs::VfsPaginationParams<'_>,
        color_filter: Option<i32>,
    ) -> VfsResult<(Vec<VfsFileInfo>, i64)> {
        let (list, total) = self
            .index_service
            .list_favorites_paginated(&self.user_id, params, color_filter)
            .await
            .map_err(|e| VfsError::Internal(e.to_string()))?;
        let files = list
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
    pub(super) async fn move_to_trash_impl(&self, path: &str) -> VfsResult<VfsFileInfo> {
        self.check_maintenance()?;
        let info = self.stat_impl(path).await?;
        let timestamp = chrono::Utc::now().timestamp();
        let trash_path = format!("/.recycle_bin/{}_{}", timestamp, info.name);
        let _ = self
            .index_service
            .trash_file(&self.user_id, &info.path, &trash_path)
            .await;
        self.move_file_impl(path, &trash_path).await
    }
    pub(super) async fn restore_from_trash_impl(&self, path: &str) -> VfsResult<VfsFileInfo> {
        self.check_maintenance()?;
        if let Ok(Some(meta)) = self
            .index_service
            .get_file_metadata(&self.user_id, path)
            .await
            && let Some(orig) = meta.original_path
        {
            let _ = self
                .index_service
                .restore_file(&self.user_id, path, &orig)
                .await;
            return self.move_file_impl(path, &orig).await;
        }
        Err(VfsError::Internal(
            "Not in trash or original path lost".to_string(),
        ))
    }
    pub(super) async fn list_trash_impl(&self) -> VfsResult<Vec<VfsFileInfo>> {
        let list = self
            .index_service
            .list_trash(&self.user_id)
            .await
            .map_err(|e| VfsError::Internal(e.to_string()))?;
        Ok(list
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
    pub(super) async fn sync_index_impl(&self, path: &str) -> VfsResult<Vec<VfsFileInfo>> {
        let normalized = self.validate_file_operation(path).await?;
        // Lock to avoid redundant syncs.
        let guard = self.pool.sync_guards.get_guard(&self.user_id, &normalized);
        let _lock = guard.lock().await;
        // Fetch physical listing.
        let p_path = self.get_physical_path(&normalized).await?;
        let p_entries = self.pool.list(&p_path).await?;
        // Prepare batch upsert.
        let mut active_models = Vec::with_capacity(p_entries.len());
        let now = chrono::Utc::now();
        let norm_path = if normalized == "/" {
            "/"
        } else {
            normalized.trim_end_matches('/')
        };
        let norm_path_slash = format!("{}/", norm_path);
        for e in p_entries {
            let translated = self.translate_file_info(e, false);
            if self.is_thumbnail_cache_path(translated.path.as_ref()) {
                continue;
            }
            // Filter out the directory itself and any out-of-scope paths
            let trans_path = translated.path.as_ref();
            if trans_path == norm_path || (norm_path != "/" && trans_path == norm_path_slash) {
                continue;
            }
            // Ensure trans_path is a direct child of norm_path
            let parent = if let Some((p, _)) = trans_path.rsplit_once('/') {
                if p.is_empty() { "/" } else { p }
            } else {
                continue;
            };
            if parent != norm_path {
                continue;
            }
            active_models.push(crate::business::entities::file_index::ActiveModel {
                id: sea_orm::ActiveValue::Set(uuid::Uuid::new_v4().to_string()),
                user_id: sea_orm::ActiveValue::Set(self.user_id.to_string()),
                parent_path: sea_orm::ActiveValue::Set(normalized.to_string()),
                name: sea_orm::ActiveValue::Set(translated.name.to_string()),
                path: sea_orm::ActiveValue::Set(translated.path.to_string()),
                is_dir: sea_orm::ActiveValue::Set(translated.is_dir),
                size: sea_orm::ActiveValue::Set(translated.size as i64),
                file_updated_at: sea_orm::ActiveValue::Set(translated.modified.map(|dt| dt.into())),
                row_updated_at: sea_orm::ActiveValue::Set(now.into()),
                ..Default::default()
            });
        }
        // Sync.
        self.index_service
            .sync_directory_optimized(&self.user_id, &normalized, active_models)
            .await
            .map_err(|e| VfsError::Internal(e.to_string()))?;
        // Invalidate cache.
        self.cache.invalidate("ls", &normalized).await;
        // Re-fetch merged list from DB.
        let merged_entries = self
            .index_service
            .list_files(&self.user_id, &normalized)
            .await
            .map_err(|e| VfsError::Internal(e.to_string()))?;
        Ok(merged_entries
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
    pub(super) fn get_recursive_size_impl(&self, path: &str) -> BoxFuture<'static, VfsResult<i64>> {
        let service = Arc::clone(&self.index_service);
        let uid = Arc::clone(&self.user_id);
        let p = path.to_string();
        Box::pin(async move {
            service
                .get_total_size(uid.as_ref(), &p)
                .await
                .map_err(|e| VfsError::Internal(e.to_string()))
        })
    }
    pub(super) async fn batch_remove_impl(&self, paths: &[String]) -> VfsResult<VfsBatchResult> {
        let mut result = VfsBatchResult::default();
        for p in paths {
            match self.delete_impl(p).await {
                Ok(_) => result.success.push(p.as_str().into()),
                Err(e) => result.failed.push(VfsBatchError {
                    path: p.as_str().into(),
                    error: e.to_string().into(),
                }),
            }
        }
        Ok(result)
    }
    pub(super) async fn batch_move_impl(
        &self,
        src_paths: &[String],
        dst_dir: &str,
    ) -> VfsResult<VfsBatchResult> {
        let mut result = VfsBatchResult::default();
        for s in src_paths {
            let filename = s.split('/').next_back().map_or("", |value| value);
            let d = format!("{}/{}", dst_dir.trim_end_matches('/'), filename);
            match self.move_file_impl(s, &d).await {
                Ok(_) => result.success.push(s.as_str().into()),
                Err(e) => result.failed.push(VfsBatchError {
                    path: s.as_str().into(),
                    error: e.to_string().into(),
                }),
            }
        }
        Ok(result)
    }
}
