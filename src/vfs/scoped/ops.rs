use super::ScopedVfsStorageEngine;
use crate::vfs::error::{VfsError, VfsResult};
use crate::vfs::types::VfsBatchError;
use crate::vfs::{VfsBatchResult, VfsFileInfo, VfsStorage};
use futures::future::BoxFuture;
use std::sync::Arc;
impl ScopedVfsStorageEngine {
    async fn move_regular_path_without_wal(&self, src: &str, dst: &str) -> VfsResult<VfsFileInfo> {
        if self.is_temp_path(src) || self.is_temp_path(dst) {
            return Err(VfsError::Internal(
                "Recycle bin operations do not support temp paths".to_string(),
            ));
        }
        self.pool
            .move_file(
                &self.get_physical_path(src).await?,
                &self.get_physical_path(dst).await?,
            )
            .await?;
        self.cache.invalidate_parent_ls(src).await;
        self.cache.invalidate_parent_ls(dst).await;
        self.stat_impl(dst).await
    }
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
        let normalized = self.validate_file_operation(path).await?;
        if normalized.starts_with("/.recycle_bin/") {
            return Err(VfsError::Internal(
                "Path is already inside recycle bin".to_string(),
            ));
        }
        let info = self.stat_impl(&normalized).await?;
        let timestamp = chrono::Utc::now().timestamp();
        let trash_path = format!("/.recycle_bin/{}_{}", timestamp, info.name);
        if !self.exists_impl("/.recycle_bin").await.unwrap_or(false) {
            self.pool
                .create_dir_all(&self.get_physical_path("/.recycle_bin").await?)
                .await?;
        }
        let wal_id = self
            .begin_wal(
                crate::vfs::wal::WalOperation::MoveToTrash {
                    path: normalized.to_string(),
                    trash_path: trash_path.clone(),
                },
                self.should_skip_wal_for_path(&normalized).await,
            )
            .await?;
        let result = self
            .move_regular_path_without_wal(&normalized, &trash_path)
            .await;
        match result {
            Ok(info) => {
                self.mark_wal_physical_done(wal_id).await;
                if let Err(err) = self
                    .index_service
                    .trash_file(&self.user_id, &normalized, &trash_path)
                    .await
                {
                    self.fail_wal(
                        wal_id,
                        &format!(
                            "MOVE_TO_TRASH metadata sync failed for {} -> {}: {}",
                            normalized, trash_path, err
                        ),
                    )
                    .await;
                    return Err(VfsError::Internal(err.to_string()));
                }
                self.mark_wal_metadata_done(wal_id).await;
                self.complete_wal(wal_id).await;
                self.journal_log("MOVE_TO_TRASH", &normalized, Some(&trash_path), true, None)
                    .await;
                Ok(info)
            }
            Err(err) => {
                self.fail_wal(wal_id, &err.to_string()).await;
                self.journal_log(
                    "MOVE_TO_TRASH",
                    &normalized,
                    Some(&trash_path),
                    false,
                    Some(err.to_string()),
                )
                .await;
                Err(err)
            }
        }
    }
    pub(super) async fn restore_from_trash_impl(&self, path: &str) -> VfsResult<VfsFileInfo> {
        self.check_maintenance()?;
        let normalized = self.validate_file_operation(path).await?;
        if !normalized.starts_with("/.recycle_bin/") {
            return Err(VfsError::Internal(
                "Path is not inside recycle bin".to_string(),
            ));
        }
        if let Ok(Some(meta)) = self
            .index_service
            .get_file_metadata(&self.user_id, &normalized)
            .await
            && let Some(orig) = meta.original_path
        {
            let wal_id = self
                .begin_wal(
                    crate::vfs::wal::WalOperation::RestoreTrash {
                        trash_path: normalized.to_string(),
                        original_path: orig.to_string(),
                    },
                    self.should_skip_wal_for_path(&normalized).await,
                )
                .await?;
            let result = self.move_regular_path_without_wal(&normalized, &orig).await;
            return match result {
                Ok(info) => {
                    self.mark_wal_physical_done(wal_id).await;
                    if let Err(err) = self
                        .index_service
                        .restore_file(&self.user_id, &normalized, &orig)
                        .await
                    {
                        self.fail_wal(
                            wal_id,
                            &format!(
                                "RESTORE_TRASH metadata sync failed for {} -> {}: {}",
                                normalized, orig, err
                            ),
                        )
                        .await;
                        Err(VfsError::Internal(err.to_string()))
                    } else {
                        self.mark_wal_metadata_done(wal_id).await;
                        self.complete_wal(wal_id).await;
                        self.journal_log("RESTORE_TRASH", &normalized, Some(&orig), true, None)
                            .await;
                        Ok(info)
                    }
                }
                Err(err) => {
                    self.fail_wal(wal_id, &err.to_string()).await;
                    self.journal_log(
                        "RESTORE_TRASH",
                        &normalized,
                        Some(&orig),
                        false,
                        Some(err.to_string()),
                    )
                    .await;
                    Err(err)
                }
            };
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
        let storage_id = self.pool.config.get_name().to_string();
        let backend_type = self.pool.get_backend_type();
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
            let backend_key = self.get_physical_path(trans_path).await?;
            active_models.push(crate::business::entities::file_index::ActiveModel {
                id: sea_orm::ActiveValue::Set(uuid::Uuid::now_v7().to_string()),
                user_id: sea_orm::ActiveValue::Set(self.user_id.to_string()),
                parent_path: sea_orm::ActiveValue::Set(normalized.to_string()),
                name: sea_orm::ActiveValue::Set(translated.name.to_string()),
                path: sea_orm::ActiveValue::Set(translated.path.to_string()),
                is_dir: sea_orm::ActiveValue::Set(translated.is_dir),
                storage_id: sea_orm::ActiveValue::Set(Some(storage_id.clone())),
                backend_type: sea_orm::ActiveValue::Set(Some(backend_type.clone())),
                backend_key: sea_orm::ActiveValue::Set(Some(backend_key)),
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
