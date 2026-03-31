use super::ScopedVfsStorageEngine;
use crate::vfs::error::{VfsError, VfsResult};
use crate::vfs::types::VfsBatchError;
use crate::vfs::{VfsBatchResult, VfsFileInfo, VfsStorage, global_vfs_metrics};
use futures::future::BoxFuture;
use std::sync::Arc;

#[inline]
fn favorites_page_size() -> u64 {
    match yh_config_infra::utils::current_hardware_profile() {
        "low_memory" => 64,
        "throughput" => 512,
        _ => 128,
    }
}

impl ScopedVfsStorageEngine {
    pub(super) async fn sync_index_internal(
        &self,
        path: &str,
        collect_entries: bool,
    ) -> VfsResult<Option<Vec<VfsFileInfo>>> {
        let normalized = self.validate_file_operation(path).await?;
        if self.get_protected_plan(&normalized).await?.is_some() {
            if !collect_entries {
                return Ok(None);
            }
            let db_entries = self
                .index_service
                .list_files(&self.user_id, &normalized)
                .await
                .map_err(|e| VfsError::Internal(e.to_string()))?;
            let files = db_entries
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
            return Ok(Some(files));
        }
        let guard = self.pool.sync_guards.get_guard(&self.user_id, &normalized);
        let _lock = guard.lock().await;
        let p_path = self.get_physical_path(&normalized).await?;
        let p_entries = self.pool.list(&p_path).await?;
        let now = chrono::Utc::now();
        let chunk_size = crate::config::get_vfs_hub_config()
            .await
            .get_file_index()
            .get_effective_max_files_per_refresh() as usize;
        let storage_id = self.pool.config.get_name().to_string();
        let backend_type = self.pool.get_backend_type();
        let norm_path = if normalized == "/" {
            "/"
        } else {
            normalized.trim_end_matches('/')
        };
        let norm_path_slash = format!("{}/", norm_path);
        let (txn, sync_start) = self
            .index_service
            .begin_directory_sync_txn()
            .await
            .map_err(|e| VfsError::Internal(e.to_string()))?;
        let mut active_models = Vec::with_capacity(chunk_size.max(1));
        let mut translated_entries = collect_entries.then(Vec::new);
        let mut synced_entry_count = 0_u64;
        let mut flushed_chunks = 0_u64;

        let normalized_owned = normalized.to_string();
        for e in p_entries {
            let backend_key = e.path.to_string();
            let translated = self.translate_file_info(e, false);
            if self.is_hidden_storage_path(translated.path.as_ref()) {
                continue;
            }
            let trans_path = translated.path.as_ref();
            if trans_path == norm_path || (norm_path != "/" && trans_path == norm_path_slash) {
                continue;
            }
            let parent = if let Some((p, _)) = trans_path.rsplit_once('/') {
                if p.is_empty() { "/" } else { p }
            } else {
                continue;
            };
            if parent != norm_path {
                continue;
            }
            let name = translated.name.to_string();
            let logical_path = translated.path.to_string();
            let is_dir = translated.is_dir;
            let size = translated.size as i64;
            let file_updated_at = translated.modified.as_ref().map(|dt| dt.to_owned().into());
            if let Some(entries) = translated_entries.as_mut() {
                entries.push(translated);
            }
            synced_entry_count += 1;
            active_models.push(crate::business::entities::file_index::ActiveModel {
                id: sea_orm::ActiveValue::Set(uuid::Uuid::now_v7().to_string()),
                user_id: sea_orm::ActiveValue::Set(self.user_id.to_string()),
                parent_path: sea_orm::ActiveValue::Set(normalized_owned.clone()),
                name: sea_orm::ActiveValue::Set(name),
                path: sea_orm::ActiveValue::Set(logical_path),
                is_dir: sea_orm::ActiveValue::Set(is_dir),
                storage_id: sea_orm::ActiveValue::Set(Some(storage_id.clone())),
                backend_type: sea_orm::ActiveValue::Set(Some(backend_type.clone())),
                backend_key: sea_orm::ActiveValue::Set(Some(backend_key)),
                size: sea_orm::ActiveValue::Set(size),
                file_updated_at: sea_orm::ActiveValue::Set(file_updated_at),
                favorite_color: sea_orm::ActiveValue::Set(0),
                row_created_at: sea_orm::ActiveValue::Set(now.into()),
                row_updated_at: sea_orm::ActiveValue::Set(now.into()),
                ..Default::default()
            });
            if active_models.len() >= chunk_size {
                self.index_service
                    .upsert_directory_chunk_txn(&txn, std::mem::take(&mut active_models))
                    .await
                    .map_err(|e| VfsError::Internal(e.to_string()))?;
                flushed_chunks += 1;
            }
        }
        self.index_service
            .upsert_directory_chunk_txn(&txn, active_models)
            .await
            .map_err(|e| VfsError::Internal(e.to_string()))?;
        if synced_entry_count > 0 {
            flushed_chunks += 1;
        }
        self.index_service
            .finish_directory_sync_txn(txn, &self.user_id, &normalized, sync_start)
            .await
            .map_err(|e| VfsError::Internal(e.to_string()))?;
        global_vfs_metrics().record_index_sync_completed(synced_entry_count, flushed_chunks);
        self.cache.invalidate("ls", &normalized).await;
        if let Some(mut entries) = translated_entries {
            entries.sort_by(|a, b| a.name.cmp(&b.name));
            Ok(Some(entries))
        } else {
            Ok(None)
        }
    }

    async fn move_regular_path_without_wal(&self, src: &str, dst: &str) -> VfsResult<VfsFileInfo> {
        if self.is_temp_path(src) || self.is_temp_path(dst) {
            return Err(VfsError::Internal(
                "Recycle bin operations do not support temp paths".to_string(),
            ));
        }
        self.flush_pending_write_cache_for_path(src).await?;
        self.flush_pending_write_cache_for_path(dst).await?;
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
        let stream = self.index_service.list_favorites_stream_with_page_size(
            self.user_id.to_string(),
            color_filter,
            favorites_page_size(),
        );
        let mut list = Vec::new();
        futures::pin_mut!(stream);
        while let Some(item) = futures::StreamExt::next(&mut stream).await {
            list.push(item.map_err(|e| VfsError::Internal(e.to_string()))?);
        }
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
        if self.is_protected_subdir_path(&normalized).await? {
            return Err(VfsError::Internal(
                "Recycle bin is disabled for protected subdirectory storage".to_string(),
            ));
        }
        if normalized.starts_with("/.recycle_bin/") {
            return Err(VfsError::Internal(
                "Path is already inside recycle bin".to_string(),
            ));
        }
        let info = self.stat_impl(&normalized).await?;
        let timestamp = chrono::Utc::now().timestamp();
        let trash_path = format!("/.recycle_bin/{}_{}", timestamp, info.name);
        if let Some(plan) = self.get_protected_plan(&normalized).await?
            && plan.root == "/"
            && !self.is_hidden_storage_path(&normalized)
        {
            let wal_id = self
                .begin_wal(
                    crate::vfs::wal::WalOperation::MoveToTrash {
                        path: normalized.to_string(),
                        trash_path: trash_path.clone(),
                    },
                    self.should_skip_wal_for_path(&normalized).await,
                )
                .await?;
            self.mark_wal_physical_done(wal_id).await;
            self.index_service
                .trash_file(&self.user_id, &normalized, &trash_path)
                .await
                .map_err(|e| VfsError::Internal(e.to_string()))?;
            self.complete_wal(wal_id).await;
            self.journal_log("MOVE_TO_TRASH", &normalized, Some(&trash_path), true, None)
                .await;
            let mut trashed_info = info.clone();
            trashed_info.path = trash_path.into();
            trashed_info.original_path = Some(normalized.into());
            trashed_info.trashed_at = Some(chrono::Utc::now());
            return Ok(trashed_info);
        }
        self.ensure_recycle_bin_initialized().await?;
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
            if let Some(plan) = self.get_protected_plan(&orig).await?
                && plan.root == "/"
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
                self.mark_wal_physical_done(wal_id).await;
                self.index_service
                    .restore_file(&self.user_id, &normalized, &orig)
                    .await
                    .map_err(|e| VfsError::Internal(e.to_string()))?;
                self.complete_wal(wal_id).await;
                self.journal_log("RESTORE_TRASH", &normalized, Some(&orig), true, None)
                    .await;
                return self.stat_impl(&orig).await;
            }
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
        Ok(self
            .sync_index_internal(path, true)
            .await?
            .unwrap_or_default())
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
        use futures::StreamExt;
        let concurrency = crate::config::get_vfs_hub_config()
            .await
            .get_batch_operation()
            .get_effective_max_concurrent_tasks()
            .max(1);
        let mut result = VfsBatchResult::default();
        let mut stream = futures::stream::iter(
            paths
                .iter()
                .map(std::borrow::ToOwned::to_owned)
                .map(|path| {
                    let engine = self.clone_for_async();
                    async move {
                        let result = engine.delete_impl(&path).await;
                        (path, result)
                    }
                }),
        )
        .buffer_unordered(concurrency);
        while let Some((path, op_result)) = stream.next().await {
            match op_result {
                Ok(_) => result.success.push(path.into()),
                Err(e) => result.failed.push(VfsBatchError {
                    path: path.into(),
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
        use futures::StreamExt;
        let concurrency = crate::config::get_vfs_hub_config()
            .await
            .get_batch_operation()
            .get_effective_max_concurrent_tasks()
            .max(1);
        let mut result = VfsBatchResult::default();
        let dst_dir = dst_dir.to_string();
        let mut stream =
            futures::stream::iter(src_paths.iter().map(std::borrow::ToOwned::to_owned).map(
                |src| {
                    let engine = self.clone_for_async();
                    let dst_dir = dst_dir.clone();
                    async move {
                        let filename = src.split('/').next_back().map_or("", |value| value);
                        let dst = format!("{}/{}", dst_dir.trim_end_matches('/'), filename);
                        let result = engine.move_file_impl(&src, &dst).await;
                        (src, result)
                    }
                },
            ))
            .buffer_unordered(concurrency);
        while let Some((src, op_result)) = stream.next().await {
            match op_result {
                Ok(_) => result.success.push(src.into()),
                Err(e) => result.failed.push(VfsBatchError {
                    path: src.into(),
                    error: e.to_string().into(),
                }),
            }
        }
        Ok(result)
    }
}
