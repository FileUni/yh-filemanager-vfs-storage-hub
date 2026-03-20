use super::ScopedVfsStorageEngine;
use crate::vfs::error::{VfsError, VfsResult};
use crate::vfs::wal::WalOperation;
use crate::vfs::{LOGICAL_TEMP_PREFIX, VfsFileInfo, VfsStorage};
use bytes::Bytes;
use futures::stream::BoxStream;
impl ScopedVfsStorageEngine {
    pub(super) async fn write_impl(&self, path: &str, data: Bytes) -> VfsResult<VfsFileInfo> {
        self.check_maintenance()?;
        let normalized = self.validate_file_operation(path).await?;
        if self.is_refresh_trigger(&normalized).await {
            return self.stat_impl(&normalized).await;
        }
        // Check quota difference
        let skip_quota = self.is_thumbnail_cache_path(&normalized);
        let mut diff = 0i64;
        if !skip_quota {
            let current_size = self.get_file_size(&normalized).await;
            diff = data.len() as i64 - current_size;
            if diff > 0 {
                self.check_quota(diff).await?;
            }
        }
        let wal_id = self
            .begin_wal(
                WalOperation::Write {
                    path: normalized.to_string(),
                    size: data.len() as u64,
                },
                self.should_skip_wal_for_write(&normalized, data.len() as u64)
                    .await,
            )
            .await?;
        let result = if self.is_temp_path(&normalized) {
            let rel = self.get_relative_path(&normalized, LOGICAL_TEMP_PREFIX);
            let local_path = self.temp_manager.get_user_temp_dir(&self.user_id).join(rel);
            if let Some(parent) = local_path.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(VfsError::Io)?;
            }
            tokio::fs::write(&local_path, data)
                .await
                .map_err(VfsError::Io)?;
            self.stat_impl(path).await
        } else {
            match self
                .pool
                .write(&self.get_physical_path(&normalized).await?, data)
                .await
            {
                Ok(info) => {
                    let translated = self.translate_file_info(info, false);
                    self.upsert_index_helper(&normalized, &translated).await?;
                    Ok(translated)
                }
                Err(e) => Err(e),
            }
        };
        match &result {
            Ok(_) => {
                self.complete_wal(wal_id).await;
                // Update quota
                if !skip_quota && diff != 0 {
                    let _ = self.update_quota(diff).await;
                }
                self.journal_log("WRITE", &normalized, None, true, None)
                    .await
            }
            Err(e) => {
                self.journal_log("WRITE", &normalized, None, false, Some(e.to_string()))
                    .await
            }
        }
        result
    }
    pub(super) async fn delete_impl(&self, path: &str) -> VfsResult<VfsFileInfo> {
        self.check_maintenance()?;
        let normalized = self.validate_file_operation(path).await?;
        let info = self.stat_impl(&normalized).await?;
        // Record size before deletion for quota update
        let size = info.size as i64;
        let skip_quota = self.is_thumbnail_cache_path(&normalized);
        let wal_id = self
            .begin_wal(
                WalOperation::Delete {
                    path: normalized.to_string(),
                },
                self.should_skip_wal_for_path(&normalized).await,
            )
            .await?;
        if self.is_temp_path(&normalized) {
            let local_path = self
                .temp_manager
                .get_user_temp_dir(&self.user_id)
                .join(self.get_relative_path(&normalized, LOGICAL_TEMP_PREFIX));
            if local_path.is_dir() {
                tokio::fs::remove_dir_all(local_path)
                    .await
                    .map_err(VfsError::Io)?;
            } else {
                tokio::fs::remove_file(local_path)
                    .await
                    .map_err(VfsError::Io)?;
            }
            self.complete_wal(wal_id).await;
            return Ok(info);
        }
        match self
            .pool
            .delete(&self.get_physical_path(&normalized).await?)
            .await
        {
            Ok(_) => {
                self.complete_wal(wal_id).await;
                let _ = self
                    .index_service
                    .delete_file(&self.user_id, &normalized)
                    .await;
                self.cache.invalidate_parent_ls(&normalized).await;
                // Update quota (decrease)
                if !skip_quota {
                    let _ = self.update_quota(-size).await;
                }
                self.journal_log("DELETE", &normalized, None, true, None)
                    .await;
                Ok(info)
            }
            Err(e) => {
                self.journal_log("DELETE", &normalized, None, false, Some(e.to_string()))
                    .await;
                Err(e)
            }
        }
    }
    pub(super) async fn write_stream_impl(
        &self,
        path: &str,
        stream: BoxStream<'static, VfsResult<Bytes>>,
    ) -> VfsResult<VfsFileInfo> {
        self.check_maintenance()?;
        let normalized = self.validate_file_operation(path).await?;
        self.journal_log("WRITE_STREAM", &normalized, None, true, None)
            .await;
        if self.is_refresh_trigger(&normalized).await {
            let parent = if let Some(parent_path) = std::path::Path::new(&normalized).parent() {
                parent_path.to_string_lossy().to_string()
            } else {
                "/".to_string()
            };
            let _ = self.sync_index_impl(&parent).await?;
            return self.stat_impl(&parent).await;
        }
        let result = self.write_stream_internal(&normalized, stream).await;
        match &result {
            Ok(_) => {
                self.journal_log("WRITE_STREAM_FINISH", &normalized, None, true, None)
                    .await
            }
            Err(e) => {
                self.journal_log(
                    "WRITE_STREAM_FINISH",
                    &normalized,
                    None,
                    false,
                    Some(e.to_string()),
                )
                .await
            }
        }
        result
    }
    pub(super) async fn write_at_impl(
        &self,
        path: &str,
        offset: u64,
        data: Bytes,
    ) -> VfsResult<VfsFileInfo> {
        self.check_maintenance()?;
        let normalized = self.validate_file_operation(path).await?;
        if self.is_refresh_trigger(&normalized).await {
            return self.write_impl(path, data).await;
        }
        if self.is_temp_path(&normalized) {
            return Err(VfsError::Internal("Not supported for temp".to_string()));
        }
        let physical_path = self.get_physical_path(&normalized).await?;
        // Get current size to calculate quota difference
        let skip_quota = self.is_thumbnail_cache_path(&normalized);
        let mut diff = 0i64;
        if !skip_quota {
            let current_size = self.get_file_size(&normalized).await;
            let new_end = offset + data.len() as u64;
            diff = if new_end > current_size as u64 {
                (new_end - current_size as u64) as i64
            } else {
                0
            };
            if diff > 0 {
                self.check_quota(diff).await?;
            }
        }
        let wal_id = self
            .begin_wal(
                WalOperation::Write {
                    path: normalized.to_string(),
                    size: data.len() as u64,
                },
                self.should_skip_wal_for_write(&normalized, data.len() as u64)
                    .await,
            )
            .await?;
        let mut content = match self.pool.read(&physical_path).await {
            Ok((d, _)) => d.to_vec(),
            Err(_) => Vec::new(),
        };
        let offset_idx = offset as usize;
        if offset_idx + data.len() > content.len() {
            content.resize(offset_idx + data.len(), 0);
        }
        if let Some(target_slice) = content.get_mut(offset_idx..offset_idx + data.len()) {
            target_slice.copy_from_slice(&data);
        } else {
            // Unreachable after resize; guard anyway.
            return Err(VfsError::Internal(
                "Buffer overflow in write_at".to_string(),
            ));
        }
        let result = self.pool.write(&physical_path, Bytes::from(content)).await;
        match result {
            Ok(info) => {
                self.complete_wal(wal_id).await;
                // Update quota
                if !skip_quota && diff > 0 {
                    let _ = self.update_quota(diff).await;
                }
                self.journal_log("WRITE_AT", &normalized, None, true, None)
                    .await;
                let translated = self.translate_file_info(info, false);
                self.upsert_index_helper(&normalized, &translated).await?;
                Ok(translated)
            }
            Err(e) => {
                self.journal_log("WRITE_AT", &normalized, None, false, Some(e.to_string()))
                    .await;
                Err(e)
            }
        }
    }
    pub(super) async fn create_dir_impl(&self, path: &str) -> VfsResult<VfsFileInfo> {
        self.check_maintenance()?;
        let normalized = self.validate_file_operation(path).await?;
        let wal_id = self
            .begin_wal(
                WalOperation::CreateDir {
                    path: normalized.to_string(),
                },
                self.should_skip_wal_for_path(&normalized).await,
            )
            .await?;
        if self.is_temp_path(&normalized) {
            tokio::fs::create_dir_all(
                self.temp_manager
                    .get_user_temp_dir(&self.user_id)
                    .join(self.get_relative_path(&normalized, LOGICAL_TEMP_PREFIX)),
            )
            .await
            .map_err(VfsError::Io)?;
            self.complete_wal(wal_id).await;
            return self.stat_impl(path).await;
        }
        let physical_path = self.get_physical_path(&normalized).await?;
        let result = self.pool.create_dir(&physical_path).await;
        match result {
            Ok(_) => {
                self.complete_wal(wal_id).await;
                self.journal_log("MKDIR", &normalized, None, true, None)
                    .await;
                self.cache.invalidate_parent_ls(&normalized).await;
                let info = self.pool.stat(&physical_path).await?;
                let translated = self.translate_file_info(info, false);
                self.upsert_index_helper(&normalized, &translated).await?;
                Ok(translated)
            }
            Err(e) => {
                self.journal_log("MKDIR", &normalized, None, false, Some(e.to_string()))
                    .await;
                Err(e)
            }
        }
    }
    pub(super) async fn move_file_impl(&self, src: &str, dst: &str) -> VfsResult<VfsFileInfo> {
        self.check_maintenance()?;
        let norm_src = self.validate_file_operation(src).await?;
        let norm_dst = self.validate_file_operation(dst).await?;
        let wal_id = self
            .begin_wal(
                WalOperation::Move {
                    src: norm_src.to_string(),
                    dst: norm_dst.to_string(),
                },
                self.should_skip_wal_for_path(&norm_src).await,
            )
            .await?;
        let result = if self.is_temp_path(&norm_src) == self.is_temp_path(&norm_dst) {
            if self.is_temp_path(&norm_src) {
                let s = self
                    .temp_manager
                    .get_user_temp_dir(&self.user_id)
                    .join(self.get_relative_path(&norm_src, LOGICAL_TEMP_PREFIX));
                let d = self
                    .temp_manager
                    .get_user_temp_dir(&self.user_id)
                    .join(self.get_relative_path(&norm_dst, LOGICAL_TEMP_PREFIX));
                tokio::fs::rename(s, d).await.map_err(VfsError::Io)?;
                self.stat_impl(dst).await
            } else {
                match self
                    .pool
                    .move_file(
                        &self.get_physical_path(&norm_src).await?,
                        &self.get_physical_path(&norm_dst).await?,
                    )
                    .await
                {
                    Ok(_) => {
                        self.cache.invalidate_parent_ls(&norm_src).await;
                        self.cache.invalidate_parent_ls(&norm_dst).await;
                        if !self.is_thumbnail_cache_path(&norm_src)
                            && !self.is_thumbnail_cache_path(&norm_dst)
                        {
                            let _ = self
                                .index_service
                                .move_file(&self.user_id, &norm_src, &norm_dst)
                                .await;
                        }
                        self.stat_impl(dst).await
                    }
                    Err(e) => Err(e),
                }
            }
        } else {
            let (data, _) = self.read_impl(src).await?;
            let info = self.write_impl(dst, data).await?;
            let _ = self.delete_impl(src).await;
            Ok(info)
        };
        match &result {
            Ok(_) => {
                self.complete_wal(wal_id).await;
                self.journal_log("MOVE", &norm_src, Some(&norm_dst), true, None)
                    .await
            }
            Err(e) => {
                self.journal_log(
                    "MOVE",
                    &norm_src,
                    Some(&norm_dst),
                    false,
                    Some(e.to_string()),
                )
                .await
            }
        }
        result
    }
    pub(super) async fn copy_file_impl(&self, src: &str, dst: &str) -> VfsResult<VfsFileInfo> {
        self.check_maintenance()?;
        let norm_src = self.validate_file_operation(src).await?;
        let norm_dst = self.validate_file_operation(dst).await?;
        // Get source file size for quota check
        let src_info = self.stat_impl(&norm_src).await?;
        let size = src_info.size as i64;
        let skip_quota = self.is_thumbnail_cache_path(&norm_dst);
        // Check quota
        if !skip_quota {
            self.check_quota(size).await?;
        }
        let result = if self.is_temp_path(&norm_src) == self.is_temp_path(&norm_dst) {
            if self.is_temp_path(&norm_src) {
                let s = self
                    .temp_manager
                    .get_user_temp_dir(&self.user_id)
                    .join(self.get_relative_path(&norm_src, LOGICAL_TEMP_PREFIX));
                let d = self
                    .temp_manager
                    .get_user_temp_dir(&self.user_id)
                    .join(self.get_relative_path(&norm_dst, LOGICAL_TEMP_PREFIX));
                tokio::fs::copy(s, d).await.map_err(VfsError::Io)?;
                self.stat_impl(dst).await
            } else {
                match self
                    .pool
                    .copy_file(
                        &self.get_physical_path(&norm_src).await?,
                        &self.get_physical_path(&norm_dst).await?,
                    )
                    .await
                {
                    Ok(_) => {
                        let info = self
                            .pool
                            .stat(&self.get_physical_path(&norm_dst).await?)
                            .await?;
                        let translated = self.translate_file_info(info, false);
                        self.upsert_index_helper(&norm_dst, &translated).await?;
                        Ok(translated)
                    }
                    Err(e) => Err(e),
                }
            }
        } else {
            let (data, _) = self.read_impl(src).await?;
            self.write_impl(dst, data).await
        };
        match &result {
            Ok(_) => {
                // Update quota
                if !skip_quota {
                    let _ = self.update_quota(size).await;
                }
                self.journal_log("COPY", &norm_src, Some(&norm_dst), true, None)
                    .await
            }
            Err(e) => {
                self.journal_log(
                    "COPY",
                    &norm_src,
                    Some(&norm_dst),
                    false,
                    Some(e.to_string()),
                )
                .await
            }
        }
        result
    }
}
