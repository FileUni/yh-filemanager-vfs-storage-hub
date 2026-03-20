use super::ScopedVfsStorageEngine;
use crate::vfs::error::{VfsError, VfsResult};
use crate::vfs::wal::WalOperation;
use crate::vfs::{LOGICAL_TEMP_PREFIX, VfsFileInfo, VfsStorage};
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::stream::BoxStream;
impl ScopedVfsStorageEngine {
    pub(super) async fn write_impl(&self, path: &str, data: Bytes) -> VfsResult<VfsFileInfo> {
        self.check_maintenance()?;
        let normalized = self.validate_file_operation(path).await?;
        if self.is_refresh_trigger(&normalized).await {
            return self.stat_impl(&normalized).await;
        }
        if let Some(info) = self
            .try_enqueue_write_cache(&normalized, data.clone())
            .await?
        {
            return Ok(info);
        }
        self.flush_pending_write_cache_for_path(&normalized).await?;
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
            self.mark_wal_physical_done(wal_id).await;
            self.stat_impl(path).await
        } else {
            match self
                .pool
                .write(&self.get_physical_path(&normalized).await?, data)
                .await
            {
                Ok(info) => {
                    self.mark_wal_physical_done(wal_id).await;
                    let translated = self.translate_file_info(info, false);
                    if let Err(err) = self.upsert_index_helper(&normalized, &translated).await {
                        self.fail_wal(
                            wal_id,
                            &format!("WRITE metadata sync failed for {}: {}", normalized, err),
                        )
                        .await;
                        return Err(err);
                    }
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
                self.fail_wal(wal_id, &e.to_string()).await;
                self.journal_log("WRITE", &normalized, None, false, Some(e.to_string()))
                    .await
            }
        }
        result
    }
    pub(super) async fn delete_impl(&self, path: &str) -> VfsResult<VfsFileInfo> {
        self.check_maintenance()?;
        let normalized = self.validate_file_operation(path).await?;
        self.flush_pending_write_cache_for_path(&normalized).await?;
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
            self.mark_wal_physical_done(wal_id).await;
            self.complete_wal(wal_id).await;
            return Ok(info);
        }
        match self
            .pool
            .delete(&self.get_physical_path(&normalized).await?)
            .await
        {
            Ok(_) => {
                self.mark_wal_physical_done(wal_id).await;
                let mut metadata_complete = true;
                if !skip_quota {
                    if let Err(err) = self
                        .index_service
                        .delete_file(&self.user_id, &normalized)
                        .await
                    {
                        metadata_complete = false;
                        self.fail_wal(
                            wal_id,
                            &format!("DELETE metadata sync failed for {}: {}", normalized, err),
                        )
                        .await;
                        yh_console_log::yhlog(
                            "warn",
                            &format!(
                                "VFS delete metadata sync failed for user_id={} path={} err={}",
                                self.user_id, normalized, err
                            ),
                        );
                    }
                }
                if metadata_complete {
                    self.complete_wal(wal_id).await;
                }
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
                self.fail_wal(wal_id, &e.to_string()).await;
                self.journal_log("DELETE", &normalized, None, false, Some(e.to_string()))
                    .await;
                Err(e)
            }
        }
    }
    pub(super) async fn write_stream_impl(
        &self,
        path: &str,
        mut stream: BoxStream<'static, VfsResult<Bytes>>,
    ) -> VfsResult<VfsFileInfo> {
        self.check_maintenance()?;
        let normalized = self.validate_file_operation(path).await?;
        if self.is_refresh_trigger(&normalized).await {
            let parent = if let Some(parent_path) = std::path::Path::new(&normalized).parent() {
                parent_path.to_string_lossy().to_string()
            } else {
                "/".to_string()
            };
            let _ = self.sync_index_impl(&parent).await?;
            return self.stat_impl(&parent).await;
        }
        if self.should_use_write_cache(&normalized, 1) {
            let limit = self.pool.write_cache_max_file_size().unwrap_or(0) as usize;
            let mut buffer = BytesMut::new();
            let mut chunks = Vec::new();
            while let Some(chunk) = stream.next().await {
                let bytes = chunk?;
                if buffer.len() + bytes.len() > limit {
                    let prefix_stream = futures::stream::iter(
                        chunks
                            .into_iter()
                            .chain(std::iter::once(bytes))
                            .map(Ok::<Bytes, VfsError>),
                    )
                    .boxed();
                    let merged_stream = prefix_stream.chain(stream).boxed();
                    self.journal_log("WRITE_STREAM", &normalized, None, true, None)
                        .await;
                    let result = self.write_stream_internal(&normalized, merged_stream).await;
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
                    return result;
                }
                buffer.extend_from_slice(&bytes);
                chunks.push(bytes);
            }
            return self.write_impl(&normalized, buffer.freeze()).await;
        }
        self.journal_log("WRITE_STREAM", &normalized, None, true, None)
            .await;
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
        self.flush_pending_write_cache_for_path(&normalized).await?;
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
                self.mark_wal_physical_done(wal_id).await;
                // Update quota
                if !skip_quota && diff > 0 {
                    let _ = self.update_quota(diff).await;
                }
                self.journal_log("WRITE_AT", &normalized, None, true, None)
                    .await;
                let translated = self.translate_file_info(info, false);
                if let Err(err) = self.upsert_index_helper(&normalized, &translated).await {
                    self.fail_wal(
                        wal_id,
                        &format!("WRITE_AT metadata sync failed for {}: {}", normalized, err),
                    )
                    .await;
                    return Err(err);
                }
                self.complete_wal(wal_id).await;
                Ok(translated)
            }
            Err(e) => {
                self.fail_wal(wal_id, &e.to_string()).await;
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
            self.mark_wal_physical_done(wal_id).await;
            self.complete_wal(wal_id).await;
            return self.stat_impl(path).await;
        }
        let physical_path = self.get_physical_path(&normalized).await?;
        let result = self.pool.create_dir(&physical_path).await;
        match result {
            Ok(_) => {
                self.mark_wal_physical_done(wal_id).await;
                self.journal_log("MKDIR", &normalized, None, true, None)
                    .await;
                self.cache.invalidate_parent_ls(&normalized).await;
                let info = self.pool.stat(&physical_path).await?;
                let translated = self.translate_file_info(info, false);
                if let Err(err) = self.upsert_index_helper(&normalized, &translated).await {
                    self.fail_wal(
                        wal_id,
                        &format!(
                            "CREATE_DIR metadata sync failed for {}: {}",
                            normalized, err
                        ),
                    )
                    .await;
                    return Err(err);
                }
                self.complete_wal(wal_id).await;
                Ok(translated)
            }
            Err(e) => {
                self.fail_wal(wal_id, &e.to_string()).await;
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
        self.flush_pending_write_cache_for_path(&norm_src).await?;
        self.flush_pending_write_cache_for_path(&norm_dst).await?;
        let wal_id = self
            .begin_wal(
                WalOperation::Move {
                    src: norm_src.to_string(),
                    dst: norm_dst.to_string(),
                },
                self.should_skip_wal_for_path(&norm_src).await,
            )
            .await?;
        let mut should_complete_wal = false;
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
                self.mark_wal_physical_done(wal_id).await;
                should_complete_wal = true;
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
                        self.mark_wal_physical_done(wal_id).await;
                        let mut metadata_complete = true;
                        self.cache.invalidate_parent_ls(&norm_src).await;
                        self.cache.invalidate_parent_ls(&norm_dst).await;
                        if !self.is_thumbnail_cache_path(&norm_src)
                            && !self.is_thumbnail_cache_path(&norm_dst)
                        {
                            if let Err(err) = self
                                .index_service
                                .move_file(&self.user_id, &norm_src, &norm_dst)
                                .await
                            {
                                metadata_complete = false;
                                self.fail_wal(
                                    wal_id,
                                    &format!(
                                        "MOVE metadata sync failed for {} -> {}: {}",
                                        norm_src, norm_dst, err
                                    ),
                                )
                                .await;
                                yh_console_log::yhlog(
                                    "warn",
                                    &format!(
                                        "VFS move metadata sync failed for user_id={} src={} dst={} err={}",
                                        self.user_id, norm_src, norm_dst, err
                                    ),
                                );
                            }
                        }
                        if metadata_complete {
                            should_complete_wal = true;
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
            should_complete_wal = true;
            Ok(info)
        };
        match &result {
            Ok(_) => {
                if should_complete_wal {
                    self.complete_wal(wal_id).await;
                }
                self.journal_log("MOVE", &norm_src, Some(&norm_dst), true, None)
                    .await
            }
            Err(e) => {
                self.fail_wal(wal_id, &e.to_string()).await;
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
        self.flush_pending_write_cache_for_path(&norm_src).await?;
        self.flush_pending_write_cache_for_path(&norm_dst).await?;
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
