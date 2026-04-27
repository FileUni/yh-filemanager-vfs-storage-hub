use super::ScopedVfsStorageEngine;
use crate::vfs::error::{VfsError, VfsResult};
use crate::vfs::wal::WalOperation;
use crate::vfs::{LOGICAL_TEMP_PREFIX, VfsFileInfo, VfsStorage};
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::stream::BoxStream;
impl ScopedVfsStorageEngine {
    fn should_prune_stale_recycle_index(path: &str, err: &VfsError) -> bool {
        if !path.starts_with("/.recycle_bin/") {
            return false;
        }
        match err {
            VfsError::NotFound(_) => true,
            VfsError::OpenDal(inner) => matches!(
                inner.kind(),
                opendal::ErrorKind::NotFound | opendal::ErrorKind::Unexpected
            ),
            VfsError::Io(io_err) => matches!(
                io_err.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::InvalidInput
            ),
            VfsError::Internal(message) => {
                let lower = message.to_ascii_lowercase();
                lower.contains("not found")
                    || lower.contains("no such file")
                    || lower.contains("invalid argument")
                    || lower.contains("unsupported file type")
                    || lower.contains("unexpected")
            }
            _ => false,
        }
    }

    pub(super) async fn write_impl(&self, path: &str, data: Bytes) -> VfsResult<VfsFileInfo> {
        self.check_maintenance()?;
        let normalized = self.validate_file_operation(path).await?;
        if let Some(plan) = self.get_protected_plan(&normalized).await? {
            return self.protected_write_impl(&normalized, data, &plan).await;
        }
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
                    protected: None,
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
        if self.get_protected_plan(&normalized).await?.is_some() {
            return self.protected_delete_impl(&normalized).await;
        }
        self.flush_pending_write_cache_for_path(&normalized).await?;
        let info = self.stat_impl(&normalized).await?;
        let reclaimed_size = if info.is_dir {
            self.list_recursive_impl(&normalized)
                .await?
                .into_iter()
                .filter(|entry| !entry.is_dir)
                .map(|entry| entry.size as i64)
                .sum()
        } else {
            info.size as i64
        };
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
        let physical_path = self.get_physical_path(&normalized).await?;
        let delete_result = if info.is_dir {
            self.pool
                .remove_tree(&physical_path)
                .await
                .map(|_| info.clone())
        } else {
            self.pool.delete(&physical_path).await
        };
        match delete_result {
            Ok(_) => {
                self.mark_wal_physical_done(wal_id).await;
                let mut metadata_complete = true;
                if !skip_quota
                    && let Err(err) = self
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
                if metadata_complete {
                    self.complete_wal(wal_id).await;
                }
                self.cache.invalidate_parent_ls(&normalized).await;
                // Update quota (decrease)
                if !skip_quota && reclaimed_size != 0 {
                    let _ = self.update_quota(-reclaimed_size).await;
                }
                self.journal_log("DELETE", &normalized, None, true, None)
                    .await;
                Ok(info)
            }
            Err(e) => {
                if Self::should_prune_stale_recycle_index(&normalized, &e) {
                    match self.index_service.delete_file(&self.user_id, &normalized).await {
                        Ok(()) => {
                            self.mark_wal_physical_done(wal_id).await;
                            self.complete_wal(wal_id).await;
                            self.cache.invalidate_parent_ls(&normalized).await;
                            yh_console_log::yhlog(
                                "warn",
                                &format!(
                                    "VFS delete pruned stale recycle entry for user_id={} path={} physical_path={} err={}",
                                    self.user_id, normalized, physical_path, e
                                ),
                            );
                            self.journal_log("DELETE", &normalized, None, true, None)
                                .await;
                            return Ok(info);
                        }
                        Err(index_err) => {
                            self.fail_wal(
                                wal_id,
                                &format!(
                                    "DELETE stale recycle entry cleanup failed for {} (physical_path={}): original_err={}, index_err={}",
                                    normalized, physical_path, e, index_err
                                ),
                            )
                            .await;
                            self.journal_log(
                                "DELETE",
                                &normalized,
                                None,
                                false,
                                Some(format!(
                                    "physical_path={} original_err={} index_cleanup_err={}",
                                    physical_path, e, index_err
                                )),
                            )
                            .await;
                            return Err(VfsError::Internal(format!(
                                "Delete failed for {} and stale recycle entry cleanup also failed: {}",
                                normalized, index_err
                            )));
                        }
                    }
                }
                self.fail_wal(
                    wal_id,
                    &format!("DELETE failed for {} (physical_path={}): {}", normalized, physical_path, e),
                )
                .await;
                self.journal_log(
                    "DELETE",
                    &normalized,
                    None,
                    false,
                    Some(format!("physical_path={} err={}", physical_path, e)),
                )
                .await;
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ScopedVfsStorageEngine;
    use crate::vfs::error::VfsError;

    #[test]
    fn stale_recycle_index_prunes_not_found_errors() {
        assert!(ScopedVfsStorageEngine::should_prune_stale_recycle_index(
            "/.recycle_bin/test.sock",
            &VfsError::NotFound("missing".to_string()),
        ));
        assert!(!ScopedVfsStorageEngine::should_prune_stale_recycle_index(
            "/normal/test.sock",
            &VfsError::NotFound("missing".to_string()),
        ));
    }

    #[test]
    fn stale_recycle_index_prunes_invalid_input_io_errors() {
        assert!(ScopedVfsStorageEngine::should_prune_stale_recycle_index(
            "/.recycle_bin/test.sock",
            &VfsError::Io(std::io::Error::from(std::io::ErrorKind::InvalidInput)),
        ));
    }
}

impl ScopedVfsStorageEngine {
    pub(super) async fn write_stream_impl(
        &self,
        path: &str,
        mut stream: BoxStream<'static, VfsResult<Bytes>>,
    ) -> VfsResult<VfsFileInfo> {
        self.check_maintenance()?;
        let normalized = self.validate_file_operation(path).await?;
        if let Some(plan) = self.get_protected_plan(&normalized).await? {
            return self
                .protected_write_stream_impl(&normalized, stream, &plan)
                .await;
        }
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
        if let Some(plan) = self.get_protected_plan(&normalized).await? {
            return self
                .protected_write_at_impl(&normalized, offset, data, &plan)
                .await;
        }
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
                    protected: None,
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
        if self.get_protected_plan(&normalized).await?.is_some() {
            return self.protected_create_dir_impl(&normalized).await;
        }
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
        let src_plan = self.get_protected_plan(&norm_src).await?;
        let dst_plan = self.get_protected_plan(&norm_dst).await?;
        if let (Some(src_plan), Some(dst_plan)) = (&src_plan, &dst_plan)
            && self.same_protected_domain(src_plan, dst_plan)
        {
            return self
                .protected_move_same_domain_impl(&norm_src, &norm_dst)
                .await;
        }
        if src_plan.is_some() || dst_plan.is_some() {
            let src_info = self.stat_impl(&norm_src).await?;
            if src_info.is_dir {
                return Err(VfsError::Internal(
                    "Protected directory move is not implemented yet".to_string(),
                ));
            }
            let (data, _) = self.read_impl(&norm_src).await?;
            let info = self.write_impl(&norm_dst, data).await?;
            let _ = self.delete_impl(&norm_src).await?;
            self.journal_log("MOVE", &norm_src, Some(&norm_dst), true, None)
                .await;
            return Ok(info);
        }
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
                let physical_src = self.get_physical_path(&norm_src).await?;
                let physical_dst = self.get_physical_path(&norm_dst).await?;
                match self.pool.move_file(&physical_src, &physical_dst).await {
                    Ok(_) => {
                        yh_console_log::yhlog(
                            "warn",
                            &format!(
                                "VFS move physical success user_id={} src={} dst={} physical_src={} physical_dst={}",
                                self.user_id, norm_src, norm_dst, physical_src, physical_dst
                            ),
                        );
                        self.mark_wal_physical_done(wal_id).await;
                        let mut metadata_complete = true;
                        self.pool.invalidate_read_cache(&physical_src).await;
                        self.pool.invalidate_read_cache(&physical_dst).await;
                        self.cache.invalidate_parent_ls(&norm_src).await;
                        self.cache.invalidate_parent_ls(&norm_dst).await;
                        self.cache.invalidate("stat", &norm_src).await;
                        self.cache.invalidate("stat", &norm_dst).await;
                        yh_console_log::yhlog(
                            "warn",
                            &format!(
                                "VFS move invalidated caches user_id={} src={} dst={}",
                                self.user_id, norm_src, norm_dst
                            ),
                        );
                        if !self.is_thumbnail_cache_path(&norm_src)
                            && !self.is_thumbnail_cache_path(&norm_dst)
                            && let Err(err) = self
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
        let src_plan = self.get_protected_plan(&norm_src).await?;
        let dst_plan = self.get_protected_plan(&norm_dst).await?;
        if let (Some(src_plan), Some(dst_plan)) = (&src_plan, &dst_plan)
            && self.same_protected_domain(src_plan, dst_plan)
        {
            return self
                .protected_copy_same_domain_impl(&norm_src, &norm_dst, src_plan)
                .await;
        }
        if src_plan.is_some() || dst_plan.is_some() {
            let src_info = self.stat_impl(&norm_src).await?;
            if src_info.is_dir {
                return Err(VfsError::Internal(
                    "Protected directory copy is not implemented yet".to_string(),
                ));
            }
            let (data, _) = self.read_impl(&norm_src).await?;
            let info = self.write_impl(&norm_dst, data).await?;
            self.journal_log("COPY", &norm_src, Some(&norm_dst), true, None)
                .await;
            return Ok(info);
        }
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
        let wal_id = self
            .begin_wal(
                WalOperation::Copy {
                    src: norm_src.to_string(),
                    dst: norm_dst.to_string(),
                    protected: None,
                },
                self.should_skip_wal_for_write(&norm_dst, src_info.size)
                    .await,
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
                tokio::fs::copy(s, d).await.map_err(VfsError::Io)?;
                self.mark_wal_physical_done(wal_id).await;
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
                        self.mark_wal_physical_done(wal_id).await;
                        let info = self
                            .pool
                            .stat(&self.get_physical_path(&norm_dst).await?)
                            .await?;
                        let translated = self.translate_file_info(info, false);
                        if let Err(err) = self.upsert_index_helper(&norm_dst, &translated).await {
                            self.fail_wal(
                                wal_id,
                                &format!(
                                    "COPY metadata sync failed for {} -> {}: {}",
                                    norm_src, norm_dst, err
                                ),
                            )
                            .await;
                            return Err(err);
                        }
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
                self.complete_wal(wal_id).await;
                // Update quota
                if !skip_quota {
                    let _ = self.update_quota(size).await;
                }
                self.journal_log("COPY", &norm_src, Some(&norm_dst), true, None)
                    .await
            }
            Err(e) => {
                self.fail_wal(wal_id, &e.to_string()).await;
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
