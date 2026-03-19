use super::ScopedVfsStorageEngine;
use crate::business::services::UserSettingsService;
use crate::vfs::error::{VfsError, VfsResult};
use crate::vfs::{LOGICAL_TEMP_PREFIX, VfsFileInfo, VfsStorage};
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
impl ScopedVfsStorageEngine {
    pub(super) async fn should_skip_wal(&self, path: &str, size: u64) -> bool {
        let config = crate::config::get_vfs_hub_config().await;
        let cfg = config.get_batch_operation();
        if cfg.is_wal_skip_temp_path() && self.is_temp_path(path) {
            return true;
        }
        if self.is_thumbnail_cache_path(path) {
            return true;
        }
        if size < cfg.get_wal_min_size_bytes() {
            return true;
        }
        false
    }
    pub(super) async fn journal_log(
        &self,
        action: &str,
        src: &str,
        dst: Option<&str>,
        success: bool,
        error: Option<String>,
    ) {
        if let Some(recorder) = &self.journal_recorder {
            recorder
                .log_event(crate::vfs::VfsJournalEvent {
                    user_id: &self.user_id,
                    action,
                    src,
                    dst,
                    success,
                    error,
                })
                .await;
        }
    }
    pub(super) async fn get_physical_path(&self, logical_path: &str) -> VfsResult<String> {
        // Force validation first
        let validated = self.validate_file_operation(logical_path).await?;
        let path = validated.trim_start_matches('/');
        Ok(format!("{}/{}", self.user_id, path))
    }
    fn validate_file_operation_impl(path: &str) -> VfsResult<String> {
        let normalized = if path.starts_with('/') {
            path.to_string()
        } else {
            format!("/{}", path)
        };

        // Prohibit control characters
        if normalized.chars().any(|c| c.is_control()) {
            return Err(VfsError::Internal(
                "Security violation: control characters in path".to_string(),
            ));
        }

        // Normalize separators to avoid backend-dependent behavior.
        let normalized = normalized.replace('\\', "/");

        fn validate_segments(p: &str) -> Result<(), VfsError> {
            if !p.starts_with('/') {
                return Err(VfsError::Internal(
                    "Security violation: path must be absolute".to_string(),
                ));
            }

            if p == "/" {
                return Ok(());
            }

            // Reject empty segments in the middle ("//" or "///"), but allow leading '/' and
            // allow a single trailing '/' for directory semantics.
            let ends_with_slash = p.ends_with('/') && p.len() > 1;
            let mut split = p.split('/').peekable();
            let mut idx: usize = 0;

            while let Some(seg) = split.next() {
                let is_last = split.peek().is_none();
                if seg.is_empty() {
                    let is_leading = idx == 0;
                    let is_trailing = ends_with_slash && is_last;
                    if is_leading || is_trailing {
                        idx = idx.saturating_add(1);
                        continue;
                    }
                    return Err(VfsError::Internal(format!(
                        "Security violation: empty path segment in '{}'",
                        p
                    )));
                }
                if seg == "." || seg == ".." {
                    return Err(VfsError::Internal(format!(
                        "Security violation: dot segment detected in '{}'",
                        p
                    )));
                }

                idx = idx.saturating_add(1);
            }
            Ok(())
        }

        // Validate raw segments first.
        validate_segments(&normalized)?;

        // Also validate percent-decoded view to prevent encoded traversal (e.g. %2e%2e).
        if normalized.contains('%') {
            let decoded = percent_encoding::percent_decode_str(&normalized)
                .decode_utf8()
                .map_err(|_| {
                    VfsError::Internal("Security violation: invalid percent encoding in path".to_string())
                })?;
            validate_segments(decoded.as_ref())?;
        }

        Ok(normalized)
    }

    pub(super) async fn validate_file_operation(&self, path: &str) -> VfsResult<String> {
        Self::validate_file_operation_impl(path)
    }
    pub(super) fn is_temp_path(&self, path: &str) -> bool {
        path.starts_with(LOGICAL_TEMP_PREFIX)
    }
    pub(super) fn is_thumbnail_cache_path(&self, path: &str) -> bool {
        let normalized = path.trim_end_matches('/');
        if normalized == "/.thumbs" || normalized == "/.thumbs_cache" {
            return true;
        }
        if normalized.contains("/.thumbs/") {
            return true;
        }
        normalized.starts_with("/.thumbs_cache/")
    }
    pub(super) fn get_relative_path(&self, full_path: &str, prefix: &str) -> String {
        if let Some(stripped) = full_path.strip_prefix(prefix) {
            stripped.trim_start_matches('/').to_string()
        } else {
            full_path.trim_start_matches('/').to_string()
        }
    }
    pub(super) async fn is_refresh_trigger(&self, path: &str) -> bool {
        path.ends_with("/._refresh_here_.txt")
    }
    pub(super) async fn update_quota(&self, delta: i64) -> VfsResult<()> {
        UserSettingsService::update_storage_used(&self.db, &self.user_id, delta)
            .await
            .map_err(|e| VfsError::Internal(e.to_string()))
    }
    pub(super) async fn check_quota(&self, additional_size: i64) -> VfsResult<()> {
        if UserSettingsService::check_quota_exceeded(&self.db, &self.user_id, additional_size)
            .await
            .map_err(|e| VfsError::Internal(e.to_string()))?
        {
            return Err(VfsError::QuotaExceeded);
        }
        Ok(())
    }
    pub(super) async fn get_quota_impl(&self) -> VfsResult<(u64, Option<u64>)> {
        let settings = UserSettingsService::get_user_settings(&self.db, &self.user_id)
            .await
            .map_err(|e| VfsError::Internal(e.to_string()))?;
        if let Some(s) = settings {
            let quota = if s.storage_quota > 0 {
                Some(s.storage_quota as u64)
            } else {
                None
            };
            Ok((s.storage_used as u64, quota))
        } else {
            Ok((0, None))
        }
    }
    pub(super) async fn upsert_index_helper(
        &self,
        logical_path: &str,
        info: &VfsFileInfo,
    ) -> VfsResult<()> {
        if self.is_thumbnail_cache_path(logical_path) {
            return Ok(());
        }
        let _ = self
            .index_service
            .upsert_file(&self.user_id, logical_path, info)
            .await;
        self.cache.invalidate_parent_ls(logical_path).await;
        self.cache.invalidate("stat", logical_path).await;
        Ok(())
    }
    pub(super) async fn get_file_size(&self, path: &str) -> i64 {
        match self.stat_impl(path).await {
            Ok(info) => info.size as i64,
            Err(err) => {
                yh_console_log::yhlog(
                    "warn",
                    &format!("Failed to stat file size for '{}': {}", path, err),
                );
                0
            }
        }
    }
    pub(super) fn translate_file_info(&self, info: VfsFileInfo, is_temp: bool) -> VfsFileInfo {
        let mut info = info;
        if is_temp {
            let mut new_path = String::with_capacity(LOGICAL_TEMP_PREFIX.len() + info.path.len());
            new_path.push_str(LOGICAL_TEMP_PREFIX);
            new_path.push_str(&info.path);
            info.path = new_path.into();
        } else {
            // Decode path from OpenDAL
            let decoded_path = percent_encoding::percent_decode_str(&info.path)
                .decode_utf8_lossy()
                .to_string();
            let decoded_path = decoded_path.replace("\\", "/"); // Normalize slashes
            // Restore physical path to logical path
            //"user_id/relative/path"
            let prefix = format!("{}/", self.user_id);
            if let Some(logical) = decoded_path.strip_prefix(&prefix) {
                info.path =
                    format!("/{}", logical.trim_start_matches('/').trim_end_matches('/')).into();
            } else if decoded_path == self.user_id.as_ref()
                || decoded_path == prefix
                || decoded_path == "."
                || decoded_path == "./"
            {
                info.path = "/".into();
            } else {
                //user_id OpenDAL Driver
                let search_pattern = format!("/{}/", self.user_id);
                if let Some(pos) = decoded_path.find(&search_pattern) {
                    let start = pos + search_pattern.len();
                    if let Some(logical) = decoded_path.get(start..) {
                        info.path =
                            format!("/{}", logical.trim_start_matches('/').trim_end_matches('/'))
                                .into();
                    } else {
                        info.path = format!(
                            "/{}",
                            decoded_path.trim_start_matches('/').trim_end_matches('/')
                        )
                        .into();
                    }
                } else {
                    info.path = format!(
                        "/{}",
                        decoded_path.trim_start_matches('/').trim_end_matches('/')
                    )
                    .into();
                }
            }
        }
        info
    }
    pub(super) fn check_maintenance(&self) -> VfsResult<()> {
        if crate::vfs::is_user_under_maintenance(&self.user_id) {
            return Err(VfsError::MaintenanceMode);
        }
        Ok(())
    }
    pub(super) async fn write_stream_internal(
        &self,
        normalized: &str,
        mut stream: BoxStream<'static, VfsResult<Bytes>>,
    ) -> VfsResult<VfsFileInfo> {
        if self.is_temp_path(normalized) {
            let rel = self.get_relative_path(normalized, LOGICAL_TEMP_PREFIX);
            let mut file = tokio::fs::File::create(
                self.temp_manager.get_user_temp_dir(&self.user_id).join(rel),
            )
            .await
            .map_err(VfsError::Io)?;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                let mut reader = chunk.as_ref();
                tokio::io::copy(&mut reader, &mut file)
                    .await
                    .map_err(VfsError::Io)?;
            }
            return self.stat_impl(normalized).await;
        }
        let (temp_dir, _guard) = self
            .temp_manager
            .create_user_temp_dir(&self.user_id, "upload")
            .await
            .map_err(|e| VfsError::Internal(e.to_string()))?;
        let temp_file_path = temp_dir.join(uuid::Uuid::now_v7().to_string());
        let mut file = tokio::fs::File::create(&temp_file_path)
            .await
            .map_err(VfsError::Io)?;
        let mut total_written = 0u64;
        let initial_file_size = self.get_file_size(normalized).await;
        while let Some(chunk) = stream.next().await {
            let data = chunk?;
            total_written += data.len() as u64;
            // Real-time quota check
            let current_diff = total_written as i64 - initial_file_size;
            if current_diff > 0 {
                self.check_quota(current_diff).await?;
            }
            let mut reader = data.as_ref();
            tokio::io::copy(&mut reader, &mut file)
                .await
                .map_err(VfsError::Io)?;
        }
        file.sync_all().await.map_err(VfsError::Io)?;
        drop(file);
        let buffer_file = tokio::fs::File::open(&temp_file_path)
            .await
            .map_err(VfsError::Io)?;
        use tokio_util::io::ReaderStream;
        let vfs_stream = ReaderStream::new(buffer_file)
            .map(|res| res.map_err(|e| VfsError::Internal(e.to_string())));
        self.pool
            .write_stream(
                &self.get_physical_path(normalized).await?,
                Box::pin(vfs_stream),
            )
            .await?;
        let _ = tokio::fs::remove_file(&temp_file_path).await;
        if total_written as i64 - initial_file_size != 0 {
            let _ = self
                .update_quota(total_written as i64 - initial_file_size)
                .await;
        }
        let info = self
            .pool
            .stat(&self.get_physical_path(normalized).await?)
            .await?;
        let translated = self.translate_file_info(info, false);
        self.upsert_index_helper(normalized, &translated).await?;
        Ok(translated)
    }
}

#[cfg(test)]
mod tests {
    use super::ScopedVfsStorageEngine;

    #[test]
    fn path_allows_double_dot_in_filename() {
        assert!(ScopedVfsStorageEngine::validate_file_operation_impl("/foo..bar.txt").is_ok());
    }

    #[test]
    fn path_rejects_parent_traversal_segment() {
        assert!(
            ScopedVfsStorageEngine::validate_file_operation_impl("/documents/../etc/passwd")
                .is_err()
        );
    }

    #[test]
    fn path_rejects_current_dir_segment() {
        assert!(
            ScopedVfsStorageEngine::validate_file_operation_impl("/documents/./file.txt")
                .is_err()
        );
    }

    #[test]
    fn path_rejects_double_slash() {
        assert!(ScopedVfsStorageEngine::validate_file_operation_impl("/a//b/c").is_err());
    }

    #[test]
    fn path_rejects_percent_encoded_parent_traversal() {
        assert!(ScopedVfsStorageEngine::validate_file_operation_impl("/a/%2e%2e/b").is_err());
    }
}
