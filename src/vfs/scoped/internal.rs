use super::ScopedVfsStorageEngine;
use crate::business::services::UserSettingsService;
use crate::vfs::error::{VfsError, VfsResult};
use crate::vfs::{LOGICAL_TEMP_PREFIX, VfsFileInfo, VfsStorage};
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use std::borrow::Cow;
use std::sync::atomic::Ordering;
impl ScopedVfsStorageEngine {
    pub(super) async fn should_skip_wal_for_write(&self, path: &str, size: u64) -> bool {
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
    pub(super) async fn should_skip_wal_for_path(&self, path: &str) -> bool {
        let config = crate::config::get_vfs_hub_config().await;
        let cfg = config.get_batch_operation();
        if cfg.is_wal_skip_temp_path() && self.is_temp_path(path) {
            return true;
        }
        self.is_thumbnail_cache_path(path)
    }
    pub(super) async fn begin_wal(
        &self,
        operation: crate::vfs::wal::WalOperation,
        skip: bool,
    ) -> VfsResult<Option<i64>> {
        if skip {
            return Ok(None);
        }
        let Some(wm) = &self.wal_manager else {
            return Ok(None);
        };
        let wal_id = wm
            .log_operation(&self.user_id, operation)
            .await
            .map_err(|e| VfsError::Internal(e.to_string()))?;
        Ok(Some(wal_id))
    }
    pub(super) async fn complete_wal(&self, wal_id: Option<i64>) {
        let (Some(id), Some(wm)) = (wal_id, &self.wal_manager) else {
            return;
        };
        if let Err(err) = wm.complete_operation(id).await {
            yh_console_log::yhlog(
                "warn",
                &format!(
                    "VFS WAL complete failed: user_id={} wal_id={} err={}",
                    self.user_id, id, err
                ),
            );
        }
    }
    pub(super) async fn mark_wal_physical_done(&self, wal_id: Option<i64>) {
        let (Some(id), Some(wm)) = (wal_id, &self.wal_manager) else {
            return;
        };
        if let Err(err) = wm.mark_physical_done(id).await {
            yh_console_log::yhlog(
                "warn",
                &format!(
                    "VFS WAL physical_done failed: user_id={} wal_id={} err={}",
                    self.user_id, id, err
                ),
            );
        }
    }
    pub(super) async fn fail_wal(&self, wal_id: Option<i64>, reason: &str) {
        let (Some(id), Some(wm)) = (wal_id, &self.wal_manager) else {
            return;
        };
        if let Err(err) = wm.fail_operation(id, reason).await {
            yh_console_log::yhlog(
                "warn",
                &format!(
                    "VFS WAL fail failed: user_id={} wal_id={} err={} reason={}",
                    self.user_id, id, err, reason
                ),
            );
        }
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
    pub(super) fn should_use_write_cache(&self, logical_path: &str, size: usize) -> bool {
        if self.is_temp_path(logical_path)
            || self.is_thumbnail_cache_path(logical_path)
            || logical_path.starts_with("/.recycle_bin/")
        {
            return false;
        }
        self.pool
            .write_cache
            .as_ref()
            .is_some_and(|cache| cache.should_cache(logical_path, size))
    }
    pub(super) async fn try_enqueue_write_cache(
        &self,
        logical_path: &str,
        data: Bytes,
    ) -> VfsResult<Option<VfsFileInfo>> {
        if !self.should_use_write_cache(logical_path, data.len()) {
            return Ok(None);
        }
        let physical_path = self.get_physical_path(logical_path).await?;
        let result = match self
            .pool
            .enqueue_write_cache(&self.user_id, logical_path, &physical_path, data)
            .await
        {
            Ok(result) => result,
            Err(err) => {
                yh_console_log::yhlog(
                    "warn",
                    &format!(
                        "Write cache enqueue failed for user_id={} path={} err={}",
                        self.user_id, logical_path, err
                    ),
                );
                return Ok(None);
            }
        };
        if result.is_none() {
            return Ok(None);
        }
        self.cache.invalidate_parent_ls(logical_path).await;
        self.cache.invalidate("stat", logical_path).await;
        Ok(result.map(|info| self.translate_file_info(info, false)))
    }
    pub(super) async fn flush_pending_write_cache_for_path(
        &self,
        logical_path: &str,
    ) -> VfsResult<()> {
        if self.is_temp_path(logical_path) {
            return Ok(());
        }
        let physical_path = self.get_physical_path(logical_path).await?;
        self.pool.flush_write_cache(&physical_path).await
    }
    pub(super) async fn pending_stat(&self, logical_path: &str) -> Option<VfsFileInfo> {
        if self.is_temp_path(logical_path) {
            return None;
        }
        let physical_path = self.get_physical_path(logical_path).await.ok()?;
        self.pool
            .pending_stat(&physical_path)
            .await
            .map(|info| self.translate_file_info(info, false))
    }
    pub(super) async fn pending_children(&self, logical_path: &str) -> VfsResult<Vec<VfsFileInfo>> {
        if self.is_temp_path(logical_path) {
            return Ok(Vec::new());
        }
        let physical_path = self.get_physical_path(logical_path).await?;
        Ok(self
            .pool
            .pending_children(&physical_path)
            .await
            .into_iter()
            .map(|info| self.translate_file_info(info, false))
            .collect())
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
                VfsError::Internal(
                    "Security violation: invalid percent encoding in path".to_string(),
                )
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
    pub(super) fn is_protected_blob_path(&self, path: &str) -> bool {
        let normalized = path.trim_end_matches('/');
        normalized == crate::vfs::protected::PROTECTED_STORAGE_DIR
            || normalized.starts_with(&format!(
                "{}/",
                crate::vfs::protected::PROTECTED_STORAGE_DIR
            ))
    }
    pub(super) fn is_hidden_storage_path(&self, path: &str) -> bool {
        self.is_thumbnail_cache_path(path) || self.is_protected_blob_path(path)
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
        let pending_delta = self.pool.pending_quota_delta(&self.user_id);
        let settings = UserSettingsService::get_user_settings(&self.db, &self.user_id)
            .await
            .map_err(|e| VfsError::Internal(e.to_string()))?;
        if let Some(settings) = settings
            && settings.storage_quota > 0
        {
            let projected = settings
                .storage_used
                .saturating_add(pending_delta)
                .saturating_add(additional_size);
            if projected > settings.storage_quota {
                return Err(VfsError::QuotaExceeded);
            }
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
            let used = s
                .storage_used
                .saturating_add(self.pool.pending_quota_delta(&self.user_id));
            Ok((used.max(0) as u64, quota))
        } else {
            Ok((0, None))
        }
    }
    pub(super) async fn get_user_settings_snapshot(
        &self,
    ) -> VfsResult<Option<crate::business::services::UserSettingsSnapshot>> {
        let settings = UserSettingsService::get_user_settings(&self.db, &self.user_id)
            .await
            .map_err(|e| VfsError::Internal(e.to_string()))?;
        Ok(settings
            .as_ref()
            .map(crate::business::services::UserSettingsSnapshot::from))
    }
    pub(super) async fn get_index_metadata(
        &self,
        path: &str,
    ) -> VfsResult<Option<crate::business::entities::file_index::Model>> {
        self.index_service
            .get_file_metadata(&self.user_id, path)
            .await
            .map_err(|e| VfsError::Internal(e.to_string()))
    }
    pub(super) async fn is_protected_subdir_path(&self, path: &str) -> VfsResult<bool> {
        let Some(settings) = self.get_user_settings_snapshot().await? else {
            return Ok(false);
        };
        if !settings.is_protected_subdir_root() {
            return Ok(false);
        }
        Ok(settings.matches_protected_root(path))
    }
    pub(super) async fn get_protected_plan(
        &self,
        path: &str,
    ) -> VfsResult<Option<crate::vfs::protected::ProtectedPathPlan>> {
        let cached = self
            .protected_plan_cache
            .get_or_try_init(|| async { self.resolve_protected_plan_uncached().await })
            .await?;
        match cached {
            Some(plan) if plan.matches_path(path) => Ok(Some(plan.clone())),
            _ => Ok(None),
        }
    }
    async fn resolve_protected_plan_uncached(
        &self,
    ) -> VfsResult<Option<crate::vfs::protected::ProtectedPathPlan>> {
        let Some(settings) = self.get_user_settings_snapshot().await? else {
            return Ok(None);
        };
        let Some(root) = settings.protected_root_trimmed() else {
            return Ok(None);
        };
        let root = root.to_string();
        let mode_raw = settings
            .protected_mode
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| VfsError::Internal("Protected mode is missing".to_string()))?;
        let slot_id = settings
            .protected_key_slot_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| VfsError::Internal("Protected key slot is missing".to_string()))?
            .to_string();
        let mode = mode_raw
            .parse::<crate::vfs::protected::ProtectedMode>()
            .map_err(|_| VfsError::Internal(format!("Unsupported protected mode: {}", mode_raw)))?;

        // License check for encryption
        if mode == crate::vfs::protected::ProtectedMode::Encrypt {
            let authorized =
                if let Some(config_arc) = yh_config_infra::core_crate_config::get_core_config() {
                    let cfg = config_arc.read().await;
                    cfg.license
                        .is_feature_authorized_in_config_cached("storage_encryption")
                        .await
                } else {
                    false
                };
            if !authorized {
                return Err(VfsError::Internal(
                    "Storage encryption is disabled. Valid license and 'enabled' flag required."
                        .to_string(),
                ));
            }
        }

        let config = crate::config::get_vfs_hub_config().await;
        let protected_cfg = config.get_protected_storage();
        let global_mode = protected_cfg.get_global_mode().trim().to_ascii_lowercase();
        if global_mode != mode_raw.to_ascii_lowercase() {
            return Err(VfsError::Internal(
                "Protected storage is temporarily unavailable because global mode does not match"
                    .to_string(),
            ));
        }
        let obfuscation = protected_cfg.get_obfuscation();
        let prng = obfuscation
            .get_prng()
            .parse::<crate::vfs::protected::ProtectedPrng>()
            .map_err(|_| {
                VfsError::Internal(format!(
                    "Unsupported protected storage PRNG: {}",
                    obfuscation.get_prng()
                ))
            })?;
        let encrypt_key = if mode == crate::vfs::protected::ProtectedMode::Encrypt {
            let wrapped = settings
                .protected_wrapped_key
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    VfsError::Internal("Protected wrapped key is missing".to_string())
                })?;
            let wrap_key = protected_cfg.get_encrypt().get_binary_wrap_key();
            let plaintext = yh_config_infra::aead::decrypt_from_string_v1(wrapped, &wrap_key)
                .map_err(VfsError::Internal)?;
            let raw = hex::decode(plaintext.trim()).map_err(|e| {
                VfsError::Internal(format!("Decode protected wrapped key failed: {}", e))
            })?;
            if raw.len() != 32 {
                return Err(VfsError::Internal(
                    "Protected wrapped key size is invalid".to_string(),
                ));
            }
            let mut key = [0u8; 32];
            key.copy_from_slice(&raw);
            Some(key)
        } else {
            None
        };
        Ok(Some(crate::vfs::protected::ProtectedPathPlan {
            root,
            mode,
            key_slot_id: slot_id,
            block_size: obfuscation.get_block_size_kib() as usize * 1024,
            prng,
            encrypt_key,
            workers: obfuscation.get_workers(),
        }))
    }
    pub(super) async fn next_protected_blob_physical_path(
        &self,
        key_slot_id: &str,
    ) -> VfsResult<String> {
        let logical_blob_path = crate::vfs::protected::next_blob_logical_path(key_slot_id);
        let physical_blob_path = self.get_physical_path(&logical_blob_path).await?;
        if let Some(parent) = std::path::Path::new(&physical_blob_path).parent()
            && let Some(parent_str) = parent.to_str()
            && !parent_str.is_empty()
        {
            let _ = self.pool.create_dir_all(parent_str).await;
        }
        Ok(physical_blob_path)
    }
    pub(super) async fn upsert_index_helper_with_backend_key(
        &self,
        logical_path: &str,
        info: &VfsFileInfo,
        backend_key: Option<&str>,
        physical_size: Option<i64>,
        protected_meta: Option<&str>,
    ) -> VfsResult<()> {
        if self.is_protected_blob_path(logical_path) {
            return Ok(());
        }
        if self.is_thumbnail_cache_path(logical_path)
            && self.get_protected_plan(logical_path).await?.is_none()
        {
            return Ok(());
        }
        let backend_type = self.pool.get_backend_type();
        if let Err(e) = self
            .index_service
            .upsert_file_with_location(
                &self.user_id,
                logical_path,
                info,
                Some(self.pool.config.get_name()),
                Some(backend_type.as_str()),
                backend_key,
                physical_size,
                protected_meta,
            )
            .await
        {
            yh_console_log::yhlog(
                "error",
                &format!(
                    "VFS index upsert failed: user_id={} path={} err={}",
                    self.user_id, logical_path, e
                ),
            );
            return Err(VfsError::Internal(e.to_string()));
        }
        self.cache.invalidate_parent_ls(logical_path).await;
        self.cache.invalidate("stat", logical_path).await;
        Ok(())
    }
    pub(super) async fn upsert_index_helper(
        &self,
        logical_path: &str,
        info: &VfsFileInfo,
    ) -> VfsResult<()> {
        let physical_path = self.get_physical_path(logical_path).await?;
        self.upsert_index_helper_with_backend_key(
            logical_path,
            info,
            Some(physical_path.as_str()),
            None,
            None,
        )
        .await
    }
    pub(super) async fn get_file_size(&self, path: &str) -> i64 {
        match self.stat_impl(path).await {
            Ok(info) => info.size as i64,
            Err(err) => {
                if !Self::is_not_found_error(&err) {
                    yh_console_log::yhlog(
                        "warn",
                        &format!("Failed to stat file size for '{}': {}", path, err),
                    );
                }
                0
            }
        }
    }
    fn is_not_found_error(err: &VfsError) -> bool {
        match err {
            VfsError::NotFound(_) => true,
            VfsError::OpenDal(inner) => inner.kind() == opendal::ErrorKind::NotFound,
            _ => false,
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
            let raw_path = info.path.as_ref();
            let normalized_path = if raw_path.contains('\\') {
                Cow::Owned(raw_path.replace("\\", "/"))
            } else {
                Cow::Borrowed(raw_path)
            };
            if let Some(logical) = normalized_path.strip_prefix(self.user_path_prefix.as_ref()) {
                info.path =
                    format!("/{}", logical.trim_start_matches('/').trim_end_matches('/')).into();
            } else if normalized_path == self.user_id.as_ref()
                || normalized_path == self.user_path_prefix.as_ref()
                || normalized_path == "."
                || normalized_path == "./"
            {
                info.path = "/".into();
            } else {
                let search_pattern = format!("/{}/", self.user_id);
                if let Some(pos) = normalized_path.find(&search_pattern) {
                    let start = pos + search_pattern.len();
                    if let Some(logical) = normalized_path.get(start..) {
                        info.path =
                            format!("/{}", logical.trim_start_matches('/').trim_end_matches('/'))
                                .into();
                    } else {
                        info.path = format!(
                            "/{}",
                            normalized_path
                                .trim_start_matches('/')
                                .trim_end_matches('/')
                        )
                        .into();
                    }
                } else {
                    info.path = format!(
                        "/{}",
                        normalized_path
                            .trim_start_matches('/')
                            .trim_end_matches('/')
                    )
                    .into();
                }
            }
        }
        info
    }
    pub(super) async fn ensure_recycle_bin_initialized(&self) -> VfsResult<()> {
        if self.recycle_bin_initialized.load(Ordering::Relaxed) {
            return Ok(());
        }
        self.pool
            .create_dir_all(&self.get_physical_path("/.recycle_bin").await?)
            .await?;
        self.recycle_bin_initialized.store(true, Ordering::Relaxed);
        Ok(())
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
            let local_path = self.temp_manager.get_user_temp_dir(&self.user_id).join(rel);
            if let Some(parent) = local_path.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(VfsError::Io)?;
            }
            let mut file = tokio::fs::File::create(&local_path)
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
        let skip_quota = self.is_thumbnail_cache_path(normalized);
        let initial_file_size = self.get_file_size(normalized).await;
        let quota_projection = if skip_quota {
            None
        } else {
            let pending_delta = self.pool.pending_quota_delta(&self.user_id);
            let settings = UserSettingsService::get_user_settings(&self.db, &self.user_id)
                .await
                .map_err(|e| VfsError::Internal(e.to_string()))?;
            settings.and_then(|settings| {
                (settings.storage_quota > 0).then_some((
                    settings.storage_used.saturating_add(pending_delta),
                    settings.storage_quota,
                ))
            })
        };
        while let Some(chunk) = stream.next().await {
            let data = chunk?;
            total_written += data.len() as u64;
            let current_diff = total_written as i64 - initial_file_size;
            if let Some((projected_used, quota_limit)) = quota_projection
                && current_diff > 0
                && projected_used.saturating_add(current_diff) > quota_limit
            {
                return Err(VfsError::QuotaExceeded);
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
        let physical_path = self.get_physical_path(normalized).await?;
        let wal_id = self
            .begin_wal(
                crate::vfs::wal::WalOperation::Write {
                    path: normalized.to_string(),
                    size: total_written,
                    protected: None,
                },
                self.should_skip_wal_for_write(normalized, total_written)
                    .await,
            )
            .await?;
        let write_result = self
            .pool
            .write_stream(&physical_path, Box::pin(vfs_stream))
            .await;
        let _ = tokio::fs::remove_file(&temp_file_path).await;
        if let Err(err) = write_result {
            self.fail_wal(wal_id, &err.to_string()).await;
            return Err(err);
        }
        self.mark_wal_physical_done(wal_id).await;
        if total_written as i64 - initial_file_size != 0 {
            let _ = self
                .update_quota(total_written as i64 - initial_file_size)
                .await;
        }
        let info = self.pool.stat(&physical_path).await?;
        let translated = self.translate_file_info(info, false);
        if let Err(err) = self.upsert_index_helper(normalized, &translated).await {
            self.fail_wal(
                wal_id,
                &format!(
                    "WRITE_STREAM metadata sync failed for {}: {}",
                    normalized, err
                ),
            )
            .await;
            return Err(err);
        }
        self.complete_wal(wal_id).await;
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
            ScopedVfsStorageEngine::validate_file_operation_impl("/documents/./file.txt").is_err()
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
