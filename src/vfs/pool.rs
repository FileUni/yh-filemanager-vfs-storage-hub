//! Storage pool for VFS.
use crate::config::VfsPoolConfig;
use crate::vfs::VfsResult;
use crate::vfs::VfsStorage;
use crate::vfs::cache::{ReadCacheManager, WriteCacheManager};
use crate::vfs::types::{VfsBatchResult, VfsFileInfo, VfsMetadata};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use opendal::{Entry, Lister, Metadata, Operator};
use std::sync::Arc;

#[inline]
fn file_name_from_path(path: &str) -> &str {
    match path.rsplit('/').next() {
        Some(name) => name,
        None => path,
    }
}

#[inline]
fn normalize_entry_path(path: &str) -> &str {
    if path == "/" {
        return path;
    }
    match path.strip_suffix('/') {
        Some(trimmed) => trimmed,
        None => path,
    }
}

#[inline]
fn to_vfs_file_info(path: &str, meta: &Metadata) -> VfsFileInfo {
    VfsFileInfo {
        name: file_name_from_path(path).into(),
        path: path.into(),
        is_dir: meta.is_dir(),
        size: meta.content_length(),
        modified: meta
            .last_modified()
            .map(|t| std::time::SystemTime::from(t).into()),
        favorite_color: 0,
        has_active_share: None,
        has_active_direct: None,
        trashed_at: None,
        original_path: None,
    }
}

#[inline]
fn slice_bytes(data: Bytes, start: u64, end: u64) -> Bytes {
    let len = data.len() as u64;
    let bounded_start = start.min(len) as usize;
    let bounded_end = end.min(len) as usize;
    if bounded_start >= bounded_end {
        Bytes::new()
    } else {
        data.slice(bounded_start..bounded_end)
    }
}

#[inline]
fn clamp_read_range(range: std::ops::Range<u64>, size: u64) -> std::ops::Range<u64> {
    let start = range.start.min(size);
    let end = range.end.min(size);
    start..end.max(start)
}

#[inline]
fn should_refresh_entry_metadata(meta: &Metadata) -> bool {
    meta.last_modified().is_none()
}

/// VFS Storage Pool
#[derive(Clone)]
pub struct VfsPool {
    pub primary: Arc<Operator>,
    pub backup: Option<Arc<Operator>>,
    pub config: VfsPoolConfig,
    /// Shared sync guard manager across protocols and users
    pub sync_guards: Arc<crate::vfs::maintenance::SyncGuardManager>,
    pub read_cache: Option<Arc<ReadCacheManager>>,
    pub write_cache: Option<Arc<WriteCacheManager>>,
}
impl VfsPool {
    pub fn new(
        primary: Operator,
        backup: Option<Operator>,
        config: VfsPoolConfig,
        read_cache: Option<Arc<ReadCacheManager>>,
        write_cache: Option<Arc<WriteCacheManager>>,
    ) -> Self {
        Self {
            primary: Arc::new(primary),
            backup: backup.map(Arc::new),
            config,
            sync_guards: Arc::new(crate::vfs::maintenance::SyncGuardManager::new()),
            read_cache,
            write_cache,
        }
    }

    fn log_backup_fallback(&self, op: &str, path: &str, err: &crate::vfs::error::VfsError) {
        yh_console_log::yhlog(
            "warn",
            &format!(
                "VFS pool '{}' falling back to backup for {} on '{}' after primary error: {}",
                self.config.get_name(),
                op,
                path,
                err
            ),
        );
    }

    async fn stat_with_operator(&self, operator: &Operator, path: &str) -> VfsResult<VfsFileInfo> {
        let meta = operator
            .stat(path)
            .await
            .map_err(crate::vfs::error::VfsError::from)?;
        Ok(to_vfs_file_info(path, &meta))
    }

    async fn list_entry_info(&self, operator: &Operator, entry: Entry) -> VfsResult<VfsFileInfo> {
        let meta = entry.metadata();
        let path = normalize_entry_path(entry.path());
        if should_refresh_entry_metadata(meta) {
            match operator.stat(entry.path()).await {
                Ok(stat) => Ok(to_vfs_file_info(path, &stat)),
                Err(_) => Ok(to_vfs_file_info(path, meta)),
            }
        } else {
            Ok(to_vfs_file_info(path, meta))
        }
    }

    async fn select_lister(&self, path: &str) -> VfsResult<(Arc<Operator>, Lister)> {
        match self.primary.lister(path).await {
            Ok(lister) => Ok((Arc::clone(&self.primary), lister)),
            Err(err) => {
                let primary_err = crate::vfs::error::VfsError::from(err);
                let Some(backup) = &self.backup else {
                    return Err(primary_err);
                };
                self.log_backup_fallback("list", path, &primary_err);
                let lister = backup.lister(path).await?;
                Ok((Arc::clone(backup), lister))
            }
        }
    }

    async fn select_recursive_lister(&self, path: &str) -> VfsResult<(Arc<Operator>, Lister)> {
        match self.primary.lister_with(path).recursive(true).await {
            Ok(lister) => Ok((Arc::clone(&self.primary), lister)),
            Err(err) => {
                let primary_err = crate::vfs::error::VfsError::from(err);
                let Some(backup) = &self.backup else {
                    return Err(primary_err);
                };
                self.log_backup_fallback("list_recursive", path, &primary_err);
                let lister = backup.lister_with(path).recursive(true).await?;
                Ok((Arc::clone(backup), lister))
            }
        }
    }

    pub fn is_write_cache_enabled(&self) -> bool {
        self.write_cache
            .as_ref()
            .is_some_and(|cache| cache.is_enabled())
    }

    pub fn write_cache_max_file_size(&self) -> Option<u64> {
        self.write_cache
            .as_ref()
            .map(|cache| cache.max_file_size_bytes())
    }

    pub async fn enqueue_write_cache(
        &self,
        user_id: &str,
        logical_path: &str,
        physical_path: &str,
        data: Bytes,
    ) -> VfsResult<Option<VfsFileInfo>> {
        let Some(write_cache) = &self.write_cache else {
            return Ok(None);
        };
        write_cache
            .enqueue(user_id, logical_path, physical_path, data)
            .await
            .map_err(|err| crate::vfs::error::VfsError::Internal(err.to_string()))
    }

    pub async fn flush_write_cache(&self, physical_path: &str) -> VfsResult<()> {
        let Some(write_cache) = &self.write_cache else {
            return Ok(());
        };
        write_cache.flush_path(physical_path).await
    }

    pub async fn pending_stat(&self, physical_path: &str) -> Option<VfsFileInfo> {
        let write_cache = self.write_cache.as_ref()?;
        write_cache.pending_stat(physical_path).await
    }

    pub async fn pending_children(&self, parent_physical_path: &str) -> Vec<VfsFileInfo> {
        let Some(write_cache) = &self.write_cache else {
            return Vec::new();
        };
        write_cache.pending_children(parent_physical_path).await
    }

    pub fn is_dirty_path(&self, physical_path: &str) -> bool {
        self.write_cache
            .as_ref()
            .is_some_and(|cache| cache.is_dirty_path(physical_path))
    }

    pub fn is_dirty_dir(&self, parent_physical_path: &str) -> bool {
        self.write_cache
            .as_ref()
            .is_some_and(|cache| cache.is_dirty_dir(parent_physical_path))
    }

    pub fn pending_quota_delta(&self, user_id: &str) -> i64 {
        self.write_cache
            .as_ref()
            .map(|cache| cache.pending_quota_delta(user_id))
            .unwrap_or(0)
    }

    pub async fn invalidate_read_cache(&self, path: &str) {
        if let Some(read_cache) = &self.read_cache {
            read_cache.remove(path).await;
        }
    }

    pub async fn remove_tree(&self, path: &str) -> VfsResult<()> {
        let normalized = normalize_entry_path(path).to_string();
        let list_path = if normalized == "/" {
            "/".to_string()
        } else {
            format!("{}/", normalized.trim_end_matches('/'))
        };
        let mut entries = self
            .list_recursive(&list_path)
            .await?
            .into_iter()
            .filter(|entry| {
                let entry_path = normalize_entry_path(entry.path.as_ref());
                entry_path != normalized
            })
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.path.len());
        for entry in entries.into_iter().rev() {
            let mut delete_path = entry.path.to_string();
            if entry.is_dir && !delete_path.ends_with('/') {
                delete_path.push('/');
            }
            self.invalidate_read_cache(delete_path.as_str()).await;
            self.primary.delete(delete_path.as_str()).await?;
        }
        self.invalidate_read_cache(&normalized).await;
        self.primary.delete(&list_path).await?;
        Ok(())
    }
}
#[async_trait]
impl VfsStorage for VfsPool {
    async fn read(&self, path: &str) -> VfsResult<(Bytes, VfsFileInfo)> {
        if let Some(write_cache) = &self.write_cache
            && let Some(hit) = write_cache.pending_read(path).await
        {
            return Ok(hit);
        }
        if let Some(read_cache) = &self.read_cache
            && let Some(hit) = read_cache.get(path).await
        {
            return Ok(hit);
        }
        match self.primary.read(path).await {
            Ok(data) => {
                let info = self.stat_with_operator(self.primary.as_ref(), path).await?;
                let bytes = data.to_bytes();
                if let Some(read_cache) = &self.read_cache {
                    read_cache.put(path, bytes.clone(), info.clone()).await;
                }
                Ok((bytes, info))
            }
            Err(err) => {
                let primary_err = crate::vfs::error::VfsError::from(err);
                let Some(backup) = &self.backup else {
                    return Err(primary_err);
                };
                self.log_backup_fallback("read", path, &primary_err);
                let data = backup.read(path).await?.to_bytes();
                let info = self.stat_with_operator(backup.as_ref(), path).await?;
                Ok((data, info))
            }
        }
    }
    async fn write(&self, path: &str, data: Bytes) -> VfsResult<VfsFileInfo> {
        self.invalidate_read_cache(path).await;
        self.primary.write(path, data).await?;
        self.stat(path).await
    }
    async fn delete(&self, path: &str) -> VfsResult<VfsFileInfo> {
        let info = self.stat(path).await?;
        self.invalidate_read_cache(path).await;
        if info.is_dir {
            self.remove_tree(path).await?;
        } else {
            self.primary.delete(path).await?;
        }
        Ok(info)
    }
    async fn list(&self, path: &str) -> VfsResult<Vec<VfsFileInfo>> {
        use futures::StreamExt;
        let list_path = if !path.is_empty() && !path.ends_with('/') {
            format!("{}/", path)
        } else {
            path.to_string()
        };
        let (operator, mut lister) = self.select_lister(&list_path).await?;
        let mut result = Vec::new();
        while let Some(entry_res) = lister.next().await {
            result.push(self.list_entry_info(operator.as_ref(), entry_res?).await?);
        }
        Ok(result)
    }
    fn list_stream(&self, path: &str) -> BoxStream<'static, VfsResult<VfsFileInfo>> {
        use futures::StreamExt;
        let primary = Arc::clone(&self.primary);
        let backup = self.backup.as_ref().map(Arc::clone);
        let pool_name = self.config.get_name().to_string();
        let path_clone = if !path.is_empty() && !path.ends_with('/') {
            format!("{}/", path)
        } else {
            path.to_string()
        };
        Box::pin(
            futures::stream::once(async move {
                match primary.lister(&path_clone).await {
                    Ok(lister) => Ok((primary, lister)),
                    Err(err) => {
                        let primary_err = crate::vfs::error::VfsError::from(err);
                        let Some(backup) = backup else {
                            return Err(primary_err);
                        };
                        yh_console_log::yhlog(
                            "warn",
                            &format!(
                                "VFS pool '{}' falling back to backup for list_stream on '{}' after primary error: {}",
                                pool_name, path_clone, primary_err
                            ),
                        );
                        let lister = backup.lister(&path_clone).await?;
                        Ok((backup, lister))
                    }
                }
            })
            .flat_map(move |state| match state {
                Ok((operator, lister)) => Box::pin(lister.then(move |entry_res| {
                    let operator = Arc::clone(&operator);
                    async move {
                        let entry = entry_res.map_err(crate::vfs::error::VfsError::from)?;
                        let meta = entry.metadata();
                        let path = normalize_entry_path(entry.path());
                        if should_refresh_entry_metadata(meta) {
                            match operator.stat(entry.path()).await {
                                Ok(stat) => Ok(to_vfs_file_info(path, &stat)),
                                Err(_) => Ok(to_vfs_file_info(path, meta)),
                            }
                        } else {
                            Ok(to_vfs_file_info(path, meta))
                        }
                    }
                }))
                    as BoxStream<'static, VfsResult<VfsFileInfo>>,
                Err(err) => {
                    Box::pin(futures::stream::once(async move { Err(err) }))
                        as BoxStream<'static, VfsResult<VfsFileInfo>>
                }
            }),
        )
    }
    async fn list_recursive(&self, path: &str) -> VfsResult<Vec<VfsFileInfo>> {
        use futures::StreamExt;
        let (operator, mut lister) = self.select_recursive_lister(path).await?;
        let mut result = Vec::new();
        while let Some(entry_res) = lister.next().await {
            result.push(self.list_entry_info(operator.as_ref(), entry_res?).await?);
        }
        Ok(result)
    }
    async fn read_stream(
        &self,
        path: &str,
    ) -> VfsResult<(
        std::pin::Pin<Box<dyn futures::Stream<Item = VfsResult<Bytes>> + Send + Sync>>,
        VfsFileInfo,
    )> {
        self.read_stream_range(path, 0..u64::MAX).await
    }
    async fn read_stream_range(
        &self,
        path: &str,
        range: std::ops::Range<u64>,
    ) -> VfsResult<(
        std::pin::Pin<Box<dyn futures::Stream<Item = VfsResult<Bytes>> + Send + Sync>>,
        VfsFileInfo,
    )> {
        use futures::StreamExt;
        if let Some(write_cache) = &self.write_cache
            && let Some((data, info)) = write_cache.pending_read(path).await
        {
            let payload = slice_bytes(data, range.start, range.end);
            let stream = futures::stream::once(async move { Ok(payload) });
            return Ok((Box::pin(stream), info));
        }
        if let Some(read_cache) = &self.read_cache
            && let Some((data, info)) = read_cache.get(path).await
        {
            let payload = slice_bytes(data, range.start, range.end);
            let stream = futures::stream::once(async move { Ok(payload) });
            return Ok((Box::pin(stream), info));
        }
        let (reader, info) = match self.primary.reader(path).await {
            Ok(reader) => (
                reader,
                self.stat_with_operator(self.primary.as_ref(), path).await?,
            ),
            Err(err) => {
                let primary_err = crate::vfs::error::VfsError::from(err);
                let Some(backup) = &self.backup else {
                    return Err(primary_err);
                };
                self.log_backup_fallback("read_stream_range", path, &primary_err);
                let reader = backup.reader(path).await?;
                let info = self.stat_with_operator(backup.as_ref(), path).await?;
                (reader, info)
            }
        };
        let stream = reader
            .into_bytes_stream(clamp_read_range(range, info.size))
            .await?;
        let vfs_stream = stream.map(|res| res.map_err(crate::vfs::error::VfsError::from));
        Ok((Box::pin(vfs_stream), info))
    }
    async fn write_stream(
        &self,
        path: &str,
        mut stream: BoxStream<'static, VfsResult<Bytes>>,
    ) -> VfsResult<VfsFileInfo> {
        use futures::StreamExt;
        self.invalidate_read_cache(path).await;
        // Use temporary file for atomic write
        let temp_path = format!("{}.tmp", path);
        // Write to temporary file
        let mut writer = self.primary.writer(&temp_path).await?;
        while let Some(chunk) = stream.next().await {
            writer.write(chunk?).await?;
        }
        writer.close().await?;
        // Rename to final filename
        self.primary.rename(&temp_path, path).await?;
        self.stat(path).await
    }
    async fn exists(&self, path: &str) -> VfsResult<bool> {
        if let Some(write_cache) = &self.write_cache
            && write_cache.pending_stat(path).await.is_some()
        {
            return Ok(true);
        }
        match self.primary.exists(path).await {
            Ok(true) => Ok(true),
            Ok(false) => {
                if let Some(backup) = &self.backup {
                    return Ok(backup.exists(path).await?);
                }
                Ok(false)
            }
            Err(err) => {
                let primary_err = crate::vfs::error::VfsError::from(err);
                let Some(backup) = &self.backup else {
                    return Err(primary_err);
                };
                self.log_backup_fallback("exists", path, &primary_err);
                Ok(backup.exists(path).await?)
            }
        }
    }
    async fn stat(&self, path: &str) -> VfsResult<VfsFileInfo> {
        if let Some(write_cache) = &self.write_cache
            && let Some(info) = write_cache.pending_stat(path).await
        {
            return Ok(info);
        }
        match self.stat_with_operator(self.primary.as_ref(), path).await {
            Ok(info) => Ok(info),
            Err(primary_err) => {
                let Some(backup) = &self.backup else {
                    return Err(primary_err);
                };
                self.log_backup_fallback("stat", path, &primary_err);
                self.stat_with_operator(backup.as_ref(), path).await
            }
        }
    }
    async fn metadata(&self, path: &str) -> VfsResult<VfsMetadata> {
        let info = self.stat(path).await?;
        Ok(VfsMetadata {
            path: info.path,
            is_dir: info.is_dir,
            size: info.size,
            modified: info.modified,
            content_type: None,
            etag: None,
        })
    }
    async fn read_range(
        &self,
        path: &str,
        start: u64,
        end: u64,
    ) -> VfsResult<(Bytes, VfsFileInfo)> {
        if let Some(write_cache) = &self.write_cache
            && let Some((data, info)) = write_cache.pending_read(path).await
        {
            return Ok((slice_bytes(data, start, end), info));
        }
        if let Some(read_cache) = &self.read_cache
            && let Some((data, info)) = read_cache.get(path).await
        {
            return Ok((slice_bytes(data, start, end), info));
        }
        let primary_info = self.stat_with_operator(self.primary.as_ref(), path).await;
        match primary_info {
            Ok(info) => {
                let range = clamp_read_range(start..end, info.size);
                let data = self.primary.read_with(path).range(range).await?;
                Ok((data.to_bytes(), info))
            }
            Err(err) => {
                let primary_err = err;
                let Some(backup) = &self.backup else {
                    return Err(primary_err);
                };
                self.log_backup_fallback("read_range", path, &primary_err);
                let info = self.stat_with_operator(backup.as_ref(), path).await?;
                let range = clamp_read_range(start..end, info.size);
                let data = backup.read_with(path).range(range).await?.to_bytes();
                Ok((data, info))
            }
        }
    }
    async fn write_at(&self, _path: &str, _offset: u64, _data: Bytes) -> VfsResult<VfsFileInfo> {
        Err(crate::vfs::error::VfsError::Internal(
            "Write at not supported in Pool".to_string(),
        ))
    }
    async fn set_times(
        &self,
        path: &str,
        _atime: Option<u64>,
        _mtime: Option<u64>,
    ) -> VfsResult<VfsFileInfo> {
        self.stat(path).await
    }
    async fn check_quota(&self, _additional_size: i64) -> VfsResult<()> {
        Ok(())
    }
    async fn get_quota(&self) -> VfsResult<(u64, Option<u64>)> {
        Ok((0, None))
    }
    async fn create_dir(&self, path: &str) -> VfsResult<VfsFileInfo> {
        let dir_path = if path.ends_with('/') {
            path.to_string()
        } else {
            format!("{}/", path)
        };
        self.invalidate_read_cache(&dir_path).await;
        self.primary.create_dir(&dir_path).await?;
        self.stat(&dir_path).await
    }
    async fn create_dir_all(&self, path: &str) -> VfsResult<VfsFileInfo> {
        let dir_path = if path.ends_with('/') {
            path.to_string()
        } else {
            format!("{}/", path)
        };
        self.invalidate_read_cache(&dir_path).await;
        self.primary.create_dir(&dir_path).await?;
        self.stat(&dir_path).await
    }
    async fn move_file(&self, src: &str, dst: &str) -> VfsResult<VfsFileInfo> {
        self.invalidate_read_cache(src).await;
        self.invalidate_read_cache(dst).await;
        self.primary.rename(src, dst).await?;
        self.stat(dst).await
    }
    async fn copy_file(&self, src: &str, dst: &str) -> VfsResult<VfsFileInfo> {
        self.invalidate_read_cache(src).await;
        self.invalidate_read_cache(dst).await;
        self.primary.copy(src, dst).await?;
        self.stat(dst).await
    }
    async fn canonicalize_path(&self, path: &str) -> VfsResult<String> {
        Ok(path.to_string())
    }
    async fn batch_remove(&self, paths: &[String]) -> VfsResult<VfsBatchResult> {
        let mut success = Vec::new();
        let mut failed = Vec::new();
        for path in paths {
            match self.delete(path).await {
                Ok(_) => success.push(path.as_str().into()),
                Err(e) => failed.push(crate::vfs::types::VfsBatchError {
                    path: path.as_str().into(),
                    error: e.to_string().into(),
                }),
            }
        }
        Ok(VfsBatchResult { success, failed })
    }
    async fn batch_move(&self, src_paths: &[String], dst_dir: &str) -> VfsResult<VfsBatchResult> {
        let mut success = Vec::new();
        let mut failed = Vec::new();
        for src in src_paths {
            let name = file_name_from_path(src);
            let dst = format!("{}/{}", dst_dir.trim_end_matches('/'), name);
            match self.move_file(src, &dst).await {
                Ok(_) => success.push(src.as_str().into()),
                Err(e) => failed.push(crate::vfs::types::VfsBatchError {
                    path: src.as_str().into(),
                    error: e.to_string().into(),
                }),
            }
        }
        Ok(VfsBatchResult { success, failed })
    }
    async fn compress(
        &self,
        source_path: &str,
        target_path: &str,
        user_id: &str,
        password: Option<&str>,
        encrypt_filenames: bool,
    ) -> VfsResult<VfsFileInfo> {
        use crate::utils::compression::compress_compat;
        compress_compat(
            self,
            source_path,
            target_path,
            user_id,
            password,
            encrypt_filenames,
        )
        .await
    }
    async fn decompress(
        &self,
        archive_path: &str,
        target_dir: &str,
        user_id: &str,
        overwrite: bool,
        password: Option<&str>,
    ) -> VfsResult<VfsFileInfo> {
        use crate::utils::compression::decompress_compat;
        decompress_compat(self, archive_path, target_dir, user_id, overwrite, password).await
    }
    async fn create_temp_file_for_upload(
        &self,
    ) -> VfsResult<(std::path::PathBuf, crate::utils::VfsTempFileGuard)> {
        Err(crate::vfs::error::VfsError::Internal(
            "Temp file not supported in Pool".to_string(),
        ))
    }
    // Business Extensions ===
    fn get_recursive_size(
        &self,
        _path: &str,
    ) -> futures::future::BoxFuture<'static, VfsResult<i64>> {
        Box::pin(async move {
            Err(crate::vfs::error::VfsError::Internal(
                "Not implemented in Pool".to_string(),
            ))
        })
    }
    async fn set_favorite(&self, path: &str, _color: i32) -> VfsResult<VfsFileInfo> {
        self.stat(path).await
    }
    async fn list_favorites(&self, _color_filter: Option<i32>) -> VfsResult<Vec<VfsFileInfo>> {
        Ok(vec![])
    }
    async fn move_to_trash(&self, path: &str) -> VfsResult<VfsFileInfo> {
        self.stat(path).await
    }
    async fn restore_from_trash(&self, path: &str) -> VfsResult<VfsFileInfo> {
        self.stat(path).await
    }
    async fn list_trash(&self) -> VfsResult<Vec<VfsFileInfo>> {
        Ok(vec![])
    }
    async fn sync_index(&self, _path: &str) -> VfsResult<Vec<VfsFileInfo>> {
        Ok(vec![])
    }
    async fn migrate_storage(
        &self,
        path: &str,
        _target_storage_id: &str,
    ) -> VfsResult<VfsFileInfo> {
        Ok(self.stat(path).await?)
    }
    fn get_backend_type(&self) -> String {
        self.primary.info().scheme().to_string()
    }
}
