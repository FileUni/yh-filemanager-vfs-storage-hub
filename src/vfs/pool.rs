// VFS Storage Pool
//
// Manages primary and backup storage connections
use crate::config::VfsPoolConfig;
use crate::vfs::VfsResult;
use crate::vfs::VfsStorage;
use crate::vfs::types::{VfsBatchResult, VfsFileInfo, VfsMetadata};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use opendal::Operator;
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
/// VFS Storage Pool
#[derive(Debug, Clone)]
pub struct VfsPool {
    pub primary: Arc<Operator>,
    pub backup: Option<Arc<Operator>>,
    pub config: VfsPoolConfig,
    /// Shared sync guard manager across protocols and users
    pub sync_guards: Arc<crate::vfs::maintenance::SyncGuardManager>,
}
impl VfsPool {
    pub fn new(primary: Operator, backup: Option<Operator>, config: VfsPoolConfig) -> Self {
        Self {
            primary: Arc::new(primary),
            backup: backup.map(Arc::new),
            config,
            sync_guards: Arc::new(crate::vfs::maintenance::SyncGuardManager::new()),
        }
    }
}
#[async_trait]
impl VfsStorage for VfsPool {
    async fn read(&self, path: &str) -> VfsResult<(Bytes, VfsFileInfo)> {
        let data = self.primary.read(path).await?.to_bytes();
        let info = self.stat(path).await?;
        Ok((data, info))
    }
    async fn write(&self, path: &str, data: Bytes) -> VfsResult<VfsFileInfo> {
        self.primary.write(path, data).await?;
        self.stat(path).await
    }
    async fn delete(&self, path: &str) -> VfsResult<VfsFileInfo> {
        let info = self.stat(path).await?;
        self.primary.delete(path).await?;
        Ok(info)
    }
    async fn list(&self, path: &str) -> VfsResult<Vec<VfsFileInfo>> {
        use futures::StreamExt;
        let list_path = if !path.is_empty() && !path.ends_with('/') {
            format!("{}/", path)
        } else {
            path.to_string()
        };
        let mut lister = self.primary.lister(&list_path).await?;
        let mut result = Vec::new();
        while let Some(entry_res) = lister.next().await {
            let entry = entry_res?;
            // Try to get metadata directly
            let meta = entry.metadata();
            // FS stat
            // Compension: If critical metadata is missing (common in FS backend), do explicit stat
            let (is_dir, size, modified) = if meta.last_modified().is_none()
                || (meta.is_file() && meta.content_length() == 0)
            {
                match self.primary.stat(entry.path()).await {
                    Ok(st) => (
                        st.is_dir(),
                        st.content_length(),
                        st.last_modified()
                            .map(|t| std::time::SystemTime::from(t).into()),
                    ),
                    Err(_) => (
                        meta.is_dir(),
                        meta.content_length(),
                        meta.last_modified()
                            .map(|t| std::time::SystemTime::from(t).into()),
                    ),
                }
            } else {
                (
                    meta.is_dir(),
                    meta.content_length(),
                    meta.last_modified()
                        .map(|t| std::time::SystemTime::from(t).into()),
                )
            };
            let entry_path = normalize_entry_path(entry.path());
            result.push(VfsFileInfo {
                name: entry.name().trim_end_matches('/').into(),
                path: entry_path.into(),
                is_dir,
                size,
                modified,
                favorite_color: 0,
                has_active_share: None,
                has_active_direct: None,
                trashed_at: None,
                original_path: None,
            });
        }
        Ok(result)
    }
    fn list_stream(&self, path: &str) -> BoxStream<'static, VfsResult<VfsFileInfo>> {
        use futures::StreamExt;
        let primary = Arc::clone(&self.primary);
        let primary_for_lister = Arc::clone(&primary);
        let path_clone = if !path.is_empty() && !path.ends_with('/') {
            format!("{}/", path)
        } else {
            path.to_string()
        };
        Box::pin(
            futures::stream::once(async move { primary_for_lister.lister(&path_clone).await })
                .flat_map(move |lister_res| {
                    let primary_clone = Arc::clone(&primary);
                    match lister_res {
                        Ok(lister) => Box::pin(lister.then(move |entry_res| {
                            let primary_clone2 = Arc::clone(&primary_clone);
                            async move {
                                let entry = entry_res.map_err(crate::vfs::error::VfsError::from)?;
                                let meta = entry.metadata();
                                let (is_dir, size, modified) = if meta.last_modified().is_none()
                                    || (meta.is_file() && meta.content_length() == 0)
                                {
                                    match primary_clone2.stat(entry.path()).await {
                                        Ok(st) => (
                                            st.is_dir(),
                                            st.content_length(),
                                            st.last_modified()
                                                .map(|t| std::time::SystemTime::from(t).into()),
                                        ),
                                        Err(_) => (
                                            meta.is_dir(),
                                            meta.content_length(),
                                            meta.last_modified()
                                                .map(|t| std::time::SystemTime::from(t).into()),
                                        ),
                                    }
                                } else {
                                    (
                                        meta.is_dir(),
                                        meta.content_length(),
                                        meta.last_modified()
                                            .map(|t| std::time::SystemTime::from(t).into()),
                                    )
                                };
                                let entry_path = normalize_entry_path(entry.path());
                                Ok(VfsFileInfo {
                                    name: entry.name().trim_end_matches('/').into(),
                                    path: entry_path.into(),
                                    is_dir,
                                    size,
                                    modified,
                                    favorite_color: 0,
                                    has_active_share: None,
                                    has_active_direct: None,
                                    trashed_at: None,
                                    original_path: None,
                                })
                            }
                        }))
                            as BoxStream<'static, VfsResult<VfsFileInfo>>,
                        Err(e) => Box::pin(futures::stream::once(async move {
                            Err(crate::vfs::error::VfsError::from(e))
                        }))
                            as BoxStream<'static, VfsResult<VfsFileInfo>>,
                    }
                }),
        )
    }
    async fn list_recursive(&self, path: &str) -> VfsResult<Vec<VfsFileInfo>> {
        use futures::StreamExt;
        let mut lister = self.primary.lister_with(path).recursive(true).await?;
        let mut result = Vec::new();
        while let Some(entry_res) = lister.next().await {
            let entry = entry_res?;
            // Same compensation for recursive listing
            let meta = entry.metadata();
            let (is_dir, size, modified) = if meta.last_modified().is_none()
                || (meta.is_file() && meta.content_length() == 0)
            {
                match self.primary.stat(entry.path()).await {
                    Ok(st) => (
                        st.is_dir(),
                        st.content_length(),
                        st.last_modified()
                            .map(|t| std::time::SystemTime::from(t).into()),
                    ),
                    Err(_) => (
                        meta.is_dir(),
                        meta.content_length(),
                        meta.last_modified()
                            .map(|t| std::time::SystemTime::from(t).into()),
                    ),
                }
            } else {
                (
                    meta.is_dir(),
                    meta.content_length(),
                    meta.last_modified()
                        .map(|t| std::time::SystemTime::from(t).into()),
                )
            };
            let entry_path = normalize_entry_path(entry.path());
            result.push(VfsFileInfo {
                name: entry.name().trim_end_matches('/').into(),
                path: entry_path.into(),
                is_dir,
                size,
                modified,
                favorite_color: 0,
                has_active_share: None,
                has_active_direct: None,
                trashed_at: None,
                original_path: None,
            });
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
        let info = self.stat(path).await?;
        let reader = self.primary.reader(path).await?;
        let stream = reader.into_bytes_stream(range).await?;
        let vfs_stream = stream.map(|res| res.map_err(crate::vfs::error::VfsError::from));
        Ok((Box::pin(vfs_stream), info))
    }
    async fn write_stream(
        &self,
        path: &str,
        mut stream: BoxStream<'static, VfsResult<Bytes>>,
    ) -> VfsResult<VfsFileInfo> {
        use futures::StreamExt;
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
        Ok(self.primary.exists(path).await?)
    }
    async fn stat(&self, path: &str) -> VfsResult<VfsFileInfo> {
        let meta = self
            .primary
            .stat(path)
            .await
            .map_err(crate::vfs::error::VfsError::from)?;
        let file_name = file_name_from_path(path);
        Ok(VfsFileInfo {
            name: file_name.into(),
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
        })
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
        let data = self
            .primary
            .read_with(path)
            .range(start..end)
            .await?
            .to_bytes();
        let info = self.stat(path).await?;
        Ok((data, info))
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
        self.primary.create_dir(&dir_path).await?;
        self.stat(&dir_path).await
    }
    async fn create_dir_all(&self, path: &str) -> VfsResult<VfsFileInfo> {
        let dir_path = if path.ends_with('/') {
            path.to_string()
        } else {
            format!("{}/", path)
        };
        self.primary.create_dir(&dir_path).await?;
        self.stat(&dir_path).await
    }
    async fn move_file(&self, src: &str, dst: &str) -> VfsResult<VfsFileInfo> {
        self.primary.rename(src, dst).await?;
        self.stat(dst).await
    }
    async fn copy_file(&self, src: &str, dst: &str) -> VfsResult<VfsFileInfo> {
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
