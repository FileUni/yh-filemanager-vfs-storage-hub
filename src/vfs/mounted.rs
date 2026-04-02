use async_trait::async_trait;
use bytes::Bytes;
use chrono::{Duration as ChronoDuration, Utc};
use futures::TryStreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use once_cell::sync::Lazy;
use sea_orm::DatabaseConnection;
use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore};

use crate::business::{RemoteMountService, RemoteMountSnapshot, RemoteMountSyncMode};
use crate::config::{VfsConnectorConfig, VfsPoolConfig};
use crate::vfs::connector::build_operator;
use crate::vfs::{
    VfsBatchError, VfsBatchResult, VfsError, VfsFileInfo, VfsMetadata, VfsPaginationParams,
    VfsPool, VfsResult, VfsStorage, VfsStorageHub,
};

#[derive(Clone)]
pub struct MountRuntime {
    pub snapshot: RemoteMountSnapshot,
    pub storage: Arc<dyn VfsStorage>,
}

#[derive(Clone)]
pub struct MountedUserStorage {
    base: Arc<dyn VfsStorage>,
    mounts: Vec<MountRuntime>,
    user_id: Arc<str>,
    task_handler: Option<Arc<dyn crate::vfs::task::VfsTaskHandler>>,
}

#[derive(Clone)]
struct StorageEndpoint {
    storage: Arc<dyn VfsStorage>,
    path: String,
    mount_id: Option<String>,
    is_mount_root: bool,
}

static MOUNT_SYNC_LOCKS: Lazy<Mutex<HashMap<String, Arc<AsyncMutex<()>>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static BATCH_TASK_SEMAPHORE: once_cell::sync::OnceCell<Arc<Semaphore>> =
    once_cell::sync::OnceCell::new();
const MOUNTED_BATCH_TASK_TIMEOUT: Duration = Duration::from_secs(24 * 3600);

fn normalize_logical_path(path: &str) -> String {
    if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{}", path)
    }
}

fn normalize_dir_path(path: &str) -> String {
    if path == "/" {
        "/".to_string()
    } else {
        normalize_logical_path(path)
            .trim_end_matches('/')
            .to_string()
    }
}

fn is_path_within(path: &str, dir: &str) -> bool {
    path == dir
        || path
            .strip_prefix(dir)
            .is_some_and(|rest| rest.starts_with('/'))
}

fn join_logical_path(base: &str, child: &str) -> String {
    if child.is_empty() {
        return base.to_string();
    }
    if base == "/" {
        format!("/{}", child.trim_start_matches('/'))
    } else {
        format!(
            "{}/{}",
            base.trim_end_matches('/'),
            child.trim_start_matches('/')
        )
    }
}

fn file_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

async fn ensure_parent_dir(storage: &Arc<dyn VfsStorage>, path: &str) -> VfsResult<()> {
    let parent = Path::new(path)
        .parent()
        .map(|value| value.to_string_lossy().to_string())
        .unwrap_or_else(|| "/".to_string());
    if parent != "/" && !storage.exists(&parent).await? {
        let _ = storage.create_dir_all(&parent).await?;
    }
    Ok(())
}

fn copy_path_between(
    src_storage: Arc<dyn VfsStorage>,
    src_path: String,
    dst_storage: Arc<dyn VfsStorage>,
    dst_path: String,
) -> BoxFuture<'static, VfsResult<()>> {
    Box::pin(async move {
        let info = src_storage.stat(&src_path).await?;
        if info.is_dir {
            let _ = dst_storage.create_dir_all(&dst_path).await?;
            let entries = src_storage.list(&src_path).await?;
            for entry in entries {
                let child_dst = join_logical_path(&dst_path, entry.name.as_ref());
                copy_path_between(
                    Arc::clone(&src_storage),
                    entry.path.to_string(),
                    Arc::clone(&dst_storage),
                    child_dst,
                )
                .await?;
            }
            return Ok(());
        }

        ensure_parent_dir(&dst_storage, &dst_path).await?;
        let (stream, _info) = src_storage.read_stream(&src_path).await?;
        let _ = dst_storage
            .write_stream(&dst_path, Box::pin(stream))
            .await?;
        Ok(())
    })
}

fn delete_path_recursive(
    storage: Arc<dyn VfsStorage>,
    path: String,
) -> BoxFuture<'static, VfsResult<()>> {
    Box::pin(async move {
        let info = storage.stat(&path).await?;
        if info.is_dir {
            let entries = storage.list(&path).await?;
            for entry in entries {
                delete_path_recursive(Arc::clone(&storage), entry.path.to_string()).await?;
            }
        }
        let _ = storage.delete(&path).await?;
        Ok(())
    })
}

impl MountedUserStorage {
    pub fn new(
        base: Arc<dyn VfsStorage>,
        mounts: Vec<MountRuntime>,
        user_id: Arc<str>,
        task_handler: Option<Arc<dyn crate::vfs::task::VfsTaskHandler>>,
    ) -> Self {
        let mut mounts = mounts;
        mounts.sort_by(|left, right| {
            right
                .snapshot
                .model
                .mount_dir
                .len()
                .cmp(&left.snapshot.model.mount_dir.len())
        });
        Self {
            base,
            mounts,
            user_id,
            task_handler,
        }
    }

    async fn acquire_batch_task_permit() -> VfsResult<OwnedSemaphorePermit> {
        let semaphore = if let Some(existing) = BATCH_TASK_SEMAPHORE.get() {
            Arc::clone(existing)
        } else {
            let cfg = crate::config::get_vfs_hub_config().await;
            let permits = cfg
                .get_batch_operation()
                .get_effective_max_concurrent_tasks();
            let created = Arc::new(Semaphore::new(permits));
            let _ = BATCH_TASK_SEMAPHORE.set(Arc::clone(&created));
            created
        };
        semaphore
            .acquire_owned()
            .await
            .map_err(|err| VfsError::Internal(format!("Acquire batch task permit failed: {}", err)))
    }

    fn spawn_batch_task<F>(task_name: &'static str, task: F)
    where
        F: futures::Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(async move {
            if tokio::time::timeout(MOUNTED_BATCH_TASK_TIMEOUT, task)
                .await
                .is_err()
            {
                yh_console_log::yhlog(
                    "error",
                    &format!(
                        "Mounted batch task '{}' timed out after 24 hours",
                        task_name
                    ),
                );
            }
        });
    }

    fn contains_mounted_path(&self, path: &str) -> bool {
        self.resolve_runtime(path).is_some()
    }

    fn validate_path(path: &str) -> VfsResult<String> {
        if path.chars().any(|c| c.is_control()) {
            return Err(VfsError::Internal(
                "Security violation: control characters in path".to_string(),
            ));
        }
        let normalized = normalize_logical_path(path).replace('\\', "/");
        if normalized.contains("..") {
            return Err(VfsError::Internal(
                "Security violation: parent path segments are not allowed".to_string(),
            ));
        }
        Ok(normalized)
    }

    fn mount_root_info(runtime: &MountRuntime) -> VfsFileInfo {
        let path = runtime.snapshot.model.mount_dir.as_str();
        VfsFileInfo {
            name: file_name(path).into(),
            path: path.into(),
            is_dir: true,
            size: 0,
            modified: runtime
                .snapshot
                .model
                .last_sync_at
                .or(Some(runtime.snapshot.model.updated_at)),
            favorite_color: 0,
            has_active_share: None,
            has_active_direct: None,
            trashed_at: None,
            original_path: None,
        }
    }

    fn map_runtime_path(runtime: &MountRuntime, remote_path: &str) -> String {
        let mount_dir = runtime.snapshot.model.mount_dir.as_str();
        if remote_path == "/" || remote_path.is_empty() {
            mount_dir.to_string()
        } else {
            join_logical_path(mount_dir, remote_path.trim_start_matches('/'))
        }
    }

    fn map_remote_info(runtime: &MountRuntime, info: VfsFileInfo) -> VfsFileInfo {
        let mapped_path = Self::map_runtime_path(runtime, info.path.as_ref());
        let mapped_name = if mapped_path == runtime.snapshot.model.mount_dir {
            file_name(&mapped_path).to_string()
        } else {
            info.name.to_string()
        };
        VfsFileInfo {
            name: mapped_name.into(),
            path: mapped_path.into(),
            is_dir: info.is_dir,
            size: info.size,
            modified: info.modified,
            favorite_color: info.favorite_color,
            has_active_share: info.has_active_share,
            has_active_direct: info.has_active_direct,
            trashed_at: info.trashed_at,
            original_path: info.original_path,
        }
    }

    fn resolve_runtime<'a>(&'a self, path: &str) -> Option<(&'a MountRuntime, String)> {
        let normalized = normalize_dir_path(path);
        self.mounts.iter().find_map(|runtime| {
            let mount_dir = runtime.snapshot.model.mount_dir.as_str();
            if normalized == mount_dir {
                Some((runtime, "/".to_string()))
            } else {
                normalized
                    .strip_prefix(mount_dir)
                    .filter(|rest| rest.starts_with('/'))
                    .map(|rest| (runtime, normalize_logical_path(rest)))
            }
        })
    }

    fn direct_mount_children(&self, parent_path: &str) -> Vec<&MountRuntime> {
        let normalized = normalize_dir_path(parent_path);
        self.mounts
            .iter()
            .filter(|runtime| {
                let mount_dir = runtime.snapshot.model.mount_dir.as_str();
                if mount_dir == "/" {
                    return false;
                }
                let parent = Path::new(mount_dir)
                    .parent()
                    .map(|value| value.to_string_lossy().to_string())
                    .unwrap_or_else(|| "/".to_string());
                normalize_dir_path(&parent) == normalized
            })
            .collect()
    }

    fn descendant_mounts(&self, root_path: &str) -> Vec<&MountRuntime> {
        let normalized = normalize_dir_path(root_path);
        self.mounts
            .iter()
            .filter(|runtime| {
                is_path_within(runtime.snapshot.model.mount_dir.as_str(), &normalized)
            })
            .collect()
    }

    fn resolve_endpoint(&self, path: &str) -> VfsResult<StorageEndpoint> {
        let normalized = Self::validate_path(path)?;
        if let Some((runtime, remote_path)) = self.resolve_runtime(&normalized) {
            return Ok(StorageEndpoint {
                storage: Arc::clone(&runtime.storage),
                path: remote_path.clone(),
                mount_id: Some(runtime.snapshot.model.id.clone()),
                is_mount_root: remote_path == "/",
            });
        }
        Ok(StorageEndpoint {
            storage: Arc::clone(&self.base),
            path: normalized,
            mount_id: None,
            is_mount_root: false,
        })
    }

    async fn merge_local_listing(&self, path: &str) -> VfsResult<Vec<VfsFileInfo>> {
        let mut entries = self.base.list(path).await.unwrap_or_default();
        let mount_entries = self
            .direct_mount_children(path)
            .into_iter()
            .map(Self::mount_root_info);
        entries.extend(mount_entries);
        entries.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(entries)
    }

    async fn generic_move_or_copy(
        &self,
        src: &str,
        dst: &str,
        delete_source: bool,
    ) -> VfsResult<VfsFileInfo> {
        let src_endpoint = self.resolve_endpoint(src)?;
        let dst_endpoint = self.resolve_endpoint(dst)?;
        if src_endpoint.is_mount_root || dst_endpoint.is_mount_root {
            return Err(VfsError::PermissionDenied(
                "Moving or copying a mount root is not supported".to_string(),
            ));
        }
        copy_path_between(
            Arc::clone(&src_endpoint.storage),
            src_endpoint.path.clone(),
            Arc::clone(&dst_endpoint.storage),
            dst_endpoint.path.clone(),
        )
        .await?;
        if delete_source {
            delete_path_recursive(Arc::clone(&src_endpoint.storage), src_endpoint.path).await?;
        }
        self.stat(dst).await
    }
}

#[async_trait]
impl VfsStorage for MountedUserStorage {
    async fn read(&self, path: &str) -> VfsResult<(Bytes, VfsFileInfo)> {
        let endpoint = self.resolve_endpoint(path)?;
        if endpoint.is_mount_root {
            return Err(VfsError::Internal(
                "Cannot read a mount root directory".to_string(),
            ));
        }
        let (data, info) = endpoint.storage.read(&endpoint.path).await?;
        if let Some(mount_id) = endpoint.mount_id
            && let Some(runtime) = self
                .mounts
                .iter()
                .find(|item| item.snapshot.model.id == mount_id)
        {
            return Ok((data, Self::map_remote_info(runtime, info)));
        }
        Ok((data, info))
    }

    async fn write(&self, path: &str, data: Bytes) -> VfsResult<VfsFileInfo> {
        let endpoint = self.resolve_endpoint(path)?;
        if endpoint.is_mount_root {
            return Err(VfsError::PermissionDenied(
                "Cannot overwrite a mount root".to_string(),
            ));
        }
        let info = endpoint.storage.write(&endpoint.path, data).await?;
        if let Some(mount_id) = endpoint.mount_id
            && let Some(runtime) = self
                .mounts
                .iter()
                .find(|item| item.snapshot.model.id == mount_id)
        {
            return Ok(Self::map_remote_info(runtime, info));
        }
        Ok(info)
    }

    async fn delete(&self, path: &str) -> VfsResult<VfsFileInfo> {
        let endpoint = self.resolve_endpoint(path)?;
        if endpoint.is_mount_root {
            return Err(VfsError::PermissionDenied(
                "Use mount management to remove a mounted directory".to_string(),
            ));
        }
        let info = endpoint.storage.delete(&endpoint.path).await?;
        if let Some(mount_id) = endpoint.mount_id
            && let Some(runtime) = self
                .mounts
                .iter()
                .find(|item| item.snapshot.model.id == mount_id)
        {
            return Ok(Self::map_remote_info(runtime, info));
        }
        Ok(info)
    }

    async fn list(&self, path: &str) -> VfsResult<Vec<VfsFileInfo>> {
        let normalized = Self::validate_path(path)?;
        if let Some((runtime, remote_path)) = self.resolve_runtime(&normalized) {
            let entries = runtime.storage.list(&remote_path).await?;
            return Ok(entries
                .into_iter()
                .map(|info| Self::map_remote_info(runtime, info))
                .collect());
        }
        self.merge_local_listing(&normalized).await
    }

    fn list_stream(&self, path: &str) -> BoxStream<'static, VfsResult<VfsFileInfo>> {
        let storage = self.clone();
        let path = path.to_owned();
        Box::pin(
            futures::stream::once(async move {
                storage
                    .list(&path)
                    .await
                    .map(|entries| futures::stream::iter(entries.into_iter().map(Ok)))
            })
            .try_flatten(),
        )
    }

    async fn list_recursive(&self, path: &str) -> VfsResult<Vec<VfsFileInfo>> {
        let normalized = Self::validate_path(path)?;
        if let Some((runtime, remote_path)) = self.resolve_runtime(&normalized) {
            let entries = runtime.storage.list_recursive(&remote_path).await?;
            return Ok(entries
                .into_iter()
                .map(|info| Self::map_remote_info(runtime, info))
                .collect());
        }
        let mut entries = self
            .base
            .list_recursive(&normalized)
            .await
            .unwrap_or_default();
        for runtime in self.descendant_mounts(&normalized) {
            entries.push(Self::mount_root_info(runtime));
            let remote_entries = runtime.storage.list_recursive("/").await?;
            entries.extend(
                remote_entries
                    .into_iter()
                    .map(|info| Self::map_remote_info(runtime, info)),
            );
        }
        Ok(entries)
    }

    async fn exists(&self, path: &str) -> VfsResult<bool> {
        let endpoint = self.resolve_endpoint(path)?;
        if endpoint.is_mount_root {
            return Ok(true);
        }
        endpoint.storage.exists(&endpoint.path).await
    }

    async fn stat(&self, path: &str) -> VfsResult<VfsFileInfo> {
        let endpoint = self.resolve_endpoint(path)?;
        if endpoint.is_mount_root
            && let Some(mount_id) = endpoint.mount_id.as_deref()
            && let Some(runtime) = self
                .mounts
                .iter()
                .find(|item| item.snapshot.model.id == mount_id)
        {
            return Ok(Self::mount_root_info(runtime));
        }
        let info = endpoint.storage.stat(&endpoint.path).await?;
        if let Some(mount_id) = endpoint.mount_id.as_deref()
            && let Some(runtime) = self
                .mounts
                .iter()
                .find(|item| item.snapshot.model.id == mount_id)
        {
            return Ok(Self::map_remote_info(runtime, info));
        }
        Ok(info)
    }

    async fn read_stream(
        &self,
        path: &str,
    ) -> VfsResult<(
        Pin<Box<dyn futures::Stream<Item = VfsResult<Bytes>> + Send + Sync>>,
        VfsFileInfo,
    )> {
        let endpoint = self.resolve_endpoint(path)?;
        if endpoint.is_mount_root {
            return Err(VfsError::Internal(
                "Cannot stream a mount root directory".to_string(),
            ));
        }
        let (stream, info) = endpoint.storage.read_stream(&endpoint.path).await?;
        if let Some(mount_id) = endpoint.mount_id
            && let Some(runtime) = self
                .mounts
                .iter()
                .find(|item| item.snapshot.model.id == mount_id)
        {
            return Ok((stream, Self::map_remote_info(runtime, info)));
        }
        Ok((stream, info))
    }

    async fn read_stream_range(
        &self,
        path: &str,
        range: std::ops::Range<u64>,
    ) -> VfsResult<(
        Pin<Box<dyn futures::Stream<Item = VfsResult<Bytes>> + Send + Sync>>,
        VfsFileInfo,
    )> {
        let endpoint = self.resolve_endpoint(path)?;
        if endpoint.is_mount_root {
            return Err(VfsError::Internal(
                "Cannot stream a mount root directory".to_string(),
            ));
        }
        let (stream, info) = endpoint
            .storage
            .read_stream_range(&endpoint.path, range)
            .await?;
        if let Some(mount_id) = endpoint.mount_id
            && let Some(runtime) = self
                .mounts
                .iter()
                .find(|item| item.snapshot.model.id == mount_id)
        {
            return Ok((stream, Self::map_remote_info(runtime, info)));
        }
        Ok((stream, info))
    }

    async fn write_stream(
        &self,
        path: &str,
        stream: BoxStream<'static, VfsResult<Bytes>>,
    ) -> VfsResult<VfsFileInfo> {
        let endpoint = self.resolve_endpoint(path)?;
        if endpoint.is_mount_root {
            return Err(VfsError::PermissionDenied(
                "Cannot write a mount root".to_string(),
            ));
        }
        let info = endpoint
            .storage
            .write_stream(&endpoint.path, stream)
            .await?;
        if let Some(mount_id) = endpoint.mount_id
            && let Some(runtime) = self
                .mounts
                .iter()
                .find(|item| item.snapshot.model.id == mount_id)
        {
            return Ok(Self::map_remote_info(runtime, info));
        }
        Ok(info)
    }

    async fn metadata(&self, path: &str) -> VfsResult<VfsMetadata> {
        let endpoint = self.resolve_endpoint(path)?;
        if endpoint.is_mount_root {
            return Ok(VfsMetadata {
                path: path.into(),
                is_dir: true,
                size: 0,
                modified: None,
                content_type: None,
                etag: None,
            });
        }
        let meta = endpoint.storage.metadata(&endpoint.path).await?;
        if endpoint.mount_id.is_some() {
            return Ok(VfsMetadata {
                path: normalize_logical_path(path).into(),
                is_dir: meta.is_dir,
                size: meta.size,
                modified: meta.modified,
                content_type: meta.content_type,
                etag: meta.etag,
            });
        }
        Ok(meta)
    }

    async fn read_range(
        &self,
        path: &str,
        start: u64,
        end: u64,
    ) -> VfsResult<(Bytes, VfsFileInfo)> {
        let endpoint = self.resolve_endpoint(path)?;
        if endpoint.is_mount_root {
            return Err(VfsError::Internal(
                "Cannot read a mount root directory".to_string(),
            ));
        }
        let (data, info) = endpoint
            .storage
            .read_range(&endpoint.path, start, end)
            .await?;
        if let Some(mount_id) = endpoint.mount_id
            && let Some(runtime) = self
                .mounts
                .iter()
                .find(|item| item.snapshot.model.id == mount_id)
        {
            return Ok((data, Self::map_remote_info(runtime, info)));
        }
        Ok((data, info))
    }

    async fn write_at(&self, path: &str, offset: u64, data: Bytes) -> VfsResult<VfsFileInfo> {
        let endpoint = self.resolve_endpoint(path)?;
        if endpoint.mount_id.is_none() {
            return endpoint
                .storage
                .write_at(&endpoint.path, offset, data)
                .await;
        }
        if endpoint.is_mount_root {
            return Err(VfsError::PermissionDenied(
                "Cannot write a mount root".to_string(),
            ));
        }
        let (existing, _) = endpoint.storage.read(&endpoint.path).await?;
        let mut content = existing.to_vec();
        let start = offset as usize;
        let end = start.saturating_add(data.len());
        if content.len() < start {
            content.resize(start, 0);
        }
        if content.len() < end {
            content.resize(end, 0);
        }
        content[start..end].copy_from_slice(data.as_ref());
        self.write(path, Bytes::from(content)).await
    }

    async fn set_times(
        &self,
        path: &str,
        atime: Option<u64>,
        mtime: Option<u64>,
    ) -> VfsResult<VfsFileInfo> {
        let endpoint = self.resolve_endpoint(path)?;
        if endpoint.mount_id.is_none() {
            return endpoint
                .storage
                .set_times(&endpoint.path, atime, mtime)
                .await;
        }
        self.stat(path).await
    }

    async fn check_quota(&self, additional_size: i64) -> VfsResult<()> {
        self.base.check_quota(additional_size).await
    }

    async fn get_quota(&self) -> VfsResult<(u64, Option<u64>)> {
        self.base.get_quota().await
    }

    async fn create_dir(&self, path: &str) -> VfsResult<VfsFileInfo> {
        let endpoint = self.resolve_endpoint(path)?;
        if endpoint.is_mount_root {
            return Err(VfsError::PermissionDenied(
                "Mount root already exists".to_string(),
            ));
        }
        let info = endpoint.storage.create_dir(&endpoint.path).await?;
        if let Some(mount_id) = endpoint.mount_id
            && let Some(runtime) = self
                .mounts
                .iter()
                .find(|item| item.snapshot.model.id == mount_id)
        {
            return Ok(Self::map_remote_info(runtime, info));
        }
        Ok(info)
    }

    async fn create_dir_all(&self, path: &str) -> VfsResult<VfsFileInfo> {
        let endpoint = self.resolve_endpoint(path)?;
        if endpoint.is_mount_root {
            return self.stat(path).await;
        }
        let info = endpoint.storage.create_dir_all(&endpoint.path).await?;
        if let Some(mount_id) = endpoint.mount_id
            && let Some(runtime) = self
                .mounts
                .iter()
                .find(|item| item.snapshot.model.id == mount_id)
        {
            return Ok(Self::map_remote_info(runtime, info));
        }
        Ok(info)
    }

    async fn move_file(&self, src: &str, dst: &str) -> VfsResult<VfsFileInfo> {
        let src_endpoint = self.resolve_endpoint(src)?;
        let dst_endpoint = self.resolve_endpoint(dst)?;
        if src_endpoint.mount_id.is_none() && dst_endpoint.mount_id.is_none() {
            return self.base.move_file(src, dst).await;
        }
        if src_endpoint.mount_id == dst_endpoint.mount_id
            && src_endpoint.mount_id.is_some()
            && !src_endpoint.is_mount_root
            && !dst_endpoint.is_mount_root
        {
            let info = src_endpoint
                .storage
                .move_file(&src_endpoint.path, &dst_endpoint.path)
                .await?;
            if let Some(mount_id) = src_endpoint.mount_id
                && let Some(runtime) = self
                    .mounts
                    .iter()
                    .find(|item| item.snapshot.model.id == mount_id)
            {
                return Ok(Self::map_remote_info(runtime, info));
            }
            return Ok(info);
        }
        self.generic_move_or_copy(src, dst, true).await
    }

    async fn copy_file(&self, src: &str, dst: &str) -> VfsResult<VfsFileInfo> {
        let src_endpoint = self.resolve_endpoint(src)?;
        let dst_endpoint = self.resolve_endpoint(dst)?;
        if src_endpoint.mount_id.is_none() && dst_endpoint.mount_id.is_none() {
            return self.base.copy_file(src, dst).await;
        }
        if src_endpoint.mount_id == dst_endpoint.mount_id
            && src_endpoint.mount_id.is_some()
            && !src_endpoint.is_mount_root
            && !dst_endpoint.is_mount_root
        {
            let info = src_endpoint
                .storage
                .copy_file(&src_endpoint.path, &dst_endpoint.path)
                .await?;
            if let Some(mount_id) = src_endpoint.mount_id
                && let Some(runtime) = self
                    .mounts
                    .iter()
                    .find(|item| item.snapshot.model.id == mount_id)
            {
                return Ok(Self::map_remote_info(runtime, info));
            }
            return Ok(info);
        }
        self.generic_move_or_copy(src, dst, false).await
    }

    async fn canonicalize_path(&self, path: &str) -> VfsResult<String> {
        Self::validate_path(path)
    }

    async fn batch_remove(&self, paths: &[String]) -> VfsResult<VfsBatchResult> {
        let mut result = VfsBatchResult::default();
        for path in paths {
            match self.delete(path).await {
                Ok(_) => result.success.push(path.to_string().into()),
                Err(err) => result.failed.push(VfsBatchError {
                    path: path.to_string().into(),
                    error: err.to_string().into(),
                }),
            }
        }
        Ok(result)
    }

    async fn batch_move(&self, src_paths: &[String], dst_dir: &str) -> VfsResult<VfsBatchResult> {
        let mut result = VfsBatchResult::default();
        for path in src_paths {
            let next_name = file_name(path).to_string();
            let target = join_logical_path(dst_dir, &next_name);
            match self.move_file(path, &target).await {
                Ok(_) => result.success.push(path.to_string().into()),
                Err(err) => result.failed.push(VfsBatchError {
                    path: path.to_string().into(),
                    error: err.to_string().into(),
                }),
            }
        }
        Ok(result)
    }

    async fn compress(
        &self,
        source_path: &str,
        target_path: &str,
        user_id: &str,
        password: Option<&str>,
        encrypt_filenames: bool,
    ) -> VfsResult<VfsFileInfo> {
        let options = crate::utils::CompressionOptions {
            format: crate::utils::CompressionFormat::Zip,
            password: password.map(ToOwned::to_owned),
            encrypt_filenames,
            compression_level: 6,
        };
        crate::utils::compress_task(self, source_path, target_path, user_id, &options, false)
            .await
            .map_err(|err| VfsError::Internal(err.to_string()))
    }

    async fn decompress(
        &self,
        archive_path: &str,
        target_dir: &str,
        user_id: &str,
        overwrite: bool,
        password: Option<&str>,
    ) -> VfsResult<VfsFileInfo> {
        let options = crate::utils::DecompressionOptions {
            overwrite,
            password: password.map(ToOwned::to_owned),
        };
        crate::utils::decompress_task(self, archive_path, target_dir, user_id, &options, false)
            .await
            .map_err(|err| VfsError::Internal(err.to_string()))
    }

    async fn create_temp_file_for_upload(
        &self,
    ) -> VfsResult<(std::path::PathBuf, crate::utils::VfsTempFileGuard)> {
        self.base.create_temp_file_for_upload().await
    }

    fn get_backend_type(&self) -> String {
        "mounted".to_string()
    }

    fn get_recursive_size(&self, path: &str) -> BoxFuture<'static, VfsResult<i64>> {
        let storage = self.clone();
        let path = path.to_owned();
        Box::pin(async move {
            let entries = storage.list_recursive(&path).await?;
            Ok(entries
                .into_iter()
                .filter(|entry| !entry.is_dir)
                .fold(0_i64, |acc, entry| acc.saturating_add(entry.size as i64)))
        })
    }

    async fn set_favorite(&self, path: &str, color: i32) -> VfsResult<VfsFileInfo> {
        let endpoint = self.resolve_endpoint(path)?;
        if endpoint.mount_id.is_some() {
            return if color == 0 {
                self.stat(path).await
            } else {
                Err(VfsError::Internal(
                    "Favorites for mounted remote files are not supported yet".to_string(),
                ))
            };
        }
        self.base.set_favorite(path, color).await
    }

    async fn list_favorites(&self, color_filter: Option<i32>) -> VfsResult<Vec<VfsFileInfo>> {
        self.base.list_favorites(color_filter).await
    }

    async fn list_favorites_paginated(
        &self,
        params: VfsPaginationParams<'_>,
        color_filter: Option<i32>,
    ) -> VfsResult<(Vec<VfsFileInfo>, i64)> {
        self.base
            .list_favorites_paginated(params, color_filter)
            .await
    }

    async fn move_to_trash(&self, path: &str) -> VfsResult<VfsFileInfo> {
        let endpoint = self.resolve_endpoint(path)?;
        if endpoint.mount_id.is_some() {
            return self.delete(path).await;
        }
        self.base.move_to_trash(path).await
    }

    async fn restore_from_trash(&self, path: &str) -> VfsResult<VfsFileInfo> {
        self.base.restore_from_trash(path).await
    }

    async fn list_trash(&self) -> VfsResult<Vec<VfsFileInfo>> {
        self.base.list_trash().await
    }

    async fn sync_index(&self, path: &str) -> VfsResult<Vec<VfsFileInfo>> {
        if self.resolve_runtime(path).is_some() {
            return self.list(path).await;
        }
        self.base.sync_index(path).await
    }

    async fn migrate_storage(&self, path: &str, target_storage_id: &str) -> VfsResult<VfsFileInfo> {
        self.base.migrate_storage(path, target_storage_id).await
    }

    async fn submit_batch_move(
        &self,
        src_paths: Vec<String>,
        dst_dir: String,
    ) -> VfsResult<String> {
        if !src_paths
            .iter()
            .any(|path| self.contains_mounted_path(path))
            && !self.contains_mounted_path(&dst_dir)
        {
            return self.base.submit_batch_move(src_paths, dst_dir).await;
        }
        let task_handler = self
            .task_handler
            .as_ref()
            .ok_or_else(|| VfsError::Internal("Task handler not configured".to_string()))?
            .clone();
        let payload = serde_json::json!({ "src_paths": src_paths, "dst_dir": dst_dir });
        let task_id = task_handler
            .create_task(self.user_id.as_ref(), "batch_move", payload)
            .await
            .map_err(VfsError::Internal)?;
        let storage: Arc<dyn VfsStorage> = Arc::new(self.clone());
        let handler = Arc::clone(&task_handler);
        let user_id = self.user_id.to_string();
        let timeout = crate::config::get_vfs_hub_config()
            .await
            .get_batch_operation()
            .get_timeout_secs();
        Self::spawn_batch_task("mounted_batch_move", async move {
            let _permit = match Self::acquire_batch_task_permit().await {
                Ok(permit) => permit,
                Err(err) => {
                    let _ = handler.fail_task(task_id, &err.to_string()).await;
                    handler.cleanup_task(task_id);
                    return;
                }
            };
            crate::vfs::batch::VfsBatchExecutor::execute_move(
                storage,
                Arc::clone(&handler),
                task_id,
                src_paths,
                dst_dir,
                timeout,
                &user_id,
            )
            .await;
            handler.cleanup_task(task_id);
        });
        Ok(task_id.to_string())
    }

    async fn submit_batch_copy(
        &self,
        src_paths: Vec<String>,
        dst_dir: String,
    ) -> VfsResult<String> {
        if !src_paths
            .iter()
            .any(|path| self.contains_mounted_path(path))
            && !self.contains_mounted_path(&dst_dir)
        {
            return self.base.submit_batch_copy(src_paths, dst_dir).await;
        }
        let task_handler = self
            .task_handler
            .as_ref()
            .ok_or_else(|| VfsError::Internal("Task handler not configured".to_string()))?
            .clone();
        let payload = serde_json::json!({ "src_paths": src_paths, "dst_dir": dst_dir });
        let task_id = task_handler
            .create_task(self.user_id.as_ref(), "batch_copy", payload)
            .await
            .map_err(VfsError::Internal)?;
        let storage: Arc<dyn VfsStorage> = Arc::new(self.clone());
        let handler = Arc::clone(&task_handler);
        let user_id = self.user_id.to_string();
        let timeout = crate::config::get_vfs_hub_config()
            .await
            .get_batch_operation()
            .get_timeout_secs();
        Self::spawn_batch_task("mounted_batch_copy", async move {
            let _permit = match Self::acquire_batch_task_permit().await {
                Ok(permit) => permit,
                Err(err) => {
                    let _ = handler.fail_task(task_id, &err.to_string()).await;
                    handler.cleanup_task(task_id);
                    return;
                }
            };
            crate::vfs::batch::VfsBatchExecutor::execute_copy(
                storage,
                Arc::clone(&handler),
                task_id,
                src_paths,
                dst_dir,
                timeout,
                &user_id,
            )
            .await;
            handler.cleanup_task(task_id);
        });
        Ok(task_id.to_string())
    }

    async fn submit_batch_delete(&self, paths: Vec<String>) -> VfsResult<String> {
        if !paths.iter().any(|path| self.contains_mounted_path(path)) {
            return self.base.submit_batch_delete(paths).await;
        }
        let task_handler = self
            .task_handler
            .as_ref()
            .ok_or_else(|| VfsError::Internal("Task handler not configured".to_string()))?
            .clone();
        let payload = serde_json::json!({ "paths": paths });
        let task_id = task_handler
            .create_task(self.user_id.as_ref(), "batch_delete", payload)
            .await
            .map_err(VfsError::Internal)?;
        let storage: Arc<dyn VfsStorage> = Arc::new(self.clone());
        let handler = Arc::clone(&task_handler);
        let user_id = self.user_id.to_string();
        let timeout = crate::config::get_vfs_hub_config()
            .await
            .get_batch_operation()
            .get_timeout_secs();
        Self::spawn_batch_task("mounted_batch_delete", async move {
            let _permit = match Self::acquire_batch_task_permit().await {
                Ok(permit) => permit,
                Err(err) => {
                    let _ = handler.fail_task(task_id, &err.to_string()).await;
                    handler.cleanup_task(task_id);
                    return;
                }
            };
            crate::vfs::batch::VfsBatchExecutor::execute_delete(
                storage,
                Arc::clone(&handler),
                task_id,
                paths,
                timeout,
                &user_id,
            )
            .await;
            handler.cleanup_task(task_id);
        });
        Ok(task_id.to_string())
    }

    async fn submit_batch_compress(
        &self,
        paths: Vec<String>,
        archive_name: String,
        options: crate::utils::CompressionOptions,
        delete_source: bool,
    ) -> VfsResult<String> {
        if !paths.iter().any(|path| self.contains_mounted_path(path))
            && !self.contains_mounted_path(&archive_name)
        {
            return self
                .base
                .submit_batch_compress(paths, archive_name, options, delete_source)
                .await;
        }
        let task_handler = self
            .task_handler
            .as_ref()
            .ok_or_else(|| VfsError::Internal("Task handler not configured".to_string()))?
            .clone();
        let payload = serde_json::json!({
            "paths": paths,
            "archive_name": archive_name,
            "options": options,
            "delete_source": delete_source
        });
        let task_id = task_handler
            .create_task(self.user_id.as_ref(), "compress", payload)
            .await
            .map_err(VfsError::Internal)?;
        let storage: Arc<dyn VfsStorage> = Arc::new(self.clone());
        let handler = Arc::clone(&task_handler);
        let user_id = self.user_id.to_string();
        let timeout = crate::config::get_vfs_hub_config()
            .await
            .get_batch_operation()
            .get_timeout_secs();
        Self::spawn_batch_task("mounted_batch_compress", async move {
            let _permit = match Self::acquire_batch_task_permit().await {
                Ok(permit) => permit,
                Err(err) => {
                    let _ = handler.fail_task(task_id, &err.to_string()).await;
                    handler.cleanup_task(task_id);
                    return;
                }
            };
            let task_future = async {
                let timestamp = chrono::Utc::now().timestamp();
                let temp_dir = format!("/.virtual/tmp/batch_{}_{}", timestamp, task_id);
                if let Err(err) = storage.create_dir_all(&temp_dir).await {
                    return Err(format!("Failed to create temp dir: {}", err));
                }
                let mut copied_count = 0usize;
                let total_files = paths.len();
                for (idx, path) in paths.iter().enumerate() {
                    let Some(name) = std::path::Path::new(path).file_name() else {
                        continue;
                    };
                    let target = format!("{}/{}", temp_dir, name.to_string_lossy());
                    if storage.copy_file(path, &target).await.is_ok() {
                        copied_count += 1;
                    }
                    let progress = ((idx + 1) as f32 / total_files.max(1) as f32 * 30.0) as i32;
                    let _ = handler
                        .update_progress(task_id, progress, Some("preparing"))
                        .await;
                }
                if copied_count == 0 {
                    let _ = storage.delete(&temp_dir).await;
                    return Err("No files were successfully prepared for compression".to_string());
                }
                let _ = handler
                    .update_progress(task_id, 35, Some("compressing"))
                    .await;
                match crate::utils::compress_task(
                    storage.as_ref(),
                    &temp_dir,
                    &archive_name,
                    &user_id,
                    &options,
                    delete_source,
                )
                .await
                {
                    Ok(_) => {
                        let _ = storage.delete(&temp_dir).await;
                        if delete_source {
                            for path in &paths {
                                let _ = storage.delete(path).await;
                            }
                        }
                        Ok(())
                    }
                    Err(err) => {
                        let _ = storage.delete(&temp_dir).await;
                        Err(format!("Compression failed: {}", err))
                    }
                }
            };
            match tokio::time::timeout(std::time::Duration::from_secs(timeout), task_future).await {
                Ok(Ok(())) => {
                    let _ = handler.success_task(task_id).await;
                }
                Ok(Err(err)) => {
                    let _ = handler.fail_task(task_id, &err).await;
                }
                Err(_) => {
                    let _ = handler
                        .fail_task(task_id, "Compression task timed out after 24 hours")
                        .await;
                }
            }
            handler.cleanup_task(task_id);
        });
        Ok(task_id.to_string())
    }

    async fn submit_batch_decompress(
        &self,
        paths: Vec<String>,
        output_dir: String,
        options: crate::utils::DecompressionOptions,
        delete_archive: bool,
    ) -> VfsResult<String> {
        if !paths.iter().any(|path| self.contains_mounted_path(path))
            && !self.contains_mounted_path(&output_dir)
        {
            return self
                .base
                .submit_batch_decompress(paths, output_dir, options, delete_archive)
                .await;
        }
        let task_handler = self
            .task_handler
            .as_ref()
            .ok_or_else(|| VfsError::Internal("Task handler not configured".to_string()))?
            .clone();
        let payload = serde_json::json!({
            "paths": paths,
            "output_dir": output_dir,
            "delete_archive": delete_archive
        });
        let task_id = task_handler
            .create_task(self.user_id.as_ref(), "decompress", payload)
            .await
            .map_err(VfsError::Internal)?;
        let storage: Arc<dyn VfsStorage> = Arc::new(self.clone());
        let handler = Arc::clone(&task_handler);
        let user_id = self.user_id.to_string();
        let timeout = crate::config::get_vfs_hub_config()
            .await
            .get_batch_operation()
            .get_timeout_secs();
        Self::spawn_batch_task("mounted_batch_decompress", async move {
            let _permit = match Self::acquire_batch_task_permit().await {
                Ok(permit) => permit,
                Err(err) => {
                    let _ = handler.fail_task(task_id, &err.to_string()).await;
                    handler.cleanup_task(task_id);
                    return;
                }
            };
            let task_future = async {
                let total = paths.len();
                let mut failed_count = 0usize;
                for (idx, path) in paths.iter().enumerate() {
                    match crate::utils::decompress_task(
                        storage.as_ref(),
                        path,
                        &output_dir,
                        &user_id,
                        &options,
                        delete_archive,
                    )
                    .await
                    {
                        Ok(_) => {}
                        Err(err) => {
                            failed_count += 1;
                            yh_console_log::yhlog(
                                "error",
                                &format!(
                                    "Decompress task {} failed for file {}: {}",
                                    task_id, path, err
                                ),
                            );
                        }
                    }
                    let progress = ((idx + 1) as f32 / total.max(1) as f32 * 100.0) as i32;
                    let message =
                        format!("Processed {}/{} (Failed: {})", idx + 1, total, failed_count);
                    let _ = handler
                        .update_task(task_id, progress, Some("running"), Some(&message))
                        .await;
                }
                failed_count
            };
            match tokio::time::timeout(std::time::Duration::from_secs(timeout), task_future).await {
                Ok(0) => {
                    let _ = handler.success_task(task_id).await;
                }
                Ok(count) => {
                    let _ = handler
                        .fail_task(
                            task_id,
                            &format!("Decompression completed with {} failures", count),
                        )
                        .await;
                }
                Err(_) => {
                    let _ = handler
                        .fail_task(task_id, "Decompression task timed out after 24 hours")
                        .await;
                }
            }
            handler.cleanup_task(task_id);
        });
        Ok(task_id.to_string())
    }
}

pub async fn build_remote_storage(
    snapshot: &RemoteMountSnapshot,
) -> VfsResult<Arc<dyn VfsStorage>> {
    let connector = VfsConnectorConfig {
        name: Some(format!("remote-mount-{}", snapshot.model.id).into()),
        driver: Some(snapshot.model.driver.clone().into()),
        root: Some(snapshot.model.root.clone().into()),
        enable: Some(true),
        options: Some(snapshot.options.clone()),
    };
    let operator = build_operator(&connector).await?;
    let pool = VfsPool::new(
        operator,
        None,
        VfsPoolConfig {
            name: Some(format!("remote-mount-pool-{}", snapshot.model.id).into()),
            primary_connector: Some("runtime".into()),
            backup_connector: None,
            enable_write_cache: Some(false),
            enable: Some(true),
            options: Some(HashMap::new()),
        },
        None,
        None,
    );
    Ok(Arc::new(pool) as Arc<dyn VfsStorage>)
}

pub async fn build_user_storage_with_mounts(
    db: Arc<DatabaseConnection>,
    storage_hub: Arc<VfsStorageHub>,
    user_id: &str,
    role_id: &str,
    journal_recorder: Option<Arc<dyn crate::vfs::VfsJournalRecorder>>,
) -> VfsResult<Arc<dyn VfsStorage>> {
    let base_engine = storage_hub
        .create_scoped_engine(Arc::clone(&db), user_id, role_id, journal_recorder)
        .await?;
    let base_storage: Arc<dyn VfsStorage> = base_engine;
    let mounts = RemoteMountService::list_user_mounts(db.as_ref(), user_id)
        .await
        .map_err(|err| VfsError::Internal(err.to_string()))?;
    let mut runtimes = Vec::new();
    for snapshot in mounts.into_iter().filter(|item| item.model.enable) {
        match build_remote_storage(&snapshot).await {
            Ok(storage) => runtimes.push(MountRuntime { snapshot, storage }),
            Err(error) => {
                yh_console_log::yhlog(
                    "warn",
                    &format!(
                        "Skip remote mount '{}' for user '{}': {}",
                        snapshot.model.id, user_id, error
                    ),
                );
            }
        }
    }
    if runtimes.is_empty() {
        return Ok(base_storage);
    }
    Ok(Arc::new(MountedUserStorage::new(
        base_storage,
        runtimes,
        Arc::from(user_id.to_string()),
        storage_hub.task_handler.as_ref().map(Arc::clone),
    )) as Arc<dyn VfsStorage>)
}

fn mount_lock(mount_id: &str) -> Arc<AsyncMutex<()>> {
    let mut map = MOUNT_SYNC_LOCKS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    Arc::clone(
        map.entry(mount_id.to_string())
            .or_insert_with(|| Arc::new(AsyncMutex::new(()))),
    )
}

fn path_depth(path: &str) -> usize {
    path.split('/')
        .filter(|segment| !segment.is_empty())
        .count()
}

fn relative_path(root: &str, path: &str) -> String {
    if root == "/" {
        path.trim_start_matches('/').to_string()
    } else {
        path.strip_prefix(root)
            .unwrap_or(path)
            .trim_start_matches('/')
            .to_string()
    }
}

fn join_root(root: &str, child: &str) -> String {
    if child.is_empty() {
        return root.to_string();
    }
    if root == "/" {
        format!("/{}", child.trim_start_matches('/'))
    } else {
        format!(
            "{}/{}",
            root.trim_end_matches('/'),
            child.trim_start_matches('/')
        )
    }
}

fn compare_modified(
    left: Option<chrono::DateTime<chrono::Utc>>,
    right: Option<chrono::DateTime<chrono::Utc>>,
) -> std::cmp::Ordering {
    match (left, right) {
        (Some(l), Some(r)) => l.cmp(&r),
        (Some(_), None) => std::cmp::Ordering::Greater,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

fn debug_tree_entries(tree: &HashMap<String, VfsFileInfo>) -> Vec<String> {
    let mut entries: Vec<String> = tree
        .iter()
        .map(|(relative, info)| {
            let kind = if info.is_dir { "dir" } else { "file" };
            format!("{} => {} [{}]", relative, info.path, kind)
        })
        .collect();
    entries.sort();
    entries
}

async fn ensure_directory_exists(storage: &Arc<dyn VfsStorage>, path: &str) -> VfsResult<()> {
    if path != "/" && !storage.exists(path).await? {
        let _ = storage.create_dir_all(path).await?;
    }
    Ok(())
}

async fn collect_tree(
    storage: &Arc<dyn VfsStorage>,
    root: &str,
) -> VfsResult<HashMap<String, VfsFileInfo>> {
    if root != "/" && !storage.exists(root).await? {
        return Ok(HashMap::new());
    }
    let entries = storage.list_recursive(root).await?;
    Ok(entries
        .into_iter()
        .map(|entry| (relative_path(root, entry.path.as_ref()), entry))
        .collect())
}

async fn copy_file_between(
    src_storage: &Arc<dyn VfsStorage>,
    src_path: &str,
    dst_storage: &Arc<dyn VfsStorage>,
    dst_path: &str,
) -> VfsResult<()> {
    let parent = Path::new(dst_path)
        .parent()
        .map(|value| value.to_string_lossy().to_string())
        .unwrap_or_else(|| "/".to_string());
    ensure_directory_exists(dst_storage, &parent).await?;
    let (stream, _info) = src_storage.read_stream(src_path).await?;
    let _ = dst_storage.write_stream(dst_path, Box::pin(stream)).await?;
    Ok(())
}

async fn delete_extra_paths(
    dst_storage: &Arc<dyn VfsStorage>,
    dst_root: &str,
    source_map: &HashMap<String, VfsFileInfo>,
    dst_map: &HashMap<String, VfsFileInfo>,
) -> VfsResult<()> {
    let dst_root_normalized = dst_root.trim_end_matches('/');
    let dst_root_depth = path_depth(dst_root_normalized);
    let mut extra_paths: Vec<String> = dst_map
        .keys()
        .filter_map(|key| {
            if key.is_empty() || *key == "." || *key == "./" || source_map.contains_key(key) {
                return None;
            }
            let joined = join_root(dst_root, key);
            let joined_normalized = joined.trim_end_matches('/');
            if joined_normalized == dst_root_normalized
                || joined_normalized == format!("{}/.", dst_root_normalized)
            {
                return None;
            }
            if path_depth(joined_normalized) <= dst_root_depth {
                return None;
            }
            Some(joined)
        })
        .collect();
    extra_paths.sort_by_key(|path| std::cmp::Reverse(path_depth(path)));
    for path in extra_paths {
        delete_path_recursive(Arc::clone(dst_storage), path.clone())
            .await
            .map_err(|error| {
                VfsError::Internal(format!(
                    "Failed to delete mirror extra path '{}': {}",
                    path, error
                ))
            })?;
    }
    Ok(())
}

async fn sync_unidirectional(
    src_storage: &Arc<dyn VfsStorage>,
    src_root: &str,
    dst_storage: &Arc<dyn VfsStorage>,
    dst_root: &str,
    delete_extras: bool,
) -> VfsResult<()> {
    if src_root != "/" && !src_storage.exists(src_root).await? {
        return Err(VfsError::NotFound(format!(
            "Source directory '{}' does not exist",
            src_root
        )));
    }
    ensure_directory_exists(dst_storage, dst_root).await?;
    let source_map = collect_tree(src_storage, src_root).await?;
    let dst_map = collect_tree(dst_storage, dst_root).await?;
    let source_debug = debug_tree_entries(&source_map);
    let dst_debug = debug_tree_entries(&dst_map);

    let mut dir_entries: Vec<_> = source_map
        .iter()
        .filter(|(_, info)| info.is_dir)
        .map(|(relative, _)| relative.clone())
        .collect();
    dir_entries.sort_by_key(|path| path_depth(path));
    for relative in dir_entries {
        dst_storage
            .create_dir_all(&join_root(dst_root, &relative))
            .await
            .map_err(|error| {
                VfsError::Internal(format!(
                    "Mirror sync create_dir failed src_root='{}' dst_root='{}' relative='{}' src={:?} dst={:?}: {}",
                    src_root, dst_root, relative, source_debug, dst_debug, error
                ))
            })?;
    }

    let mut file_entries: Vec<_> = source_map.iter().filter(|(_, info)| !info.is_dir).collect();
    file_entries.sort_by_key(|(relative, _)| path_depth(relative));
    for (relative, src_info) in file_entries {
        let dst_info = dst_map.get(relative.as_str());
        let should_copy = match dst_info {
            None => true,
            Some(info) if info.is_dir => true,
            Some(info) => {
                compare_modified(src_info.modified, info.modified) == std::cmp::Ordering::Greater
                    || src_info.size != info.size
            }
        };
        if should_copy {
            copy_file_between(
                src_storage,
                src_info.path.as_ref(),
                dst_storage,
                &join_root(dst_root, relative),
            )
            .await
            .map_err(|error| {
                VfsError::Internal(format!(
                    "Mirror sync copy_file failed src_root='{}' dst_root='{}' relative='{}' src={:?} dst={:?}: {}",
                    src_root, dst_root, relative, source_debug, dst_debug, error
                ))
            })?;
        }
    }

    if delete_extras {
        delete_extra_paths(dst_storage, dst_root, &source_map, &dst_map)
            .await
            .map_err(|error| {
                VfsError::Internal(format!(
                    "Mirror sync delete_extras failed src_root='{}' dst_root='{}' src={:?} dst={:?}: {}",
                    src_root, dst_root, source_debug, dst_debug, error
                ))
            })?;
    }
    Ok(())
}

async fn sync_bidirectional_keep(
    left_storage: &Arc<dyn VfsStorage>,
    left_root: &str,
    right_storage: &Arc<dyn VfsStorage>,
    right_root: &str,
) -> VfsResult<()> {
    let left_exists = left_storage.exists(left_root).await?;
    let right_exists = right_storage.exists(right_root).await?;
    if left_exists {
        ensure_directory_exists(right_storage, right_root).await?;
    }
    if right_exists {
        ensure_directory_exists(left_storage, left_root).await?;
    }
    let left_map = collect_tree(left_storage, left_root).await?;
    let right_map = collect_tree(right_storage, right_root).await?;
    let mut all_paths = BTreeSet::new();
    all_paths.extend(left_map.keys().cloned());
    all_paths.extend(right_map.keys().cloned());

    for relative in &all_paths {
        let left_info = left_map.get(relative.as_str());
        let right_info = right_map.get(relative.as_str());
        match (left_info, right_info) {
            (Some(info), None) if info.is_dir => {
                let _ = right_storage
                    .create_dir_all(&join_root(right_root, relative))
                    .await?;
            }
            (None, Some(info)) if info.is_dir => {
                let _ = left_storage
                    .create_dir_all(&join_root(left_root, relative))
                    .await?;
            }
            (Some(info), None) => {
                copy_file_between(
                    left_storage,
                    info.path.as_ref(),
                    right_storage,
                    &join_root(right_root, relative),
                )
                .await?;
            }
            (None, Some(info)) => {
                copy_file_between(
                    right_storage,
                    info.path.as_ref(),
                    left_storage,
                    &join_root(left_root, relative),
                )
                .await?;
            }
            (Some(left_info), Some(right_info)) if left_info.is_dir && right_info.is_dir => {}
            (Some(left_info), Some(right_info)) if left_info.is_dir || right_info.is_dir => {
                return Err(VfsError::Internal(format!(
                    "Path '{}' conflicts between file and directory across sync targets",
                    relative
                )));
            }
            (Some(left_info), Some(right_info)) => {
                match compare_modified(left_info.modified, right_info.modified) {
                    std::cmp::Ordering::Greater => {
                        copy_file_between(
                            left_storage,
                            left_info.path.as_ref(),
                            right_storage,
                            &join_root(right_root, relative),
                        )
                        .await?;
                    }
                    std::cmp::Ordering::Less => {
                        copy_file_between(
                            right_storage,
                            right_info.path.as_ref(),
                            left_storage,
                            &join_root(left_root, relative),
                        )
                        .await?;
                    }
                    std::cmp::Ordering::Equal => {
                        if left_info.size != right_info.size {
                            copy_file_between(
                                left_storage,
                                left_info.path.as_ref(),
                                right_storage,
                                &join_root(right_root, relative),
                            )
                            .await?;
                        }
                    }
                }
            }
            (None, None) => {}
        }
    }
    Ok(())
}

async fn execute_mount_sync(
    mount: &RemoteMountSnapshot,
    local_storage: Arc<dyn VfsStorage>,
    remote_storage: Arc<dyn VfsStorage>,
    effective_interval_minutes: i64,
    effective_timeout_secs: i64,
) -> VfsResult<Option<chrono::DateTime<chrono::Utc>>> {
    let peer_dir =
        mount.model.sync_peer_dir.as_deref().ok_or_else(|| {
            VfsError::Internal("Mount sync peer directory is missing".to_string())
        })?;
    let sync_mode = RemoteMountSyncMode::from_i16(mount.model.sync_mode).ok_or_else(|| {
        VfsError::Internal(format!("Unsupported sync mode: {}", mount.model.sync_mode))
    })?;

    let sync_future = async {
        match sync_mode {
            RemoteMountSyncMode::PeerToMountKeep => {
                sync_unidirectional(&local_storage, peer_dir, &remote_storage, "/", false).await
            }
            RemoteMountSyncMode::PeerToMountMirror => {
                sync_unidirectional(&local_storage, peer_dir, &remote_storage, "/", true).await
            }
            RemoteMountSyncMode::MountToPeerKeep => {
                sync_unidirectional(&remote_storage, "/", &local_storage, peer_dir, false).await
            }
            RemoteMountSyncMode::MountToPeerMirror => {
                sync_unidirectional(&remote_storage, "/", &local_storage, peer_dir, true).await
            }
            RemoteMountSyncMode::BidirectionalKeep => {
                sync_bidirectional_keep(&remote_storage, "/", &local_storage, peer_dir).await
            }
        }
    };

    tokio::time::timeout(
        std::time::Duration::from_secs(effective_timeout_secs.max(1) as u64),
        sync_future,
    )
    .await
    .map_err(|_| {
        VfsError::Internal(format!(
            "Mount sync timed out after {} seconds",
            effective_timeout_secs
        ))
    })??;

    Ok(Some(
        Utc::now() + ChronoDuration::minutes(effective_interval_minutes.max(1)),
    ))
}

pub async fn sync_remote_mount_once(
    db: Arc<DatabaseConnection>,
    storage_hub: Arc<VfsStorageHub>,
    user_id: &str,
    role_id: &str,
    mount_id: &str,
) -> VfsResult<RemoteMountSnapshot> {
    let lock = mount_lock(mount_id);
    let _guard = lock.lock().await;

    let mount = RemoteMountService::get_user_mount(db.as_ref(), user_id, mount_id)
        .await
        .map_err(|err| VfsError::Internal(err.to_string()))?
        .ok_or_else(|| VfsError::NotFound(format!("Mount not found: {}", mount_id)))?;

    RemoteMountService::mark_sync_running(db.as_ref(), mount_id)
        .await
        .map_err(|err| VfsError::Internal(err.to_string()))?;

    let local_engine = storage_hub
        .create_scoped_engine(Arc::clone(&db), user_id, role_id, None)
        .await?;
    let local_storage: Arc<dyn VfsStorage> = local_engine;
    let remote_storage = build_remote_storage(&mount).await?;

    let result = execute_mount_sync(
        &mount,
        local_storage,
        remote_storage,
        mount.model.sync_interval_minutes,
        mount.model.sync_timeout_secs,
    )
    .await;

    match result {
        Ok(next_sync_at) => {
            RemoteMountService::mark_sync_finished(
                db.as_ref(),
                mount_id,
                "success",
                None,
                next_sync_at,
            )
            .await
            .map_err(|err| VfsError::Internal(err.to_string()))?;
        }
        Err(error) => {
            RemoteMountService::mark_sync_finished(
                db.as_ref(),
                mount_id,
                "failed",
                Some(error.to_string()),
                Some(
                    Utc::now() + ChronoDuration::minutes(mount.model.sync_interval_minutes.max(1)),
                ),
            )
            .await
            .map_err(|err| VfsError::Internal(err.to_string()))?;
            return Err(error);
        }
    }

    RemoteMountService::get_user_mount(db.as_ref(), user_id, mount_id)
        .await
        .map_err(|err| VfsError::Internal(err.to_string()))?
        .ok_or_else(|| VfsError::NotFound(format!("Mount not found after sync: {}", mount_id)))
}
