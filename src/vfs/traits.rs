// VFS Storage Interface Definition
use crate::vfs::VfsResult;
use crate::vfs::types::{VfsBatchResult, VfsFileInfo, VfsMetadata, VfsPaginationParams};
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use futures::stream::BoxStream;
use std::cmp::Ordering;
use std::pin::Pin;

fn cmp_optional_utc(a: Option<chrono::DateTime<chrono::Utc>>, b: Option<chrono::DateTime<chrono::Utc>>) -> Ordering {
    match (a, b) {
        (Some(left), Some(right)) => left.cmp(&right),
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}
/// Unified VFS Storage Interface
///
/// VfsFileInfo
/// This trait defines standard behaviors for all storage operations. All write operations
/// must return the latest VfsFileInfo to ensure index consistency.
#[async_trait]
pub trait VfsStorage: Send + Sync {
    // Basic IO Operations ===
    /// Read file content and return its latest metadata.
    ///(, )
    /// Returning a tuple (data, metadata) avoids redundant queries for size or modification time.
    async fn read(&self, path: &str) -> VfsResult<(Bytes, VfsFileInfo)>;
    /// Write file and return its latest metadata.
    /// Write-Through
    /// Heart of the Write-Through strategy: returns metadata immediately after physical write for DB index update.
    async fn write(&self, path: &str, data: Bytes) -> VfsResult<VfsFileInfo>;
    /// Physically delete and return the last metadata of the deleted item.
    async fn delete(&self, path: &str) -> VfsResult<VfsFileInfo>;
    /// List file entries in a directory.
    async fn list(&self, path: &str) -> VfsResult<Vec<VfsFileInfo>>;
    /// Stream file entries in a directory.
    fn list_stream(&self, path: &str) -> BoxStream<'static, VfsResult<VfsFileInfo>>;
    /// Recursively list all file entries.
    async fn list_recursive(&self, path: &str) -> VfsResult<Vec<VfsFileInfo>>;
    /// Paginated list of files in directory (with total count)
    async fn list_files_paginated(&self, parent_path: &str, page: i64, page_size: i64) -> VfsResult<(Vec<VfsFileInfo>, i64)> {
        //()
        let all = self.list(parent_path).await?;
        let total = all.len() as i64;
        let offset = ((page - 1) * page_size) as usize;
        let files = all.into_iter().skip(offset).take(page_size as usize).collect();
        Ok((files, total))
    }
    /// Paginated list of files in directory (with total, sorting and search)
    #[allow(clippy::manual_unwrap_or)]
    async fn list_files_paginated_with_sort(&self, parent_path: &str, params: VfsPaginationParams<'_>) -> VfsResult<(Vec<VfsFileInfo>, i64)> {
        //()
        let mut all = self.list(parent_path).await?;
        // Search filter
        if let Some(kw) = params.keyword
            && !kw.is_empty()
        {
            let kw_lower = kw.to_lowercase();
            all.retain(|e| e.name.to_lowercase().contains(&kw_lower) || e.path.to_lowercase().contains(&kw_lower));
        }
        // Sorting
        if let Some(field) = params.sort_by {
            let order = match params.order {
                Some(order) => order,
                None => "asc",
            };
            let is_desc = order == "desc";
            all.sort_by(|a, b| {
                let cmp = match field {
                    "name" => a.name.cmp(&b.name),
                    "size" => a.size.cmp(&b.size),
                    "modified" => a.modified.cmp(&b.modified),
                    _ => a.name.cmp(&b.name),
                };
                if is_desc { cmp.reverse() } else { cmp }
            });
        }
        let total = all.len() as i64;
        let offset = ((params.page - 1) * params.page_size) as usize;
        let files = all.into_iter().skip(offset).take(params.page_size as usize).collect();
        Ok((files, total))
    }
    /// Paginated search files (with total count)
    async fn search_files_paginated(&self, keyword: &str, page: i64, page_size: i64) -> VfsResult<(Vec<VfsFileInfo>, i64)> {
        //()
        let all = self.list_recursive("/").await?;
        let keyword_lower = keyword.to_lowercase();
        let filtered: Vec<VfsFileInfo> = all.into_iter().filter(|e| e.name.to_lowercase().contains(&keyword_lower)).collect();
        let total = filtered.len() as i64;
        let offset = ((page - 1) * page_size) as usize;
        let files = filtered.into_iter().skip(offset).take(page_size as usize).collect();
        Ok((files, total))
    }
    /// Paginated list of trash files (with total count)
    async fn list_recycle_bin_paginated(&self, page: i64, page_size: i64) -> VfsResult<(Vec<VfsFileInfo>, i64)> {
        //()
        let all = self.list_trash().await?;
        let total = all.len() as i64;
        let offset = ((page - 1) * page_size) as usize;
        let files = all.into_iter().skip(offset).take(page_size as usize).collect();
        Ok((files, total))
    }
    /// Paginated list of trash files (with total, sorting and search)
    #[allow(clippy::manual_unwrap_or)]
    async fn list_recycle_bin_paginated_with_sort(&self, params: VfsPaginationParams<'_>) -> VfsResult<(Vec<VfsFileInfo>, i64)> {
        //()
        let mut all = self.list_trash().await?;
        // Search filter
        if let Some(kw) = params.keyword
            && !kw.is_empty()
        {
            let kw_lower = kw.to_lowercase();
            all.retain(|e| e.name.to_lowercase().contains(&kw_lower) || e.path.to_lowercase().contains(&kw_lower) || e.original_path.as_ref().is_some_and(|p| p.to_lowercase().contains(&kw_lower)));
        }
        // Sorting
        if let Some(field) = params.sort_by {
            let order = match params.order {
                Some(order) => order,
                None => "asc",
            };
            let is_desc = order == "desc";
            all.sort_by(|a, b| {
                let cmp = match field {
                    "name" => a.name.cmp(&b.name),
                    "path" => a.path.cmp(&b.path),
                    "original_path" => {
                        a.original_path.as_deref().cmp(&b.original_path.as_deref())
                    }
                    "size" => a.size.cmp(&b.size),
                    "modified" => a.modified.cmp(&b.modified),
                    "trashed_at" => {
                        cmp_optional_utc(a.trashed_at, b.trashed_at)
                    }
                    _ => cmp_optional_utc(a.trashed_at, b.trashed_at),
                };
                if is_desc { cmp.reverse() } else { cmp }
            });
        }
        let total = all.len() as i64;
        let offset = ((params.page - 1) * params.page_size) as usize;
        let files = all.into_iter().skip(offset).take(params.page_size as usize).collect();
        Ok((files, total))
    }
    /// Check if path exists.
    async fn exists(&self, path: &str) -> VfsResult<bool>;
    /// Get metadata (always hits or self-heals the index).
    async fn stat(&self, path: &str) -> VfsResult<VfsFileInfo>;
    // Streaming Operations ===
    /// Read stream and return metadata.
    async fn read_stream(&self, path: &str) -> VfsResult<(Pin<Box<dyn Stream<Item = VfsResult<Bytes>> + Send + Sync>>, VfsFileInfo)>;
    /// Read stream in specified range and return metadata.
    async fn read_stream_range(&self, path: &str, range: std::ops::Range<u64>) -> VfsResult<(Pin<Box<dyn Stream<Item = VfsResult<Bytes>> + Send + Sync>>, VfsFileInfo)>;
    /// Write stream and return metadata.
    async fn write_stream(&self, path: &str, stream: BoxStream<'static, VfsResult<Bytes>>) -> VfsResult<VfsFileInfo>;
    // Metadata and Attributes ===
    async fn metadata(&self, path: &str) -> VfsResult<VfsMetadata>;
    /// Read specified range.
    async fn read_range(&self, path: &str, start: u64, end: u64) -> VfsResult<(Bytes, VfsFileInfo)>;
    /// Write content at specified offset.
    async fn write_at(&self, path: &str, offset: u64, data: Bytes) -> VfsResult<VfsFileInfo>;
    /// Update timestamps and return metadata.
    async fn set_times(&self, path: &str, atime: Option<u64>, mtime: Option<u64>) -> VfsResult<VfsFileInfo>;
    /// Check if quota is sufficient.
    async fn check_quota(&self, additional_size: i64) -> VfsResult<()>;
    /// Get quota info for current user (used, total)
    async fn get_quota(&self) -> VfsResult<(u64, Option<u64>)>;
    // Directory and Path Operations ===
    async fn create_dir(&self, path: &str) -> VfsResult<VfsFileInfo>;
    async fn create_dir_all(&self, path: &str) -> VfsResult<VfsFileInfo>;
    async fn move_file(&self, src: &str, dst: &str) -> VfsResult<VfsFileInfo>;
    async fn copy_file(&self, src: &str, dst: &str) -> VfsResult<VfsFileInfo>;
    async fn canonicalize_path(&self, path: &str) -> VfsResult<String>;
    /// Get unique path to avoid collisions
    async fn get_unique_path(&self, path: &str) -> VfsResult<String> {
        let mut current_path = path.to_string();
        if !self.exists(&current_path).await? {
            return Ok(current_path);
        }
        let p = std::path::Path::new(path);
        let parent = if let Some(parent) = p.parent() { parent } else { std::path::Path::new("/") }.to_string_lossy().into_owned();
        let stem = if let Some(stem) = p.file_stem().and_then(|s| s.to_str()) {
            stem.to_string()
        } else {
            "file".to_string()
        };
        let extension = if let Some(ext) = p.extension().and_then(|e| e.to_str()) {
            format!(".{}", ext)
        } else {
            String::new()
        };
        let mut counter = 1;
        while self.exists(&current_path).await? {
            let new_name = format!("{}_{}{}", stem, counter, extension);
            current_path = format!("{}/{}", parent.trim_end_matches('/'), new_name);
            counter += 1;
            if counter > 100 {
                break;
            }
        }
        Ok(current_path)
    }
    // Batch Operations ===
    async fn batch_remove(&self, paths: &[String]) -> VfsResult<VfsBatchResult>;
    async fn batch_move(&self, src_paths: &[String], dst_dir: &str) -> VfsResult<VfsBatchResult>;
    // Compression and Decompression ===
    /// Compress and return the metadata of the generated archive.
    async fn compress(&self, source_path: &str, target_path: &str, user_id: &str, password: Option<&str>, encrypt_filenames: bool) -> VfsResult<VfsFileInfo>;
    /// Decompress and return the metadata of the target directory.
    async fn decompress(&self, archive_path: &str, target_dir: &str, user_id: &str, overwrite: bool, password: Option<&str>) -> VfsResult<VfsFileInfo>;
    // Temp Files ===
    async fn create_temp_file_for_upload(&self) -> VfsResult<(std::path::PathBuf, crate::utils::VfsTempFileGuard)>;
    /// Get backend storage type (e.g., "fs", "s3")
    fn get_backend_type(&self) -> String;
    // Business Extensions (Optimized by Index) ===
    /// Get recursive size of a directory.
    /// O(1) O(N_dirs)
    /// Indexed storage allows O(1) or O(N_dirs) queries, much faster than physical scanning.
    fn get_recursive_size(&self, path: &str) -> futures::future::BoxFuture<'static, VfsResult<i64>>;
    /// Set favorite color (0 means unfavorite).
    async fn set_favorite(&self, path: &str, color: i32) -> VfsResult<VfsFileInfo>;
    /// List favorite items.
    async fn list_favorites(&self, color_filter: Option<i32>) -> VfsResult<Vec<VfsFileInfo>>;
    /// List favorite items (paginated, with sorting and search)
    #[allow(clippy::manual_unwrap_or)]
    async fn list_favorites_paginated(&self, params: VfsPaginationParams<'_>, color_filter: Option<i32>) -> VfsResult<(Vec<VfsFileInfo>, i64)> {
        let mut all = self.list_favorites(color_filter).await?;
        // Search filter
        if let Some(kw) = params.keyword
            && !kw.is_empty()
        {
            let kw_lower = kw.to_lowercase();
            all.retain(|e| e.name.to_lowercase().contains(&kw_lower) || e.path.to_lowercase().contains(&kw_lower));
        }
        // Sorting
        if let Some(field) = params.sort_by {
            let order = match params.order {
                Some(order) => order,
                None => "asc",
            };
            let is_desc = order == "desc";
            all.sort_by(|a, b| {
                let cmp = match field {
                    "name" => a.name.cmp(&b.name),
                    "size" => a.size.cmp(&b.size),
                    "modified" => a.modified.cmp(&b.modified),
                    _ => a.name.cmp(&b.name),
                };
                if is_desc { cmp.reverse() } else { cmp }
            });
        }
        let total = all.len() as i64;
        let offset = ((params.page - 1) * params.page_size) as usize;
        let files = all.into_iter().skip(offset).take(params.page_size as usize).collect();
        Ok((files, total))
    }
    /// Move to recycle bin.
    /// Performs physical rename and stores original path in index.
    async fn move_to_trash(&self, path: &str) -> VfsResult<VfsFileInfo>;
    /// Restore from recycle bin.
    async fn restore_from_trash(&self, path: &str) -> VfsResult<VfsFileInfo>;
    /// List items in recycle bin.
    async fn list_trash(&self) -> VfsResult<Vec<VfsFileInfo>>;
    /// Force index sync.
    /// Scans physical storage and recalibrates DB index.
    async fn sync_index(&self, path: &str) -> VfsResult<Vec<VfsFileInfo>>;
    /// Migrate storage between backends.
    /// Moves files physically while logical path remains constant.
    async fn migrate_storage(&self, path: &str, target_storage_id: &str) -> VfsResult<VfsFileInfo>;
    // Persistent Async Task Submission (with DB tracking) ===
    async fn submit_batch_move(&self, _src_paths: Vec<String>, _dst_dir: String) -> VfsResult<String> {
        Err(crate::vfs::error::VfsError::Internal("Persistent tasks not supported by this storage backend".to_string()))
    }
    async fn submit_batch_copy(&self, _src_paths: Vec<String>, _dst_dir: String) -> VfsResult<String> {
        Err(crate::vfs::error::VfsError::Internal("Persistent tasks not supported by this storage backend".to_string()))
    }
    async fn submit_batch_delete(&self, _paths: Vec<String>) -> VfsResult<String> {
        Err(crate::vfs::error::VfsError::Internal("Persistent tasks not supported by this storage backend".to_string()))
    }
    async fn submit_batch_compress(&self, _paths: Vec<String>, _archive_name: String, _options: crate::utils::CompressionOptions, _delete_source: bool) -> VfsResult<String> {
        Err(crate::vfs::error::VfsError::Internal("Persistent tasks not supported by this storage backend".to_string()))
    }
    async fn submit_batch_decompress(&self, _paths: Vec<String>, _output_dir: String, _options: crate::utils::DecompressionOptions, _delete_archive: bool) -> VfsResult<String> {
        Err(crate::vfs::error::VfsError::Internal("Persistent tasks not supported by this storage backend".to_string()))
    }
}
