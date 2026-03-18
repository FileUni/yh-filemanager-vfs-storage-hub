use super::ScopedVfsStorageEngine;
use crate::utils::temp_file::VfsTempFileGuard;
use crate::vfs::error::VfsResult;
use crate::vfs::types::VfsBatchResult;
use crate::vfs::{VfsError, VfsFileInfo, VfsMetadata, VfsPaginationParams, VfsStorage};
use bytes::Bytes;
use futures::Stream;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use std::pin::Pin;
#[async_trait::async_trait]
impl VfsStorage for ScopedVfsStorageEngine {
    async fn read(&self, path: &str) -> VfsResult<(Bytes, VfsFileInfo)> {
        self.read_impl(path).await
    }
    async fn write(&self, path: &str, data: Bytes) -> VfsResult<VfsFileInfo> {
        self.write_impl(path, data).await
    }
    async fn delete(&self, path: &str) -> VfsResult<VfsFileInfo> {
        self.delete_impl(path).await
    }
    async fn list(&self, path: &str) -> VfsResult<Vec<VfsFileInfo>> {
        self.list_impl(path).await
    }
    fn list_stream(&self, path: &str) -> BoxStream<'static, VfsResult<VfsFileInfo>> {
        self.list_stream_impl(path)
    }
    async fn list_recursive(&self, path: &str) -> VfsResult<Vec<VfsFileInfo>> {
        self.list_recursive_impl(path).await
    }
    async fn list_files_paginated(
        &self,
        parent_path: &str,
        page: i64,
        page_size: i64,
    ) -> VfsResult<(Vec<VfsFileInfo>, i64)> {
        self.list_files_paginated_impl(parent_path, page, page_size)
            .await
    }
    async fn list_files_paginated_with_sort(
        &self,
        parent_path: &str,
        params: VfsPaginationParams<'_>,
    ) -> VfsResult<(Vec<VfsFileInfo>, i64)> {
        self.list_files_paginated_with_sort_impl(parent_path, params)
            .await
    }
    async fn search_files_paginated(
        &self,
        keyword: &str,
        page: i64,
        page_size: i64,
    ) -> VfsResult<(Vec<VfsFileInfo>, i64)> {
        self.search_files_paginated_impl(keyword, page, page_size)
            .await
    }
    async fn list_recycle_bin_paginated(
        &self,
        page: i64,
        page_size: i64,
    ) -> VfsResult<(Vec<VfsFileInfo>, i64)> {
        self.list_recycle_bin_paginated_impl(page, page_size).await
    }
    async fn list_recycle_bin_paginated_with_sort(
        &self,
        params: VfsPaginationParams<'_>,
    ) -> VfsResult<(Vec<VfsFileInfo>, i64)> {
        self.list_recycle_bin_paginated_with_sort_impl(params).await
    }
    async fn exists(&self, path: &str) -> VfsResult<bool> {
        self.exists_impl(path).await
    }
    async fn stat(&self, path: &str) -> VfsResult<VfsFileInfo> {
        self.stat_impl(path).await
    }
    async fn read_stream(
        &self,
        path: &str,
    ) -> VfsResult<(
        Pin<Box<dyn Stream<Item = VfsResult<Bytes>> + Send + Sync>>,
        VfsFileInfo,
    )> {
        self.read_stream_range(path, 0..u64::MAX).await
    }
    async fn read_stream_range(
        &self,
        path: &str,
        range: std::ops::Range<u64>,
    ) -> VfsResult<(
        Pin<Box<dyn Stream<Item = VfsResult<Bytes>> + Send + Sync>>,
        VfsFileInfo,
    )> {
        self.read_stream_range_impl(path, range).await
    }
    async fn write_stream(
        &self,
        path: &str,
        stream: BoxStream<'static, VfsResult<Bytes>>,
    ) -> VfsResult<VfsFileInfo> {
        self.write_stream_impl(path, stream).await
    }
    async fn metadata(&self, path: &str) -> VfsResult<VfsMetadata> {
        self.metadata_impl(path).await
    }
    async fn read_range(
        &self,
        path: &str,
        start: u64,
        end: u64,
    ) -> VfsResult<(Bytes, VfsFileInfo)> {
        self.read_range_impl(path, start, end).await
    }
    async fn write_at(&self, path: &str, offset: u64, data: Bytes) -> VfsResult<VfsFileInfo> {
        self.write_at_impl(path, offset, data).await
    }
    async fn set_times(
        &self,
        path: &str,
        _atime: Option<u64>,
        _mtime: Option<u64>,
    ) -> VfsResult<VfsFileInfo> {
        self.stat(path).await
    }
    async fn check_quota(&self, additional_size: i64) -> VfsResult<()> {
        self.check_quota(additional_size).await
    }
    async fn get_quota(&self) -> VfsResult<(u64, Option<u64>)> {
        self.get_quota_impl().await
    }
    async fn create_dir(&self, path: &str) -> VfsResult<VfsFileInfo> {
        self.create_dir_impl(path).await
    }
    async fn create_dir_all(&self, path: &str) -> VfsResult<VfsFileInfo> {
        self.create_dir_impl(path).await
    }
    async fn move_file(&self, src: &str, dst: &str) -> VfsResult<VfsFileInfo> {
        self.move_file_impl(src, dst).await
    }
    async fn copy_file(&self, src: &str, dst: &str) -> VfsResult<VfsFileInfo> {
        self.copy_file_impl(src, dst).await
    }
    async fn canonicalize_path(&self, path: &str) -> VfsResult<String> {
        self.validate_file_operation(path).await
    }
    async fn batch_remove(&self, paths: &[String]) -> VfsResult<VfsBatchResult> {
        self.batch_remove_impl(paths).await
    }
    async fn batch_move(&self, src_paths: &[String], dst_dir: &str) -> VfsResult<VfsBatchResult> {
        self.batch_move_impl(src_paths, dst_dir).await
    }
    async fn compress(
        &self,
        source_path: &str,
        target_path: &str,
        user_id: &str,
        password: Option<&str>,
        encrypt_filenames: bool,
    ) -> VfsResult<VfsFileInfo> {
        crate::utils::compression::compress_compat(
            self.pool.as_ref(),
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
        crate::utils::compression::decompress_compat(
            self.pool.as_ref(),
            archive_path,
            target_dir,
            user_id,
            overwrite,
            password,
        )
        .await
    }
    async fn create_temp_file_for_upload(
        &self,
    ) -> VfsResult<(std::path::PathBuf, VfsTempFileGuard)> {
        self.temp_manager
            .create_user_temp_file(&self.user_id, "upload")
            .await
            .map_err(|e| VfsError::Internal(e.to_string()))
    }
    fn get_backend_type(&self) -> String {
        "scoped".to_string()
    }
    fn get_recursive_size(&self, path: &str) -> BoxFuture<'static, VfsResult<i64>> {
        self.get_recursive_size_impl(path)
    }
    async fn set_favorite(&self, path: &str, color: i32) -> VfsResult<VfsFileInfo> {
        self.set_favorite_impl(path, color).await
    }
    async fn list_favorites(&self, color_filter: Option<i32>) -> VfsResult<Vec<VfsFileInfo>> {
        self.list_favorites_impl(color_filter).await
    }
    async fn list_favorites_paginated(
        &self,
        params: VfsPaginationParams<'_>,
        color_filter: Option<i32>,
    ) -> VfsResult<(Vec<VfsFileInfo>, i64)> {
        self.list_favorites_paginated_impl(params, color_filter)
            .await
    }
    async fn move_to_trash(&self, path: &str) -> VfsResult<VfsFileInfo> {
        self.move_to_trash_impl(path).await
    }
    async fn restore_from_trash(&self, path: &str) -> VfsResult<VfsFileInfo> {
        self.restore_from_trash_impl(path).await
    }
    async fn list_trash(&self) -> VfsResult<Vec<VfsFileInfo>> {
        self.list_trash_impl().await
    }
    async fn sync_index(&self, path: &str) -> VfsResult<Vec<VfsFileInfo>> {
        self.sync_index_impl(path).await
    }
    async fn migrate_storage(
        &self,
        _path: &str,
        _target_storage_id: &str,
    ) -> VfsResult<VfsFileInfo> {
        Err(VfsError::Internal("Not implemented".to_string()))
    }
    async fn submit_batch_move(
        &self,
        src_paths: Vec<String>,
        dst_dir: String,
    ) -> VfsResult<String> {
        self.submit_batch_move_impl(src_paths, dst_dir).await
    }
    async fn submit_batch_copy(
        &self,
        src_paths: Vec<String>,
        dst_dir: String,
    ) -> VfsResult<String> {
        self.submit_batch_copy_impl(src_paths, dst_dir).await
    }
    async fn submit_batch_delete(&self, paths: Vec<String>) -> VfsResult<String> {
        self.submit_batch_delete_impl(paths).await
    }
    async fn submit_batch_compress(
        &self,
        paths: Vec<String>,
        archive_name: String,
        options: crate::utils::CompressionOptions,
        delete_source: bool,
    ) -> VfsResult<String> {
        self.submit_batch_compress_impl(paths, archive_name, options, delete_source)
            .await
    }
    async fn submit_batch_decompress(
        &self,
        paths: Vec<String>,
        output_dir: String,
        options: crate::utils::DecompressionOptions,
        delete_archive: bool,
    ) -> VfsResult<String> {
        self.submit_batch_decompress_impl(paths, output_dir, options, delete_archive)
            .await
    }
}
