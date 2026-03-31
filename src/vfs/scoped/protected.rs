use super::ScopedVfsStorageEngine;
use crate::business::entities::file_index;
use crate::vfs::VfsFileInfo;
use crate::vfs::error::{VfsError, VfsResult};
use crate::vfs::traits::VfsStorage;
use bytes::{Bytes, BytesMut};
use futures::Stream;
use futures::StreamExt;
use futures::stream::BoxStream;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use std::pin::Pin;

fn file_info_from_index(row: &file_index::Model) -> VfsFileInfo {
    VfsFileInfo {
        name: row.name.clone().into(),
        path: row.path.clone().into(),
        is_dir: row.is_dir,
        size: row.size.max(0) as u64,
        modified: row.file_updated_at.map(|t| t.into()),
        favorite_color: row.favorite_color,
        has_active_share: None,
        has_active_direct: None,
        trashed_at: row.file_trashed_at.map(|t| t.into()),
        original_path: row.original_path.clone().map(Into::into),
    }
}

fn file_info_from_index_with_path(row: &file_index::Model, path: &str) -> VfsFileInfo {
    let name = std::path::Path::new(path)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("")
        .to_string();
    VfsFileInfo {
        name: name.into(),
        path: path.to_string().into(),
        is_dir: row.is_dir,
        size: row.size.max(0) as u64,
        modified: row.file_updated_at.map(|t| t.into()),
        favorite_color: row.favorite_color,
        has_active_share: None,
        has_active_direct: None,
        trashed_at: row.file_trashed_at.map(|t| t.into()),
        original_path: row.original_path.clone().map(Into::into),
    }
}

fn map_subtree_path(src_root: &str, dst_root: &str, current_path: &str) -> String {
    if current_path == src_root {
        dst_root.to_string()
    } else {
        format!("{}{}", dst_root, &current_path[src_root.len()..])
    }
}

fn header_from_index(
    row: &file_index::Model,
) -> Result<crate::vfs::protected::ProtectedHeader, VfsError> {
    let raw = row
        .protected_meta
        .as_deref()
        .ok_or_else(|| VfsError::Internal("Protected metadata is missing".to_string()))?;
    crate::vfs::protected::header_from_meta_json(raw).map_err(VfsError::Internal)
}

fn meta_from_index(
    row: &file_index::Model,
) -> Result<crate::vfs::protected::ProtectedMetaRecord, VfsError> {
    let raw = row
        .protected_meta
        .as_deref()
        .ok_or_else(|| VfsError::Internal("Protected metadata is missing".to_string()))?;
    crate::vfs::protected::meta_from_json(raw).map_err(VfsError::Internal)
}

impl ScopedVfsStorageEngine {
    pub(super) fn same_protected_domain(
        &self,
        src: &crate::vfs::protected::ProtectedPathPlan,
        dst: &crate::vfs::protected::ProtectedPathPlan,
    ) -> bool {
        src.root == dst.root && src.mode == dst.mode && src.key_slot_id == dst.key_slot_id
    }

    async fn protected_subtree_rows(&self, root: &str) -> VfsResult<Vec<file_index::Model>> {
        let prefix = format!("{}/", root.trim_end_matches('/'));
        let mut rows = file_index::Entity::find()
            .filter(file_index::Column::UserId.eq(self.user_id.as_ref()))
            .filter(
                file_index::Column::Path
                    .eq(root)
                    .or(file_index::Column::Path.starts_with(prefix.as_str())),
            )
            .filter(file_index::Column::RowDeletedAt.is_null())
            .all(self.db.as_ref())
            .await
            .map_err(|e| VfsError::Internal(e.to_string()))?;
        rows.sort_by_key(|row| row.path.len());
        Ok(rows)
    }

    async fn protected_read_payload_range(
        &self,
        row: &file_index::Model,
        start: u64,
        end: u64,
    ) -> VfsResult<(
        crate::vfs::protected::ProtectedHeader,
        crate::vfs::protected::ProtectedMetaRecord,
        u64,
        Bytes,
        Option<Bytes>,
    )> {
        let backend_key = row.backend_key.clone().ok_or_else(|| {
            VfsError::Internal("Protected file backend key is missing".to_string())
        })?;
        let header = header_from_index(row)?;
        let meta = meta_from_index(row)?;
        let physical_size = row.physical_size.unwrap_or_default().max(0) as u64;
        let logical_end = end.min(header.logical_size);
        let (payload_logical_start, payload, mac_bytes) = match header.mode {
            crate::vfs::protected::ProtectedMode::Obfuscate => {
                let block_size = (header.block_size as u64).max(1);
                let aligned_start = (start / block_size) * block_size;
                let aligned_end = logical_end
                    .saturating_add(block_size - 1)
                    .checked_div(block_size)
                    .unwrap_or(0)
                    * block_size;
                let clamped_end = aligned_end.min(header.logical_size);
                let payload = self
                    .pool
                    .read_range(
                        &backend_key,
                        crate::vfs::protected::PROTECTED_HEADER_LEN as u64 + aligned_start,
                        crate::vfs::protected::PROTECTED_HEADER_LEN as u64 + clamped_end,
                    )
                    .await?
                    .0;
                (aligned_start, payload, None)
            }
            crate::vfs::protected::ProtectedMode::Encrypt => {
                if meta.integrity == crate::vfs::protected::PROTECTED_INTEGRITY_HMAC_SHA256_CHUNKED
                {
                    let chunk_size = meta
                        .integrity_chunk_size
                        .unwrap_or(crate::vfs::protected::DEFAULT_ENCRYPT_INTEGRITY_CHUNK_SIZE)
                        as u64;
                    let aligned_start = (start / chunk_size) * chunk_size;
                    let aligned_end = logical_end
                        .saturating_add(chunk_size - 1)
                        .checked_div(chunk_size)
                        .unwrap_or(0)
                        * chunk_size;
                    let clamped_end = aligned_end.min(header.logical_size);
                    let payload = self
                        .pool
                        .read_range(
                            &backend_key,
                            crate::vfs::protected::PROTECTED_HEADER_LEN as u64 + aligned_start,
                            crate::vfs::protected::PROTECTED_HEADER_LEN as u64 + clamped_end,
                        )
                        .await?
                        .0;
                    let start_chunk = aligned_start / chunk_size;
                    let covered_chunks = crate::vfs::protected::integrity_chunk_count(
                        clamped_end.saturating_sub(aligned_start),
                        chunk_size as u32,
                    ) as u64;
                    let mac_table_offset =
                        crate::vfs::protected::PROTECTED_HEADER_LEN as u64 + header.logical_size;
                    let mac_start = mac_table_offset
                        + start_chunk * crate::vfs::protected::PROTECTED_MAC_LEN as u64;
                    let mac_end = mac_start
                        + covered_chunks * crate::vfs::protected::PROTECTED_MAC_LEN as u64;
                    let mac_bytes = self
                        .pool
                        .read_range(&backend_key, mac_start, mac_end.min(physical_size))
                        .await?
                        .0;
                    (aligned_start, payload, Some(mac_bytes))
                } else {
                    let physical_end = (crate::vfs::protected::PROTECTED_HEADER_LEN as u64
                        + logical_end)
                        .min(physical_size.max(crate::vfs::protected::PROTECTED_HEADER_LEN as u64));
                    let payload = self
                        .pool
                        .read_range(
                            &backend_key,
                            crate::vfs::protected::PROTECTED_HEADER_LEN as u64 + start,
                            physical_end,
                        )
                        .await?
                        .0;
                    (start, payload, None)
                }
            }
        };
        Ok((header, meta, payload_logical_start, payload, mac_bytes))
    }

    pub(super) async fn protected_read_all_impl(
        &self,
        normalized: &str,
        plan: &crate::vfs::protected::ProtectedPathPlan,
    ) -> VfsResult<(Bytes, VfsFileInfo)> {
        let row = self
            .get_index_metadata(normalized)
            .await?
            .ok_or_else(|| VfsError::NotFound(normalized.to_string()))?;
        if row.is_dir {
            return Err(VfsError::Internal(
                "Protected directories cannot be read as files".to_string(),
            ));
        }
        let (header, meta, payload_logical_start, payload, mac_bytes) = self
            .protected_read_payload_range(&row, 0, row.size.max(0) as u64)
            .await?;
        if header.mode == crate::vfs::protected::ProtectedMode::Encrypt
            && meta.integrity == crate::vfs::protected::PROTECTED_INTEGRITY_HMAC_SHA256_CHUNKED
        {
            let key = plan.encrypt_key.as_ref().ok_or_else(|| {
                VfsError::Internal("Protected encrypt key is missing".to_string())
            })?;
            let chunk_size = meta
                .integrity_chunk_size
                .unwrap_or(crate::vfs::protected::DEFAULT_ENCRYPT_INTEGRITY_CHUNK_SIZE);
            crate::vfs::protected::verify_encrypt_window(
                key,
                chunk_size,
                payload_logical_start,
                payload.as_ref(),
                mac_bytes
                    .as_ref()
                    .ok_or_else(|| {
                        VfsError::Internal("Protected MAC bytes are missing".to_string())
                    })?
                    .as_ref(),
            )
            .map_err(VfsError::Internal)?;
        }
        let data = crate::vfs::protected::decode_range(
            &self.user_id,
            &plan.key_slot_id,
            plan.encrypt_key,
            plan.workers,
            &header,
            payload,
            payload_logical_start,
            0,
            row.size.max(0) as u64,
        )
        .map_err(VfsError::Internal)?;
        Ok((data, file_info_from_index(&row)))
    }

    pub(super) async fn protected_read_range_impl(
        &self,
        normalized: &str,
        start: u64,
        end: u64,
        plan: &crate::vfs::protected::ProtectedPathPlan,
    ) -> VfsResult<(Bytes, VfsFileInfo)> {
        let row = self
            .get_index_metadata(normalized)
            .await?
            .ok_or_else(|| VfsError::NotFound(normalized.to_string()))?;
        let logical_end = end.min(row.size.max(0) as u64);
        if start >= logical_end {
            return Ok((Bytes::new(), file_info_from_index(&row)));
        }
        let (header, meta, payload_logical_start, payload, mac_bytes) = self
            .protected_read_payload_range(&row, start, logical_end)
            .await?;
        if header.mode == crate::vfs::protected::ProtectedMode::Encrypt
            && meta.integrity == crate::vfs::protected::PROTECTED_INTEGRITY_HMAC_SHA256_CHUNKED
        {
            let key = plan.encrypt_key.as_ref().ok_or_else(|| {
                VfsError::Internal("Protected encrypt key is missing".to_string())
            })?;
            let chunk_size = meta
                .integrity_chunk_size
                .unwrap_or(crate::vfs::protected::DEFAULT_ENCRYPT_INTEGRITY_CHUNK_SIZE);
            crate::vfs::protected::verify_encrypt_window(
                key,
                chunk_size,
                payload_logical_start,
                payload.as_ref(),
                mac_bytes
                    .as_ref()
                    .ok_or_else(|| {
                        VfsError::Internal("Protected MAC bytes are missing".to_string())
                    })?
                    .as_ref(),
            )
            .map_err(VfsError::Internal)?;
        }
        let data = crate::vfs::protected::decode_range(
            &self.user_id,
            &plan.key_slot_id,
            plan.encrypt_key,
            plan.workers,
            &header,
            payload,
            payload_logical_start,
            start,
            logical_end,
        )
        .map_err(VfsError::Internal)?;
        Ok((data, file_info_from_index(&row)))
    }

    pub(super) async fn protected_read_stream_range_impl(
        &self,
        normalized: &str,
        range: std::ops::Range<u64>,
        plan: &crate::vfs::protected::ProtectedPathPlan,
    ) -> VfsResult<(
        Pin<Box<dyn Stream<Item = VfsResult<Bytes>> + Send + Sync>>,
        VfsFileInfo,
    )> {
        let (payload, info) = self
            .protected_read_range_impl(normalized, range.start, range.end, plan)
            .await?;
        let stream = futures::stream::once(async move { Ok(payload) });
        Ok((Box::pin(stream), info))
    }

    pub(super) async fn protected_write_impl(
        &self,
        normalized: &str,
        data: Bytes,
        plan: &crate::vfs::protected::ProtectedPathPlan,
    ) -> VfsResult<VfsFileInfo> {
        self.check_maintenance()?;
        let existing = self.get_index_metadata(normalized).await?;
        let current_size = existing.as_ref().map(|row| row.size.max(0)).unwrap_or(0);
        let diff = data.len() as i64 - current_size;
        if diff > 0 {
            self.check_quota(diff).await?;
        }
        let backend_key = existing
            .as_ref()
            .and_then(|row| row.backend_key.clone())
            .unwrap_or(
                self.next_protected_blob_physical_path(&plan.key_slot_id)
                    .await?,
            );
        let (encoded, _header, protected_meta) =
            crate::vfs::protected::encode_payload(&self.user_id, plan, data.clone())
                .map_err(VfsError::Internal)?;
        let physical_size = encoded.len() as i64;
        let wal_id = self
            .begin_wal(
                crate::vfs::wal::WalOperation::Write {
                    path: normalized.to_string(),
                    size: data.len() as u64,
                    protected: Some(crate::vfs::wal::WalProtectedWriteMeta {
                        backend_key: backend_key.clone(),
                        physical_size,
                        protected_meta: protected_meta.clone(),
                    }),
                },
                self.should_skip_wal_for_write(normalized, data.len() as u64)
                    .await,
            )
            .await?;
        let write_result = self.pool.write(&backend_key, encoded).await;
        match write_result {
            Ok(_) => {
                self.mark_wal_physical_done(wal_id).await;
                let info = VfsFileInfo {
                    name: std::path::Path::new(normalized)
                        .file_name()
                        .and_then(|value| value.to_str())
                        .unwrap_or("")
                        .to_string()
                        .into(),
                    path: normalized.to_string().into(),
                    is_dir: false,
                    size: data.len() as u64,
                    modified: Some(chrono::Utc::now()),
                    favorite_color: existing.as_ref().map_or(0, |row| row.favorite_color),
                    has_active_share: None,
                    has_active_direct: None,
                    trashed_at: None,
                    original_path: None,
                };
                self.upsert_index_helper_with_backend_key(
                    normalized,
                    &info,
                    Some(&backend_key),
                    Some(physical_size),
                    Some(&protected_meta),
                )
                .await?;
                self.complete_wal(wal_id).await;
                if diff != 0 {
                    let _ = self.update_quota(diff).await;
                }
                self.journal_log("PROTECTED_WRITE", normalized, None, true, None)
                    .await;
                Ok(info)
            }
            Err(err) => {
                self.fail_wal(wal_id, &err.to_string()).await;
                self.journal_log(
                    "PROTECTED_WRITE",
                    normalized,
                    None,
                    false,
                    Some(err.to_string()),
                )
                .await;
                Err(err)
            }
        }
    }

    pub(super) async fn protected_write_stream_impl(
        &self,
        normalized: &str,
        mut stream: BoxStream<'static, VfsResult<Bytes>>,
        plan: &crate::vfs::protected::ProtectedPathPlan,
    ) -> VfsResult<VfsFileInfo> {
        let mut buffer = BytesMut::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            buffer.extend_from_slice(&chunk);
        }
        self.protected_write_impl(normalized, buffer.freeze(), plan)
            .await
    }

    pub(super) async fn protected_write_at_impl(
        &self,
        normalized: &str,
        offset: u64,
        data: Bytes,
        plan: &crate::vfs::protected::ProtectedPathPlan,
    ) -> VfsResult<VfsFileInfo> {
        let current = match self.protected_read_all_impl(normalized, plan).await {
            Ok((bytes, _)) => bytes.to_vec(),
            Err(VfsError::NotFound(_)) => Vec::new(),
            Err(err) => return Err(err),
        };
        let mut next = current;
        let offset_idx = offset as usize;
        if offset_idx + data.len() > next.len() {
            next.resize(offset_idx + data.len(), 0);
        }
        if let Some(slice) = next.get_mut(offset_idx..offset_idx + data.len()) {
            slice.copy_from_slice(&data);
        }
        self.protected_write_impl(normalized, Bytes::from(next), plan)
            .await
    }

    pub(super) async fn protected_delete_impl(&self, normalized: &str) -> VfsResult<VfsFileInfo> {
        self.check_maintenance()?;
        let row = self
            .get_index_metadata(normalized)
            .await?
            .ok_or_else(|| VfsError::NotFound(normalized.to_string()))?;
        let info = file_info_from_index(&row);
        let descendants = if row.is_dir {
            let prefix = format!("{}/", normalized.trim_end_matches('/'));
            file_index::Entity::find()
                .filter(file_index::Column::UserId.eq(self.user_id.as_ref()))
                .filter(
                    file_index::Column::Path
                        .eq(normalized)
                        .or(file_index::Column::Path.starts_with(prefix.as_str())),
                )
                .filter(file_index::Column::RowDeletedAt.is_null())
                .all(self.db.as_ref())
                .await
                .map_err(|e| VfsError::Internal(e.to_string()))?
        } else {
            vec![row.clone()]
        };
        let reclaimed_size: i64 = descendants
            .iter()
            .filter(|item| !item.is_dir)
            .map(|item| item.size.max(0))
            .sum();
        let wal_id = self
            .begin_wal(
                crate::vfs::wal::WalOperation::Delete {
                    path: normalized.to_string(),
                },
                self.should_skip_wal_for_path(normalized).await,
            )
            .await?;
        let mut deleted_backend_keys = std::collections::HashSet::new();
        for item in &descendants {
            if let Some(backend_key) = item.backend_key.as_deref()
                && deleted_backend_keys.insert(backend_key.to_string())
            {
                self.pool.delete(backend_key).await?;
            }
        }
        self.mark_wal_physical_done(wal_id).await;
        self.index_service
            .delete_file(&self.user_id, normalized)
            .await
            .map_err(|e| VfsError::Internal(e.to_string()))?;
        self.complete_wal(wal_id).await;
        if reclaimed_size > 0 {
            let _ = self.update_quota(-reclaimed_size).await;
        }
        self.cache.invalidate_parent_ls(normalized).await;
        self.cache.invalidate("stat", normalized).await;
        self.journal_log("PROTECTED_DELETE", normalized, None, true, None)
            .await;
        Ok(info)
    }

    pub(super) async fn protected_create_dir_impl(
        &self,
        normalized: &str,
    ) -> VfsResult<VfsFileInfo> {
        if let Some(row) = self.get_index_metadata(normalized).await? {
            return Ok(file_info_from_index(&row));
        }
        let info = VfsFileInfo {
            name: std::path::Path::new(normalized)
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or("")
                .to_string()
                .into(),
            path: normalized.to_string().into(),
            is_dir: true,
            size: 0,
            modified: Some(chrono::Utc::now()),
            favorite_color: 0,
            has_active_share: None,
            has_active_direct: None,
            trashed_at: None,
            original_path: None,
        };
        self.upsert_index_helper_with_backend_key(normalized, &info, None, None, None)
            .await?;
        self.journal_log("PROTECTED_MKDIR", normalized, None, true, None)
            .await;
        Ok(info)
    }

    pub(super) async fn protected_move_same_domain_impl(
        &self,
        src: &str,
        dst: &str,
    ) -> VfsResult<VfsFileInfo> {
        if self.get_index_metadata(dst).await?.is_some() {
            return Err(VfsError::Internal(
                "Protected move target already exists".to_string(),
            ));
        }
        let wal_id = self
            .begin_wal(
                crate::vfs::wal::WalOperation::Move {
                    src: src.to_string(),
                    dst: dst.to_string(),
                },
                self.should_skip_wal_for_path(src).await,
            )
            .await?;
        self.mark_wal_physical_done(wal_id).await;
        self.index_service
            .move_file(&self.user_id, src, dst)
            .await
            .map_err(|e| VfsError::Internal(e.to_string()))?;
        self.complete_wal(wal_id).await;
        self.cache.invalidate_parent_ls(src).await;
        self.cache.invalidate_parent_ls(dst).await;
        self.cache.invalidate("stat", src).await;
        self.cache.invalidate("stat", dst).await;
        self.journal_log("PROTECTED_MOVE", src, Some(dst), true, None)
            .await;
        self.stat_impl(dst).await
    }

    pub(super) async fn protected_copy_same_domain_impl(
        &self,
        src: &str,
        dst: &str,
        plan: &crate::vfs::protected::ProtectedPathPlan,
    ) -> VfsResult<VfsFileInfo> {
        if self.get_index_metadata(dst).await?.is_some() {
            return Err(VfsError::Internal(
                "Protected copy target already exists".to_string(),
            ));
        }
        let rows = self.protected_subtree_rows(src).await?;
        if rows.is_empty() {
            return Err(VfsError::NotFound(src.to_string()));
        }
        let total_size: i64 = rows
            .iter()
            .filter(|row| !row.is_dir)
            .map(|row| row.size.max(0))
            .sum();
        if total_size > 0 {
            self.check_quota(total_size).await?;
        }
        for row in &rows {
            let new_path = map_subtree_path(src, dst, &row.path);
            let info = file_info_from_index_with_path(row, &new_path);
            if row.is_dir {
                self.upsert_index_helper_with_backend_key(&new_path, &info, None, None, None)
                    .await?;
                continue;
            }
            let src_backend = row.backend_key.as_deref().ok_or_else(|| {
                VfsError::Internal("Protected source backend key is missing".to_string())
            })?;
            let dst_backend = self
                .next_protected_blob_physical_path(&plan.key_slot_id)
                .await?;
            self.pool.copy_file(src_backend, &dst_backend).await?;
            self.upsert_index_helper_with_backend_key(
                &new_path,
                &info,
                Some(&dst_backend),
                row.physical_size,
                row.protected_meta.as_deref(),
            )
            .await?;
        }
        if total_size > 0 {
            let _ = self.update_quota(total_size).await;
        }
        self.journal_log("PROTECTED_COPY", src, Some(dst), true, None)
            .await;
        self.stat_impl(dst).await
    }
}
