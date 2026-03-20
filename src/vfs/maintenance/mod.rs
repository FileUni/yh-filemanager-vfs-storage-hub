//! VFS maintenance routines.

use crate::utils::temp_file::get_global_temp_manager;
use crate::vfs::{VfsStorage, VfsStorageHub};
use sea_orm::{DatabaseConnection, EntityTrait};
use std::sync::Arc;
use yh_console_log::yhlog;
pub mod sync_guard;
pub use sync_guard::SyncGuardManager;

#[inline]
fn maintenance_batch_size() -> u64 {
    match yh_config_infra::utils::current_hardware_profile() {
        "low_memory" => 64,
        "throughput" => 512,
        _ => 128,
    }
}

pub struct VfsMaintenanceService;
impl VfsMaintenanceService {
    /// Cleanup all expired temporary files
    pub async fn cleanup_temp_files() -> anyhow::Result<usize> {
        if let Some(mgr) = get_global_temp_manager().await {
            let count = mgr.cleanup_temp_files().await?;
            if count > 0 {
                yhlog(
                    "info",
                    &format!("VFS Maintenance: Cleaned {} expired temp files", count),
                );
            }
            Ok(count)
        } else {
            Ok(0)
        }
    }
    /// Cleanup all expired S3 multipart uploads
    pub async fn cleanup_expired_s3_uploads(max_age_secs: u64) -> usize {
        if let Some(hub) = crate::vfs::hub::get_vfs_storage_hub() {
            let count = hub.cleanup_all_s3_multiparts(max_age_secs).await;
            if count > 0 {
                yhlog(
                    "info",
                    &format!(
                        "VFS Maintenance: Cleaned {} expired S3 multipart uploads (KV phase)",
                        count
                    ),
                );
            }
            count
        } else {
            0
        }
    }
    /// Execute physical audit for S3 multipart uploads
    pub async fn cleanup_orphaned_s3_uploads(
        hub: Arc<VfsStorageHub>,
        db: Arc<DatabaseConnection>,
    ) -> usize {
        let mut total_deleted = 0;
        let vfs_cfg = crate::config::get_vfs_hub_config().await;
        let mc = vfs_cfg.get_maintenance();
        if !mc.is_s3_multipart_cleanup_enabled() {
            return 0;
        }
        let grace_period = mc.get_s3_multipart_grace_period_secs();
        use crate::business::entities::user_settings;
        if let Ok(users) = user_settings::Entity::find().all(&*db).await {
            for user in users {
                if let Ok(engine) = hub
                    .create_scoped_engine(Arc::clone(&db), &user.user_id, "100", None)
                    .await
                {
                    let multipart_root = format!("{}/.multipart", crate::vfs::LOGICAL_TEMP_PREFIX);
                    // Trait
                    if let Ok(entries) = engine.list(&multipart_root).await {
                        let now = chrono::Utc::now();
                        for entry in entries {
                            if entry.is_dir {
                                let upload_id = &entry.name;
                                if let Some(modified) = entry.modified
                                    && (now - modified).num_seconds() > grace_period as i64
                                {
                                    let kv_key = format!("s3:multipart:{}", upload_id);
                                    if let Ok(exists) =
                                        yh_fast_kv_storage_hub::api::helpers::exists(&kv_key).await
                                        && !exists
                                    {
                                        yhlog(
                                            "warn",
                                            &format!(
                                                "S3 Cleanup: Orphaned multipart dir found for user {}: {}. Deleting...",
                                                user.user_id, upload_id
                                            ),
                                        );
                                        let _ = engine
                                            .delete(&format!("{}/{}", multipart_root, upload_id))
                                            .await;
                                        total_deleted += 1;
                                    }
                                }
                            }
                        }
                    }
                }
                // Sequential execution
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
        total_deleted
    }
    /// Perform index sync for interrupted tasks
    #[allow(clippy::manual_unwrap_or)]
    pub async fn sync_index_for_interrupted_task(
        hub: Arc<VfsStorageHub>,
        db: Arc<sea_orm::DatabaseConnection>,
        user_id: &str,
        task_id: &(dyn std::fmt::Display + Send + Sync),
        path_hint: Option<&str>,
    ) {
        let lock_key = format!("vfs:sync_lock:{}", user_id);
        // Try to acquire sync lock (KV distributed lock)
        if let Ok(manager) = yh_fast_kv_storage_hub::api::cache_manager::CacheManager::get().await {
            let backend = manager.get_backend().await;
            let lock_exists = match backend.exists(&lock_key).await {
                Ok(exists) => exists,
                Err(err) => {
                    yhlog(
                        "warn",
                        &format!(
                            "VFS Maintenance: Failed to check lock key {}: {}",
                            lock_key, err
                        ),
                    );
                    false
                }
            };
            if lock_exists {
                yhlog(
                    "info",
                    &format!(
                        "VFS Maintenance: Sync already in progress for user {}, skipping for task {}",
                        user_id, task_id
                    ),
                );
                return;
            }
            let _ = backend
                .set(&lock_key, bytes::Bytes::from("locked"), Some(600))
                .await;
        }
        let sync_path = match path_hint {
            Some(path) => path,
            None => "/",
        };
        yhlog(
            "warn",
            &format!(
                "VFS Maintenance: Starting targeted sync for user {} at path {} (Task: {})",
                user_id, sync_path, task_id
            ),
        );
        if let Ok(engine) = hub.create_scoped_engine(db, user_id, "100", None).await {
            // ScopedVfsStorageEngine SyncGuardManager
            let _ = engine.sync_index(sync_path).await;
        }
        // Release lock
        if let Ok(manager) = yh_fast_kv_storage_hub::api::cache_manager::CacheManager::get().await {
            let _ = manager.get_backend().await.del(&lock_key).await;
        }
    }
    /// Cleanup all expired trash files
    pub async fn cleanup_expired_trash(hub: Arc<VfsStorageHub>) -> anyhow::Result<usize> {
        let vfs_cfg = crate::config::get_vfs_hub_config().await;
        let retention_days = vfs_cfg.get_file_share().get_trash_retention_days();
        if retention_days == 0 {
            return Ok(0);
        }
        let index_service = hub.get_index_service().await?;
        let mut total_deleted = 0;
        let mut logged_start = false;
        let batch_size = maintenance_batch_size();
        loop {
            let expired_items = index_service
                .find_expired_trash_batch(retention_days, batch_size)
                .await
                .map_err(|e| anyhow::anyhow!("DB error: {}", e))?;
            if expired_items.is_empty() {
                break;
            }
            if !logged_start {
                yhlog(
                    "info",
                    &format!(
                        "VFS Maintenance: Found expired trash items. Starting batched physical cleanup with batch_size={}...",
                        batch_size
                    ),
                );
                logged_start = true;
            }
            let mut user_items: std::collections::HashMap<
                String,
                Vec<crate::business::entities::file_index::Model>,
            > = std::collections::HashMap::new();
            for mut item in expired_items {
                let user_id = std::mem::take(&mut item.user_id);
                user_items.entry(user_id).or_default().push(item);
            }
            for (user_id, items) in user_items {
                if let Ok(engine) = hub
                    .create_scoped_engine(hub.get_db(), &user_id, "100", None)
                    .await
                {
                    for item in items {
                        match engine.delete(&item.path).await {
                            Ok(_) => {
                                total_deleted += 1;
                            }
                            Err(e) => {
                                yhlog(
                                    "error",
                                    &format!(
                                        "VFS Maintenance: Failed to physically delete trash item {} for user {}: {}",
                                        item.path, user_id, e
                                    ),
                                );
                            }
                        }
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
        if total_deleted > 0 {
            yhlog(
                "info",
                &format!(
                    "VFS Maintenance: Cleaned up {} expired trash items",
                    total_deleted
                ),
            );
        }
        Ok(total_deleted)
    }
}
