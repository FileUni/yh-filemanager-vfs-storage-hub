use super::ScopedVfsStorageEngine;
use crate::vfs::error::{VfsError, VfsResult};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

static BATCH_TASK_SEMAPHORE: once_cell::sync::OnceCell<Arc<Semaphore>> =
    once_cell::sync::OnceCell::new();
const BATCH_TASK_TIMEOUT: Duration = Duration::from_secs(24 * 3600);

impl ScopedVfsStorageEngine {
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
            .map_err(|e| VfsError::Internal(format!("Acquire batch task permit failed: {}", e)))
    }

    fn spawn_batch_task<F>(task_name: &'static str, task: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(async move {
            if tokio::time::timeout(BATCH_TASK_TIMEOUT, task)
                .await
                .is_err()
            {
                yh_console_log::yhlog(
                    "error",
                    &format!("Batch task '{}' timed out after 24 hours", task_name),
                );
            }
        });
    }

    pub(super) async fn submit_batch_move_impl(
        &self,
        src_paths: Vec<String>,
        dst_dir: String,
    ) -> VfsResult<String> {
        self.check_maintenance()?;
        let task_handler = self
            .task_handler
            .as_ref()
            .ok_or_else(|| VfsError::Internal("Task handler not configured".to_string()))?;
        let payload = serde_json::json!({ "src_paths": src_paths, "dst_dir": dst_dir });
        let task_id = task_handler
            .create_task(&self.user_id, "batch_move", payload)
            .await
            .map_err(VfsError::Internal)?;
        let engine = Arc::new(self.clone_for_async());
        let handler = Arc::clone(task_handler);
        let vfs_cfg = crate::config::get_vfs_hub_config().await;
        let timeout = vfs_cfg.get_batch_operation().get_timeout_secs();
        Self::spawn_batch_task("batch_move", async move {
            let _permit = match Self::acquire_batch_task_permit().await {
                Ok(permit) => permit,
                Err(err) => {
                    let _ = handler.fail_task(task_id, &err.to_string()).await;
                    handler.cleanup_task(task_id);
                    return;
                }
            };
            let user_id = engine.user_id.to_string();
            crate::vfs::batch::VfsBatchExecutor::execute_move(
                engine,
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
    pub(super) async fn submit_batch_copy_impl(
        &self,
        src_paths: Vec<String>,
        dst_dir: String,
    ) -> VfsResult<String> {
        self.check_maintenance()?;
        let task_handler = self
            .task_handler
            .as_ref()
            .ok_or_else(|| VfsError::Internal("Task handler not configured".to_string()))?;
        let payload = serde_json::json!({ "src_paths": src_paths, "dst_dir": dst_dir });
        let task_id = task_handler
            .create_task(&self.user_id, "batch_copy", payload)
            .await
            .map_err(VfsError::Internal)?;
        let engine = Arc::new(self.clone_for_async());
        let handler = Arc::clone(task_handler);
        let vfs_cfg = crate::config::get_vfs_hub_config().await;
        let timeout = vfs_cfg.get_batch_operation().get_timeout_secs();
        Self::spawn_batch_task("batch_copy", async move {
            let _permit = match Self::acquire_batch_task_permit().await {
                Ok(permit) => permit,
                Err(err) => {
                    let _ = handler.fail_task(task_id, &err.to_string()).await;
                    handler.cleanup_task(task_id);
                    return;
                }
            };
            let user_id = engine.user_id.to_string();
            crate::vfs::batch::VfsBatchExecutor::execute_copy(
                engine,
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
    pub(super) async fn submit_batch_delete_impl(&self, paths: Vec<String>) -> VfsResult<String> {
        self.check_maintenance()?;
        for path in &paths {
            let normalized = self.validate_file_operation(path).await?;
            if self.is_protected_subdir_path(&normalized).await? {
                return Err(VfsError::Internal(
                    "Recycle bin is disabled for protected subdirectory storage".to_string(),
                ));
            }
        }
        let task_handler = self
            .task_handler
            .as_ref()
            .ok_or_else(|| VfsError::Internal("Task handler not configured".to_string()))?;
        let payload = serde_json::json!({ "paths": paths });
        let task_id = task_handler
            .create_task(&self.user_id, "batch_delete", payload)
            .await
            .map_err(VfsError::Internal)?;
        let engine = Arc::new(self.clone_for_async());
        let handler = Arc::clone(task_handler);
        let vfs_cfg = crate::config::get_vfs_hub_config().await;
        let timeout = vfs_cfg.get_batch_operation().get_timeout_secs();
        Self::spawn_batch_task("batch_delete", async move {
            let _permit = match Self::acquire_batch_task_permit().await {
                Ok(permit) => permit,
                Err(err) => {
                    let _ = handler.fail_task(task_id, &err.to_string()).await;
                    handler.cleanup_task(task_id);
                    return;
                }
            };
            let user_id = engine.user_id.to_string();
            crate::vfs::batch::VfsBatchExecutor::execute_delete(
                engine,
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
    pub(super) async fn submit_batch_compress_impl(
        &self,
        paths: Vec<String>,
        archive_name: String,
        options: crate::utils::CompressionOptions,
        delete_source: bool,
    ) -> VfsResult<String> {
        self.check_maintenance()?;
        let task_handler = self
            .task_handler
            .as_ref()
            .ok_or_else(|| VfsError::Internal("Task handler not configured".to_string()))?;
        let payload = serde_json::json!({
            "paths": paths,
            "archive_name": archive_name,
            "options": options,
            "delete_source": delete_source
        });
        let task_id = task_handler
            .create_task(&self.user_id, "batch_compress", payload)
            .await
            .map_err(VfsError::Internal)?;
        let engine = Arc::new(self.clone_for_async());
        let handler = Arc::clone(task_handler);
        let vfs_cfg = crate::config::get_vfs_hub_config().await;
        let timeout = vfs_cfg.get_batch_operation().get_timeout_secs();
        Self::spawn_batch_task("batch_compress", async move {
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
                if let Err(e) = engine.create_dir_impl(&temp_dir).await {
                    return Err(format!("Failed to create temp dir: {}", e));
                }
                let mut copied_count = 0;
                let mut failed_count = 0usize;
                let mut prepared_paths = Vec::new();
                let total_files = paths.len();
                for (idx, path) in paths.iter().enumerate() {
                    let logical = match engine.validate_file_operation(path).await {
                        Ok(p) => p,
                        Err(err) => {
                            failed_count += 1;
                            let error_msg = err.to_string();
                            let _ = handler
                                .log_batch_operation(crate::vfs::task::BatchOperationLog {
                                    task_id,
                                    user_id: engine.user_id.as_ref(),
                                    operation_type: "compress",
                                    file_path: path,
                                    target_path: Some(&archive_name),
                                    status: "failed",
                                    error_message: Some(&error_msg),
                                    file_size: None,
                                    execution_time_ms: None,
                                })
                                .await;
                            continue;
                        }
                    };
                    let Some(name) = std::path::Path::new(&logical).file_name() else {
                        failed_count += 1;
                        let error_msg = "Path is missing a file name".to_string();
                        let _ = handler
                            .log_batch_operation(crate::vfs::task::BatchOperationLog {
                                task_id,
                                user_id: engine.user_id.as_ref(),
                                operation_type: "compress",
                                file_path: path,
                                target_path: Some(&archive_name),
                                status: "failed",
                                error_message: Some(&error_msg),
                                file_size: None,
                                execution_time_ms: None,
                            })
                            .await;
                        yh_console_log::yhlog(
                            "warn",
                            &format!(
                                "Skip path without file name during batch compression: {}",
                                logical
                            ),
                        );
                        continue;
                    };
                    let name = name.to_string_lossy();
                    let staging_path = format!("{}/{}", temp_dir, name);
                    match engine.copy_file_impl(&logical, &staging_path).await {
                        Ok(_) => {
                            copied_count += 1;
                            prepared_paths.push(path.to_string());
                        }
                        Err(err) => {
                            failed_count += 1;
                            let error_msg = err.to_string();
                            let _ = handler
                                .log_batch_operation(crate::vfs::task::BatchOperationLog {
                                    task_id,
                                    user_id: engine.user_id.as_ref(),
                                    operation_type: "compress",
                                    file_path: path,
                                    target_path: Some(&archive_name),
                                    status: "failed",
                                    error_message: Some(&error_msg),
                                    file_size: None,
                                    execution_time_ms: None,
                                })
                                .await;
                        }
                    }
                    if (idx + 1).is_multiple_of(5) || idx + 1 == total_files {
                        let progress = ((idx + 1) as f32 / total_files.max(1) as f32 * 30.0) as i32;
                        let message = format!(
                            "Prepared {}/{} (Ready: {}, Failed: {})",
                            idx + 1,
                            total_files,
                            copied_count,
                            failed_count
                        );
                        let _ = handler
                            .update_task(task_id, progress, Some("preparing"), Some(&message))
                            .await;
                    }
                }
                if copied_count == 0 {
                    let _ = engine.delete_impl(&temp_dir).await;
                    return Err("No files were successfully prepared for compression".to_string());
                }
                let _ = handler
                    .update_progress(task_id, 35, Some("compressing"))
                    .await;
                use crate::utils::compression::compress_task;
                match compress_task(
                    engine.as_ref(),
                    &temp_dir,
                    &archive_name,
                    engine.user_id.as_ref(),
                    &options,
                    delete_source,
                )
                .await
                {
                    Ok(_) => {
                        for path in &prepared_paths {
                            let _ = handler
                                .log_batch_operation(crate::vfs::task::BatchOperationLog {
                                    task_id,
                                    user_id: engine.user_id.as_ref(),
                                    operation_type: "compress",
                                    file_path: path,
                                    target_path: Some(&archive_name),
                                    status: "success",
                                    error_message: None,
                                    file_size: None,
                                    execution_time_ms: None,
                                })
                                .await;
                        }
                        let _ = engine.delete_impl(&temp_dir).await;
                        if delete_source {
                            for p in &paths {
                                let _ = engine.delete_impl(p).await;
                            }
                        }
                        Ok(())
                    }
                    Err(e) => {
                        let error_msg = format!("Compression failed: {}", e);
                        for path in &prepared_paths {
                            let _ = handler
                                .log_batch_operation(crate::vfs::task::BatchOperationLog {
                                    task_id,
                                    user_id: engine.user_id.as_ref(),
                                    operation_type: "compress",
                                    file_path: path,
                                    target_path: Some(&archive_name),
                                    status: "failed",
                                    error_message: Some(&error_msg),
                                    file_size: None,
                                    execution_time_ms: None,
                                })
                                .await;
                        }
                        let _ = engine.delete_impl(&temp_dir).await;
                        Err(error_msg)
                    }
                }
            };
            match tokio::time::timeout(std::time::Duration::from_secs(timeout), task_future).await {
                Ok(Ok(_)) => {
                    let _ = handler.success_task(task_id).await;
                }
                Ok(Err(e)) => {
                    let _ = handler.fail_task(task_id, &e).await;
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
    pub(super) async fn submit_batch_decompress_impl(
        &self,
        paths: Vec<String>,
        output_dir: String,
        options: crate::utils::DecompressionOptions,
        delete_archive: bool,
    ) -> VfsResult<String> {
        self.check_maintenance()?;
        let task_handler = self
            .task_handler
            .as_ref()
            .ok_or_else(|| VfsError::Internal("Task handler not configured".to_string()))?;
        let payload = serde_json::json!({
            "paths": paths,
            "output_dir": output_dir,
            "options": options,
            "delete_archive": delete_archive
        });
        let task_id = task_handler
            .create_task(&self.user_id, "batch_decompress", payload)
            .await
            .map_err(VfsError::Internal)?;
        let engine = Arc::new(self.clone_for_async());
        let handler = Arc::clone(task_handler);
        let vfs_cfg = crate::config::get_vfs_hub_config().await;
        let timeout = vfs_cfg.get_batch_operation().get_timeout_secs();
        Self::spawn_batch_task("batch_decompress", async move {
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
                let mut success_count = 0usize;
                let mut failed_count = 0usize;
                for (idx, path_str) in paths.into_iter().enumerate() {
                    use crate::utils::compression::decompress_task;
                    match decompress_task(
                        engine.as_ref(),
                        &path_str,
                        &output_dir,
                        engine.user_id.as_ref(),
                        &options,
                        delete_archive,
                    )
                    .await
                    {
                        Ok(_) => {
                            success_count += 1;
                            let _ = handler
                                .log_batch_operation(crate::vfs::task::BatchOperationLog {
                                    task_id,
                                    user_id: engine.user_id.as_ref(),
                                    operation_type: "decompress",
                                    file_path: &path_str,
                                    target_path: Some(&output_dir),
                                    status: "success",
                                    error_message: None,
                                    file_size: None,
                                    execution_time_ms: None,
                                })
                                .await;
                        }
                        Err(e) => {
                            failed_count += 1;
                            let error_msg = e.to_string();
                            let _ = handler
                                .log_batch_operation(crate::vfs::task::BatchOperationLog {
                                    task_id,
                                    user_id: engine.user_id.as_ref(),
                                    operation_type: "decompress",
                                    file_path: &path_str,
                                    target_path: Some(&output_dir),
                                    status: "failed",
                                    error_message: Some(&error_msg),
                                    file_size: None,
                                    execution_time_ms: None,
                                })
                                .await;
                            yh_console_log::yhlog(
                                "error",
                                &format!(
                                    "Decompress task {} failed for file {}: {}",
                                    task_id, path_str, e
                                ),
                            );
                        }
                    }
                    if (idx + 1).is_multiple_of(5) || idx + 1 == total {
                        let progress = ((idx + 1) as f32 / total.max(1) as f32 * 100.0) as i32;
                        let message = format!(
                            "Processed {}/{} (Success: {}, Failed: {})",
                            idx + 1,
                            total,
                            success_count,
                            failed_count
                        );
                        let _ = handler
                            .update_task(task_id, progress, Some("running"), Some(&message))
                            .await;
                    }
                }
                failed_count
            };
            match tokio::time::timeout(std::time::Duration::from_secs(timeout), task_future).await {
                Ok(failed_count) => {
                    if failed_count == 0 {
                        let _ = handler.success_task(task_id).await;
                    } else {
                        let _ = handler
                            .fail_task(
                                task_id,
                                &format!("Decompression completed with {} failures", failed_count),
                            )
                            .await;
                    }
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
