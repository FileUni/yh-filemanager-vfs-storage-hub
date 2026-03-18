//! Batch operation executor (progress/logging/timeout).
use crate::vfs::task::{BatchOperationLog, VfsTaskHandler};
use crate::vfs::{ScopedVfsStorageEngine, VfsStorage};
use std::sync::Arc;
use tokio::time::{Duration, Instant, timeout};
use uuid::Uuid;
use yh_console_log::yhlog;

#[inline]
fn file_name_from_path(path: &str) -> &str {
    match path.rsplit('/').next() {
        Some(name) => name,
        None => path,
    }
}

pub struct VfsBatchExecutor;
impl VfsBatchExecutor {
    /// Execute batch move task
    pub async fn execute_move(
        engine: Arc<ScopedVfsStorageEngine>,
        task_handler: Arc<dyn VfsTaskHandler>,
        task_id: Uuid,
        src_paths: Vec<String>,
        dst_dir: String,
        timeout_secs: u64,
        user_id: &str,
    ) {
        let task_future = async {
            let mut success_count = 0;
            let mut failed_count = 0;
            let total = src_paths.len();
            for (idx, src) in src_paths.iter().enumerate() {
                // Check for cancellation
                if task_handler.is_cancelled(task_id) {
                    return (success_count, failed_count);
                }
                let filename = file_name_from_path(src);
                let mut dst = format!("{}/{}", dst_dir.trim_end_matches('/'), filename);
                let start_time = Instant::now();
                // Auto-rename logic
                if let Ok(unique_dst) = engine.get_unique_path(&dst).await {
                    dst = unique_dst;
                }
                let result = engine.move_file(src, &dst).await;
                let execution_time = start_time.elapsed().as_millis() as i64;
                match result {
                    Ok(_) => {
                        success_count += 1;
                        let _ = task_handler
                            .log_batch_operation(BatchOperationLog {
                                task_id,
                                user_id,
                                operation_type: "move",
                                file_path: src,
                                target_path: Some(&dst),
                                status: "success",
                                error_message: None,
                                file_size: None,
                                execution_time_ms: Some(execution_time),
                            })
                            .await;
                    }
                    Err(e) => {
                        failed_count += 1;
                        yhlog("error", &format!("Task {} move failed: {}", task_id, e));
                        let error_msg = e.to_string();
                        let _ = task_handler
                            .log_batch_operation(BatchOperationLog {
                                task_id,
                                user_id,
                                operation_type: "move",
                                file_path: src,
                                target_path: Some(&dst),
                                status: "failed",
                                error_message: Some(&error_msg),
                                file_size: None,
                                execution_time_ms: Some(execution_time),
                            })
                            .await;
                    }
                }
                if (idx + 1) % 5 == 0 || idx == total - 1 {
                    let progress = ((idx + 1) as f32 / total as f32 * 100.0) as i32;
                    let message = format!(
                        "Processed {}/{} (Success: {}, Failed: {})",
                        idx + 1,
                        total,
                        success_count,
                        failed_count
                    );
                    let _ = task_handler
                        .update_task(task_id, progress, Some("running"), Some(&message))
                        .await;
                }
            }
            (success_count, failed_count)
        };
        match timeout(Duration::from_secs(timeout_secs), task_future).await {
            Ok((success_count, failed_count)) => {
                if failed_count == 0 {
                    let _ = task_handler.success_task(task_id).await;
                } else {
                    let _ = task_handler
                        .fail_task(
                            task_id,
                            &format!(
                                "Completed with failures: {} success, {} failed",
                                success_count, failed_count
                            ),
                        )
                        .await;
                }
            }
            Err(_) => {
                let _ = task_handler.fail_task(task_id, "Task timed out").await;
            }
        }
    }
    /// Execute batch copy task
    pub async fn execute_copy(
        engine: Arc<ScopedVfsStorageEngine>,
        task_handler: Arc<dyn VfsTaskHandler>,
        task_id: Uuid,
        src_paths: Vec<String>,
        dst_dir: String,
        timeout_secs: u64,
        user_id: &str,
    ) {
        let task_future = async {
            let mut success_count = 0;
            let mut failed_count = 0;
            let total = src_paths.len();
            for (idx, src) in src_paths.iter().enumerate() {
                // Check for cancellation
                if task_handler.is_cancelled(task_id) {
                    return (success_count, failed_count);
                }
                let filename = file_name_from_path(src);
                let mut dst = format!("{}/{}", dst_dir.trim_end_matches('/'), filename);
                let start_time = Instant::now();
                // Auto-rename logic
                if let Ok(unique_dst) = engine.get_unique_path(&dst).await {
                    dst = unique_dst;
                }
                let result = engine.copy_file(src, &dst).await;
                let execution_time = start_time.elapsed().as_millis() as i64;
                match result {
                    Ok(_) => {
                        success_count += 1;
                        let _ = task_handler
                            .log_batch_operation(BatchOperationLog {
                                task_id,
                                user_id,
                                operation_type: "copy",
                                file_path: src,
                                target_path: Some(&dst),
                                status: "success",
                                error_message: None,
                                file_size: None,
                                execution_time_ms: Some(execution_time),
                            })
                            .await;
                    }
                    Err(e) => {
                        failed_count += 1;
                        yhlog("error", &format!("Task {} copy failed: {}", task_id, e));
                        let error_msg = e.to_string();
                        let _ = task_handler
                            .log_batch_operation(BatchOperationLog {
                                task_id,
                                user_id,
                                operation_type: "copy",
                                file_path: src,
                                target_path: Some(&dst),
                                status: "failed",
                                error_message: Some(&error_msg),
                                file_size: None,
                                execution_time_ms: Some(execution_time),
                            })
                            .await;
                    }
                }
                if (idx + 1) % 5 == 0 || idx == total - 1 {
                    let progress = ((idx + 1) as f32 / total as f32 * 100.0) as i32;
                    let message = format!(
                        "Processed {}/{} (Success: {}, Failed: {})",
                        idx + 1,
                        total,
                        success_count,
                        failed_count
                    );
                    let _ = task_handler
                        .update_task(task_id, progress, Some("running"), Some(&message))
                        .await;
                }
            }
            (success_count, failed_count)
        };
        match timeout(Duration::from_secs(timeout_secs), task_future).await {
            Ok((success_count, failed_count)) => {
                if failed_count == 0 {
                    let _ = task_handler.success_task(task_id).await;
                } else {
                    let _ = task_handler
                        .fail_task(
                            task_id,
                            &format!(
                                "Completed with failures: {} success, {} failed",
                                success_count, failed_count
                            ),
                        )
                        .await;
                }
            }
            Err(_) => {
                let _ = task_handler.fail_task(task_id, "Task timed out").await;
            }
        }
    }
    /// Execute batch delete task
    pub async fn execute_delete(
        engine: Arc<ScopedVfsStorageEngine>,
        task_handler: Arc<dyn VfsTaskHandler>,
        task_id: Uuid,
        paths: Vec<String>,
        timeout_secs: u64,
        user_id: &str,
    ) {
        let task_future = async {
            let mut success_count = 0;
            let mut failed_count = 0;
            let total = paths.len();
            for (idx, path) in paths.iter().enumerate() {
                // Check for cancellation
                if task_handler.is_cancelled(task_id) {
                    return (success_count, failed_count);
                }
                let start_time = Instant::now();
                let result = engine.move_to_trash(path).await;
                let execution_time = start_time.elapsed().as_millis() as i64;
                match result {
                    Ok(_) => {
                        success_count += 1;
                        let _ = task_handler
                            .log_batch_operation(BatchOperationLog {
                                task_id,
                                user_id,
                                operation_type: "delete",
                                file_path: path,
                                target_path: None,
                                status: "success",
                                error_message: None,
                                file_size: None,
                                execution_time_ms: Some(execution_time),
                            })
                            .await;
                    }
                    Err(e) => {
                        failed_count += 1;
                        yhlog("error", &format!("Task {} delete failed: {}", task_id, e));
                        let error_msg = e.to_string();
                        let _ = task_handler
                            .log_batch_operation(BatchOperationLog {
                                task_id,
                                user_id,
                                operation_type: "delete",
                                file_path: path,
                                target_path: None,
                                status: "failed",
                                error_message: Some(&error_msg),
                                file_size: None,
                                execution_time_ms: Some(execution_time),
                            })
                            .await;
                    }
                }
                if (idx + 1) % 5 == 0 || idx == total - 1 {
                    let progress = ((idx + 1) as f32 / total as f32 * 100.0) as i32;
                    let message = format!(
                        "Processed {}/{} (Success: {}, Failed: {})",
                        idx + 1,
                        total,
                        success_count,
                        failed_count
                    );
                    let _ = task_handler
                        .update_task(task_id, progress, Some("running"), Some(&message))
                        .await;
                }
            }
            (success_count, failed_count)
        };
        match timeout(Duration::from_secs(timeout_secs), task_future).await {
            Ok((success_count, failed_count)) => {
                if failed_count == 0 {
                    let _ = task_handler.success_task(task_id).await;
                } else {
                    let _ = task_handler
                        .fail_task(
                            task_id,
                            &format!(
                                "Completed with failures: {} success, {} failed",
                                success_count, failed_count
                            ),
                        )
                        .await;
                }
            }
            Err(_) => {
                let _ = task_handler.fail_task(task_id, "Task timed out").await;
            }
        }
    }
}
