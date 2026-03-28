//! Batch operation executor (progress/logging/timeout).
use crate::vfs::VfsStorage;
use crate::vfs::task::{BatchOperationLog, VfsTaskHandler};
use futures::StreamExt;
use std::collections::HashSet;
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

#[inline]
fn split_stem_and_ext(file_name: &str) -> (&str, &str) {
    match file_name.rsplit_once('.') {
        Some((stem, ext)) if !stem.is_empty() => (stem, ext),
        _ => (file_name, ""),
    }
}

fn reserve_batch_destination(candidate: String, reserved: &mut HashSet<String>) -> String {
    if reserved.insert(candidate.clone()) {
        return candidate;
    }
    let (dir, file_name) = candidate
        .rsplit_once('/')
        .map(|(dir, file)| (dir.to_string(), file.to_string()))
        .unwrap_or_else(|| (String::new(), candidate.clone()));
    let (stem, ext) = split_stem_and_ext(&file_name);
    for idx in 1..=10_000 {
        let next_file_name = if ext.is_empty() {
            format!("{} ({})", stem, idx)
        } else {
            format!("{} ({}).{}", stem, idx, ext)
        };
        let next = if dir.is_empty() {
            next_file_name
        } else {
            format!("{}/{}", dir, next_file_name)
        };
        if reserved.insert(next.clone()) {
            return next;
        }
    }
    candidate
}

async fn batch_concurrency() -> usize {
    crate::config::get_vfs_hub_config()
        .await
        .get_batch_operation()
        .get_effective_max_concurrent_tasks()
        .max(1)
}

async fn prepare_copy_like_targets(
    storage: &Arc<dyn VfsStorage>,
    src_paths: &[String],
    dst_dir: &str,
) -> Vec<(String, String)> {
    let mut reserved = HashSet::new();
    let mut plans = Vec::with_capacity(src_paths.len());
    for src in src_paths {
        let filename = file_name_from_path(src);
        let base_dst = format!("{}/{}", dst_dir.trim_end_matches('/'), filename);
        let candidate = storage.get_unique_path(&base_dst).await.unwrap_or(base_dst);
        let dst = reserve_batch_destination(candidate, &mut reserved);
        plans.push((src.clone(), dst));
    }
    plans
}

async fn update_progress(
    task_handler: &Arc<dyn VfsTaskHandler>,
    task_id: Uuid,
    processed: usize,
    total: usize,
    success_count: usize,
    failed_count: usize,
) {
    if total == 0 {
        return;
    }
    let progress = (processed as f32 / total as f32 * 100.0) as i32;
    let message = format!(
        "Processed {}/{} (Success: {}, Failed: {})",
        processed, total, success_count, failed_count
    );
    let _ = task_handler
        .update_task(task_id, progress, Some("running"), Some(&message))
        .await;
}

pub struct VfsBatchExecutor;
impl VfsBatchExecutor {
    /// Execute batch move task
    pub async fn execute_move(
        storage: Arc<dyn VfsStorage>,
        task_handler: Arc<dyn VfsTaskHandler>,
        task_id: Uuid,
        src_paths: Vec<String>,
        dst_dir: String,
        timeout_secs: u64,
        user_id: &str,
    ) {
        let task_future = async {
            let mut success_count = 0usize;
            let mut failed_count = 0usize;
            let total = src_paths.len();
            let concurrency = batch_concurrency().await;
            let plans = prepare_copy_like_targets(&storage, &src_paths, &dst_dir).await;
            let mut stream = futures::stream::iter(plans.into_iter().map(|(src, dst)| {
                let storage = Arc::clone(&storage);
                let task_handler = Arc::clone(&task_handler);
                async move {
                    if task_handler.is_cancelled(task_id) {
                        return (src, dst, 0_i64, Ok::<Option<()>, String>(None));
                    }
                    let start_time = Instant::now();
                    let result = storage.move_file(&src, &dst).await;
                    let execution_time = start_time.elapsed().as_millis() as i64;
                    match result {
                        Ok(_) => (src, dst, execution_time, Ok(Some(()))),
                        Err(err) => (src, dst, execution_time, Err(err.to_string())),
                    }
                }
            }))
            .buffer_unordered(concurrency);

            let mut processed = 0usize;
            while let Some((src, dst, execution_time, outcome)) = stream.next().await {
                if task_handler.is_cancelled(task_id) {
                    break;
                }
                match outcome {
                    Ok(Some(())) => {
                        success_count += 1;
                        let _ = task_handler
                            .log_batch_operation(BatchOperationLog {
                                task_id,
                                user_id,
                                operation_type: "move",
                                file_path: &src,
                                target_path: Some(&dst),
                                status: "success",
                                error_message: None,
                                file_size: None,
                                execution_time_ms: Some(execution_time),
                            })
                            .await;
                    }
                    Ok(None) => break,
                    Err(error_msg) => {
                        failed_count += 1;
                        yhlog(
                            "error",
                            &format!("Task {} move failed: {}", task_id, error_msg),
                        );
                        let _ = task_handler
                            .log_batch_operation(BatchOperationLog {
                                task_id,
                                user_id,
                                operation_type: "move",
                                file_path: &src,
                                target_path: Some(&dst),
                                status: "failed",
                                error_message: Some(&error_msg),
                                file_size: None,
                                execution_time_ms: Some(execution_time),
                            })
                            .await;
                    }
                }
                processed += 1;
                if processed % 5 == 0 || processed == total {
                    update_progress(
                        &task_handler,
                        task_id,
                        processed,
                        total,
                        success_count,
                        failed_count,
                    )
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
        storage: Arc<dyn VfsStorage>,
        task_handler: Arc<dyn VfsTaskHandler>,
        task_id: Uuid,
        src_paths: Vec<String>,
        dst_dir: String,
        timeout_secs: u64,
        user_id: &str,
    ) {
        let task_future = async {
            let mut success_count = 0usize;
            let mut failed_count = 0usize;
            let total = src_paths.len();
            let concurrency = batch_concurrency().await;
            let plans = prepare_copy_like_targets(&storage, &src_paths, &dst_dir).await;
            let mut stream = futures::stream::iter(plans.into_iter().map(|(src, dst)| {
                let storage = Arc::clone(&storage);
                let task_handler = Arc::clone(&task_handler);
                async move {
                    if task_handler.is_cancelled(task_id) {
                        return (src, dst, 0_i64, Ok::<Option<()>, String>(None));
                    }
                    let start_time = Instant::now();
                    let result = storage.copy_file(&src, &dst).await;
                    let execution_time = start_time.elapsed().as_millis() as i64;
                    match result {
                        Ok(_) => (src, dst, execution_time, Ok(Some(()))),
                        Err(err) => (src, dst, execution_time, Err(err.to_string())),
                    }
                }
            }))
            .buffer_unordered(concurrency);

            let mut processed = 0usize;
            while let Some((src, dst, execution_time, outcome)) = stream.next().await {
                if task_handler.is_cancelled(task_id) {
                    break;
                }
                match outcome {
                    Ok(Some(())) => {
                        success_count += 1;
                        let _ = task_handler
                            .log_batch_operation(BatchOperationLog {
                                task_id,
                                user_id,
                                operation_type: "copy",
                                file_path: &src,
                                target_path: Some(&dst),
                                status: "success",
                                error_message: None,
                                file_size: None,
                                execution_time_ms: Some(execution_time),
                            })
                            .await;
                    }
                    Ok(None) => break,
                    Err(error_msg) => {
                        failed_count += 1;
                        yhlog(
                            "error",
                            &format!("Task {} copy failed: {}", task_id, error_msg),
                        );
                        let _ = task_handler
                            .log_batch_operation(BatchOperationLog {
                                task_id,
                                user_id,
                                operation_type: "copy",
                                file_path: &src,
                                target_path: Some(&dst),
                                status: "failed",
                                error_message: Some(&error_msg),
                                file_size: None,
                                execution_time_ms: Some(execution_time),
                            })
                            .await;
                    }
                }
                processed += 1;
                if processed % 5 == 0 || processed == total {
                    update_progress(
                        &task_handler,
                        task_id,
                        processed,
                        total,
                        success_count,
                        failed_count,
                    )
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
        storage: Arc<dyn VfsStorage>,
        task_handler: Arc<dyn VfsTaskHandler>,
        task_id: Uuid,
        paths: Vec<String>,
        timeout_secs: u64,
        user_id: &str,
    ) {
        let task_future = async {
            let mut success_count = 0usize;
            let mut failed_count = 0usize;
            let total = paths.len();
            let concurrency = batch_concurrency().await;
            let mut stream = futures::stream::iter(paths.into_iter().map(|path| {
                let storage = Arc::clone(&storage);
                let task_handler = Arc::clone(&task_handler);
                async move {
                    if task_handler.is_cancelled(task_id) {
                        return (path, 0_i64, Ok::<Option<()>, String>(None));
                    }
                    let start_time = Instant::now();
                    let result = storage.move_to_trash(&path).await;
                    let execution_time = start_time.elapsed().as_millis() as i64;
                    match result {
                        Ok(_) => (path, execution_time, Ok(Some(()))),
                        Err(err) => (path, execution_time, Err(err.to_string())),
                    }
                }
            }))
            .buffer_unordered(concurrency);

            let mut processed = 0usize;
            while let Some((path, execution_time, outcome)) = stream.next().await {
                if task_handler.is_cancelled(task_id) {
                    break;
                }
                match outcome {
                    Ok(Some(())) => {
                        success_count += 1;
                        let _ = task_handler
                            .log_batch_operation(BatchOperationLog {
                                task_id,
                                user_id,
                                operation_type: "delete",
                                file_path: &path,
                                target_path: None,
                                status: "success",
                                error_message: None,
                                file_size: None,
                                execution_time_ms: Some(execution_time),
                            })
                            .await;
                    }
                    Ok(None) => break,
                    Err(error_msg) => {
                        failed_count += 1;
                        yhlog(
                            "error",
                            &format!("Task {} delete failed: {}", task_id, error_msg),
                        );
                        let _ = task_handler
                            .log_batch_operation(BatchOperationLog {
                                task_id,
                                user_id,
                                operation_type: "delete",
                                file_path: &path,
                                target_path: None,
                                status: "failed",
                                error_message: Some(&error_msg),
                                file_size: None,
                                execution_time_ms: Some(execution_time),
                            })
                            .await;
                    }
                }
                processed += 1;
                if processed % 5 == 0 || processed == total {
                    update_progress(
                        &task_handler,
                        task_id,
                        processed,
                        total,
                        success_count,
                        failed_count,
                    )
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
