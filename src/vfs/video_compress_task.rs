use crate::utils::video_compression::{
    VideoCompressionOptions, collect_video_file_paths, compress_vfs_video_to_vfs,
    resolve_output_path,
};
use crate::vfs::VfsStorage;
use crate::vfs::task::{BatchOperationLog, VfsTaskHandler};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{Duration, Instant, timeout};
use uuid::Uuid;
use yh_console_log::yhlog;

static VIDEO_COMPRESS_TASK_SEMAPHORE: once_cell::sync::OnceCell<Arc<Semaphore>> =
    once_cell::sync::OnceCell::new();

pub(crate) async fn acquire_video_compress_task_permit() -> Result<OwnedSemaphorePermit, String> {
    let semaphore = if let Some(existing) = VIDEO_COMPRESS_TASK_SEMAPHORE.get() {
        Arc::clone(existing)
    } else {
        let cfg = crate::config::get_vfs_hub_config().await;
        let permits = cfg
            .get_media_transcoding()
            .get_effective_max_concurrent_tasks()
            .max(1);
        let created = Arc::new(Semaphore::new(permits));
        let _ = VIDEO_COMPRESS_TASK_SEMAPHORE.set(Arc::clone(&created));
        created
    };
    semaphore
        .acquire_owned()
        .await
        .map_err(|error| format!("Acquire video compression permit failed: {}", error))
}

pub(crate) async fn execute_batch_video_compress(
    storage: Arc<dyn VfsStorage>,
    task_handler: Arc<dyn VfsTaskHandler>,
    task_id: Uuid,
    input_paths: Vec<String>,
    options: VideoCompressionOptions,
    timeout_secs: u64,
    user_id: &str,
) {
    let task_future = async {
        let _ = task_handler
            .update_task(
                task_id,
                0,
                Some("running"),
                Some("Scanning selected paths for video files"),
            )
            .await;

        let output_suffix = options.normalized_suffix();
        let source_paths = collect_video_file_paths(
            storage.as_ref(),
            &input_paths,
            options.include_subdirectories,
            &output_suffix,
        )
        .await
        .map_err(|error| error.to_string())?;

        if source_paths.is_empty() {
            return Err("No video files were found in the selected paths".to_string());
        }

        let total = source_paths.len();
        let mut success_count = 0usize;
        let mut failed_count = 0usize;

        for (idx, source_path) in source_paths.iter().enumerate() {
            if task_handler.is_cancelled(task_id) {
                return Err("Video compression task cancelled by user".to_string());
            }

            let output_path = resolve_output_path(storage.as_ref(), source_path, &options)
                .await
                .map_err(|error| error.to_string())?;
            let start_time = Instant::now();

            match compress_vfs_video_to_vfs(
                storage.as_ref(),
                user_id,
                source_path,
                &output_path,
                &options,
            )
            .await
            {
                Ok(()) => {
                    success_count += 1;
                    if options.delete_source {
                        let _ = storage.delete(source_path).await;
                    }
                    let _ = task_handler
                        .log_batch_operation(BatchOperationLog {
                            task_id,
                            user_id,
                            operation_type: "video_compress",
                            file_path: source_path,
                            target_path: Some(&output_path),
                            status: "success",
                            error_message: None,
                            file_size: None,
                            execution_time_ms: Some(start_time.elapsed().as_millis() as i64),
                        })
                        .await;
                }
                Err(error) => {
                    failed_count += 1;
                    let error_message = error.to_string();
                    yhlog(
                        "error",
                        &format!(
                            "Task {} video compression failed for {}: {}",
                            task_id, source_path, error_message
                        ),
                    );
                    let _ = task_handler
                        .log_batch_operation(BatchOperationLog {
                            task_id,
                            user_id,
                            operation_type: "video_compress",
                            file_path: source_path,
                            target_path: Some(&output_path),
                            status: "failed",
                            error_message: Some(&error_message),
                            file_size: None,
                            execution_time_ms: Some(start_time.elapsed().as_millis() as i64),
                        })
                        .await;
                }
            }

            let progress = 5 + (((idx + 1) as f32 / total as f32) * 95.0) as i32;
            let message = format!(
                "Processed {}/{} videos (Success: {}, Failed: {})",
                idx + 1,
                total,
                success_count,
                failed_count
            );
            let _ = task_handler
                .update_task(task_id, progress, Some("running"), Some(&message))
                .await;
        }

        if failed_count == 0 {
            Ok(())
        } else {
            Err(format!(
                "Video compression completed with {} failures",
                failed_count
            ))
        }
    };

    match timeout(Duration::from_secs(timeout_secs), task_future).await {
        Ok(Ok(())) => {
            let _ = task_handler.success_task(task_id).await;
        }
        Ok(Err(error)) => {
            let _ = task_handler.fail_task(task_id, &error).await;
        }
        Err(_) => {
            let _ = task_handler
                .fail_task(task_id, "Video compression task timed out")
                .await;
        }
    }
}
