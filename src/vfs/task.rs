use async_trait::async_trait;
use uuid::Uuid;
#[derive(Debug, Clone)]
pub struct BatchOperationLog<'a> {
    pub task_id: Uuid,
    pub user_id: &'a str,
    pub operation_type: &'a str,
    pub file_path: &'a str,
    pub target_path: Option<&'a str>,
    pub status: &'a str,
    pub error_message: Option<&'a str>,
    pub file_size: Option<i64>,
    pub execution_time_ms: Option<i64>,
}
#[async_trait]
pub trait VfsTaskHandler: Send + Sync {
    async fn create_task(
        &self,
        user_id: &str,
        task_type: &str,
        payload: serde_json::Value,
    ) -> Result<Uuid, String>;
    async fn update_progress(
        &self,
        id: Uuid,
        progress: i32,
        status: Option<&str>,
    ) -> Result<(), String>;
    async fn update_task(
        &self,
        id: Uuid,
        progress: i32,
        status: Option<&str>,
        message: Option<&str>,
    ) -> Result<(), String>;
    async fn fail_task(&self, id: Uuid, error: &str) -> Result<(), String>;
    async fn success_task(&self, id: Uuid) -> Result<(), String>;

    /// Check if task is cancelled
    fn is_cancelled(&self, id: Uuid) -> bool;
    /// Cleanup task resources (like cancellation tokens)
    fn cleanup_task(&self, id: Uuid);
    /// Get Any reference for downcasting
    fn as_any(&self) -> &dyn std::any::Any;
    /// Log batch operation
    ///
    ///
    /// Records the execution result of a single file in batch operations, including file path,
    /// target path, status, error message, etc. This method is used to track detailed execution
    /// information of batch operations, helping users understand specific issues and failure reasons.
    async fn log_batch_operation(&self, log: BatchOperationLog<'_>) -> Result<(), String>;
}
