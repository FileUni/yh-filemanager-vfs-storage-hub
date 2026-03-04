// Scoped VFS Storage Engine Core Definition
use crate::business::services::FileIndexService;
use crate::utils::temp_file::{get_global_temp_manager_sync, VfsTempFileManager};
use crate::utils::VfsCache;
use crate::vfs::error::VfsError;
use crate::vfs::pool::VfsPool;
use crate::vfs::wal::VfsWalManager;
use crate::vfs::VfsResult;
use sea_orm::DatabaseConnection;
use std::sync::Arc;
/// Scoped VFS Storage Engine
/// Provides isolated VFS operations for a specific user
pub struct ScopedVfsStorageEngine {
    /// Unique identifier for the user
    pub(crate) user_id: Arc<str>,
    /// Physical storage pool
    pub(crate) pool: Arc<VfsPool>,
    /// Database connection handle
    pub(crate) db: Arc<DatabaseConnection>,
    /// File indexing service
    pub(crate) index_service: Arc<FileIndexService>,
    /// Path-level cache
    pub(crate) cache: Arc<VfsCache>,
    /// Temporary file manager
    pub(crate) temp_manager: Arc<VfsTempFileManager>,
    /// Activity recorder for auditing
    pub(crate) journal_recorder: Option<Arc<dyn crate::vfs::VfsJournalRecorder>>,
    /// Handler for background tasks
    pub(crate) task_handler: Option<Arc<dyn crate::vfs::task::VfsTaskHandler>>,
    /// Write-ahead log manager
    pub(crate) wal_manager: Option<Arc<VfsWalManager>>,
}
impl ScopedVfsStorageEngine {
    /// Create a new scoped storage engine instance
    pub fn new(
        db: Arc<DatabaseConnection>,
        user_id: String,
        pool: Arc<VfsPool>,
        journal_recorder: Option<Arc<dyn crate::vfs::VfsJournalRecorder>>,
        task_handler: Option<Arc<dyn crate::vfs::task::VfsTaskHandler>>,
        wal_manager: Option<Arc<VfsWalManager>>,
    ) -> VfsResult<Self> {
        // Clone Reason: Initial conversion from String to Arc<str> for the request lifecycle.
        let user_id_arc: Arc<str> = user_id.into();
        let index_service = Arc::new(FileIndexService::new(Arc::clone(&db)));
        let cache = Arc::new(VfsCache::new(user_id_arc.as_ref(), true));
        let temp_manager = get_global_temp_manager_sync().ok_or_else(|| {
            VfsError::Internal(
                "Temp manager is not initialized. Check [vfs_storage_hub.temp_file] config"
                    .to_string(),
            )
        })?;
        Ok(Self {
            db,
            user_id: user_id_arc,
            pool,
            index_service,
            cache,
            temp_manager,
            journal_recorder,
            task_handler,
            wal_manager,
        })
    }
    /// Clone self for async tasks (O(1) cheap clone)
    /// Arc
    /// Design constraint: this method clones only Arc handles without duplicating underlying resources to avoid memory amplification in background tasks
    pub fn clone_for_async(&self) -> Self {
        Self {
            // Clone Reason: Cloning Arc handles is O(1) and zero-allocation.
            user_id: Arc::clone(&self.user_id),
            pool: Arc::clone(&self.pool),
            db: Arc::clone(&self.db),
            index_service: Arc::clone(&self.index_service),
            cache: Arc::clone(&self.cache),
            temp_manager: Arc::clone(&self.temp_manager),
            journal_recorder: self.journal_recorder.as_ref().map(Arc::clone),
            task_handler: self.task_handler.as_ref().map(Arc::clone),
            wal_manager: self.wal_manager.as_ref().map(Arc::clone),
        }
    }
}
