use crate::business::services::FileIndexService;
use crate::utils::temp_file::{get_global_temp_manager_sync, VfsTempFileManager};
use crate::utils::VfsCache;
use crate::vfs::error::VfsError;
use crate::vfs::pool::VfsPool;
use crate::vfs::wal::VfsWalManager;
use crate::vfs::VfsResult;
use sea_orm::DatabaseConnection;
use std::sync::Arc;

/// User-scoped VFS engine.
pub struct ScopedVfsStorageEngine {
    pub(crate) user_id: Arc<str>,
    pub(crate) pool: Arc<VfsPool>,
    pub(crate) db: Arc<DatabaseConnection>,
    pub(crate) index_service: Arc<FileIndexService>,
    pub(crate) cache: Arc<VfsCache>,
    pub(crate) temp_manager: Arc<VfsTempFileManager>,
    pub(crate) journal_recorder: Option<Arc<dyn crate::vfs::VfsJournalRecorder>>,
    pub(crate) task_handler: Option<Arc<dyn crate::vfs::task::VfsTaskHandler>>,
    pub(crate) wal_manager: Option<Arc<VfsWalManager>>,
}
impl ScopedVfsStorageEngine {
    /// Create a new scoped storage engine.
    pub fn new(
        db: Arc<DatabaseConnection>,
        user_id: String,
        pool: Arc<VfsPool>,
        journal_recorder: Option<Arc<dyn crate::vfs::VfsJournalRecorder>>,
        task_handler: Option<Arc<dyn crate::vfs::task::VfsTaskHandler>>,
        wal_manager: Option<Arc<VfsWalManager>>,
    ) -> VfsResult<Self> {
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
    /// Clone for async tasks (Arc-only, O(1)).
    pub fn clone_for_async(&self) -> Self {
        Self {
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
