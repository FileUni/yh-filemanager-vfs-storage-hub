use crate::config::VfsHubConfigGuard;
use crate::vfs::VfsStorageHub;
use sea_orm::DatabaseConnection;
use std::sync::Arc;
/// Global VFS instance management (optional)
pub struct VfsManager {
    pub hub: Arc<VfsStorageHub>,
    pub db: Arc<DatabaseConnection>,
}
impl VfsManager {
    pub async fn new(
        config: VfsHubConfigGuard,
        db: Arc<DatabaseConnection>,
    ) -> anyhow::Result<Self> {
        let hub = Arc::new(VfsStorageHub::new(config, Arc::clone(&db)).await?);
        Ok(Self { hub, db })
    }
}
