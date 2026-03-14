use crate::config::VfsHubConfigGuard;
use crate::vfs::VfsResult;
use crate::vfs::pool::VfsPool;
use once_cell::sync::OnceCell;
use opendal::Operator;
use sea_orm::entity::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
static VFS_STORAGE_HUB: OnceCell<Arc<VfsStorageHub>> = OnceCell::new();
pub fn get_vfs_storage_hub() -> Option<&'static Arc<VfsStorageHub>> {
    VFS_STORAGE_HUB.get()
}
pub struct VfsStorageHub {
    config: VfsHubConfigGuard,
    db: Arc<DatabaseConnection>,
    _operators: HashMap<String, Operator>,
    pools: HashMap<String, Arc<VfsPool>>,
    pub task_handler: Option<Arc<dyn crate::vfs::task::VfsTaskHandler>>,
    pub wal_manager: Option<Arc<crate::vfs::wal::VfsWalManager>>,
    /// User settings memory cache
    settings_cache: dashmap::DashMap<String, Arc<crate::business::entities::user_settings::Model>>,
}
impl VfsStorageHub {
    pub async fn new(config: VfsHubConfigGuard, db: Arc<DatabaseConnection>) -> VfsResult<Self> {
        yh_console_log::yhlog("info", "VFS Storage Hub: Starting initialization...");
        let connectors = config.get_connectors();
        if connectors.is_empty() {
            return Err(crate::vfs::error::VfsError::Internal(
                "No connectors configured (vfs_storage_hub.connectors is empty)".to_string(),
            ));
        }
        let mut operators = HashMap::with_capacity(connectors.len());
        // Build operators
        for connector in connectors {
            let op = crate::vfs::connector::build_operator(connector).await?;
            operators.insert(connector.get_name().to_string(), op);
        }
        let pools_cfg = config.get_pools();
        if pools_cfg.is_empty() {
            return Err(crate::vfs::error::VfsError::Internal(
                "No pools configured (vfs_storage_hub.pools is empty)".to_string(),
            ));
        }
        let mut pools = HashMap::with_capacity(pools_cfg.len());
        for pool_cfg in pools_cfg {
            let primary_connector_name = yh_config_infra::config_require_str!(
                pool_cfg.primary_connector,
                "vfs_storage_hub",
                "pool.primary_connector"
            );
            let primary = operators
                .get(primary_connector_name)
                .cloned()
                .ok_or_else(|| {
                    crate::vfs::error::VfsError::Internal(format!(
                        "Primary connector {} not found",
                        primary_connector_name
                    ))
                })?;
            let backup = if let Some(backup_name) = &pool_cfg.backup_connector {
                let backup_name_str: &str = backup_name.as_ref();
                if backup_name_str.trim().is_empty() {
                    None
                } else {
                    Some(operators.get(backup_name_str).cloned().ok_or_else(|| {
                        crate::vfs::error::VfsError::Internal(format!(
                            "Backup connector {} not found",
                            backup_name_str
                        ))
                    })?)
                }
            } else {
                None
            };
            let pool_name = pool_cfg.get_name();
            pools.insert(
                pool_name.to_string(),
                Arc::new(VfsPool::new(primary, backup, pool_cfg.to_owned())),
            );
        }
        Ok(Self {
            config,
            db,
            _operators: operators,
            pools,
            task_handler: None,
            wal_manager: None,
            settings_cache: dashmap::DashMap::new(),
        })
    }
    /// Get index service
    pub async fn get_index_service(
        &self,
    ) -> VfsResult<Arc<crate::business::services::FileIndexService>> {
        Ok(Arc::new(crate::business::services::FileIndexService::new(
            Arc::clone(&self.db),
        )))
    }
    pub fn set_task_handler(&mut self, handler: Arc<dyn crate::vfs::task::VfsTaskHandler>) {
        self.task_handler = Some(handler);
    }
    pub fn set_wal_manager(&mut self, manager: Arc<crate::vfs::wal::VfsWalManager>) {
        self.wal_manager = Some(manager);
    }
    /// Get database connection
    pub fn get_db(&self) -> Arc<DatabaseConnection> {
        Arc::clone(&self.db)
    }
    pub fn set_global_instance(hub: Arc<Self>) {
        let _ = VFS_STORAGE_HUB.set(hub);
    }
    pub fn get_pool(&self, pool_name: &str) -> Option<Arc<VfsPool>> {
        self.pools.get(pool_name).map(Arc::clone)
    }
    pub fn route_by_role(&self, role_id: &str) -> VfsResult<Arc<VfsPool>> {
        for policy in self.config.get_policies() {
            if policy.role_id.as_deref() == Some(role_id)
                && let Some(pool_name) = &policy.pool_name
                && let Some(pool) = self.pools.get(pool_name.as_ref())
            {
                return Ok(Arc::clone(pool));
            }
        }
        let default_pool_name = self.config.default_pool.as_ref().and_then(|s| {
            if s.trim().is_empty() {
                None
            } else {
                Some(s.as_ref())
            }
        });
        if let Some(pool_name) = default_pool_name
            && let Some(pool) = self.pools.get(pool_name)
        {
            return Ok(Arc::clone(pool));
        }
        self.pools.values().next().map(Arc::clone).ok_or_else(|| {
            crate::vfs::error::VfsError::Internal("No pools configured or initialized".to_string())
        })
    }
    pub async fn route_for_user(
        &self,
        db: &sea_orm::DatabaseConnection,
        user_id: &str,
        role_id: &str,
    ) -> VfsResult<Arc<VfsPool>> {
        use crate::business::entities::user_settings;
        if let Ok(Some(settings)) = user_settings::Entity::find_by_id(user_id).one(db).await
            && let Some(pool) = self.pools.get(&settings.pool_name)
        {
            return Ok(Arc::clone(pool));
        }
        self.route_by_role(role_id)
    }
    pub async fn ensure_user_settings(
        self: &Arc<Self>,
        db: &sea_orm::DatabaseConnection,
        user_id: &str,
        role_id: &str,
    ) -> VfsResult<Arc<crate::business::entities::user_settings::Model>> {
        // Try to get from cache
        if let Some(s) = self.settings_cache.get(user_id) {
            return Ok(Arc::clone(s.value()));
        }
        use crate::business::entities::user_settings;
        use crate::business::services::user_settings::UserSettingsService;
        let settings = if let Some(s) = user_settings::Entity::find_by_id(user_id).one(db).await? {
            s
        } else {
            let mut default_quota = 0;
            let mut has_role_quota = false;
            if let Ok(parsed_role_id) = role_id.parse::<i16>() {
                use sea_orm::ConnectionTrait;
                use sea_orm::sea_query::{Alias, Expr, Query};
                let backend = db.get_database_backend();
                let mut select = Query::select();
                select
                    .column((Alias::new("yh_roles"), Alias::new("default_storage_quota")))
                    .from(Alias::new("yh_roles"))
                    .and_where(
                        Expr::col((Alias::new("yh_roles"), Alias::new("role_id")))
                            .eq(parsed_role_id),
                    );
                if let Some(row) = db.query_one(backend.build(&select)).await?
                    && let Ok(quota) = row.try_get::<i64>("", "default_storage_quota")
                {
                    default_quota = quota;
                    has_role_quota = true;
                }
            }
            if !has_role_quota {
                for policy in self.config.get_policies() {
                    if policy.role_id.as_deref() == Some(role_id) {
                        default_quota = yh_config_infra::config_require_clone!(
                            policy.default_quota,
                            "vfs_storage_hub",
                            "policies.default_quota"
                        );
                        break;
                    }
                }
            }
            let pool = self.route_by_role(role_id)?;
            let pool_name = pool.config.get_name();
            let base_dir = format!("/users/{}", user_id);
            UserSettingsService::upsert_user_settings(
                db,
                user_id,
                pool_name,
                &base_dir,
                default_quota,
            )
            .await
            .map_err(|e| {
                crate::vfs::error::VfsError::Internal(format!(
                    "Failed to create default user settings: {}",
                    e
                ))
            })?
        };
        // Write back to cache
        let settings = Arc::new(settings);
        self.settings_cache
            .insert(user_id.to_string(), Arc::clone(&settings));
        Ok(settings)
    }
    pub async fn create_scoped_engine(
        self: &Arc<Self>,
        db: Arc<sea_orm::DatabaseConnection>,
        user_id: &str,
        role_id: &str,
        journal_recorder: Option<Arc<dyn crate::vfs::VfsJournalRecorder>>,
    ) -> VfsResult<Arc<crate::vfs::scoped::ScopedVfsStorageEngine>> {
        let settings = self
            .ensure_user_settings(db.as_ref(), user_id, role_id)
            .await?;
        self.create_scoped_engine_with_pool(
            db,
            user_id,
            role_id,
            &settings.pool_name,
            journal_recorder,
        )
        .await
    }
    /// Create engine directly with a specific pool name (zero DB hits)
    pub async fn create_scoped_engine_with_pool(
        self: &Arc<Self>,
        db: Arc<sea_orm::DatabaseConnection>,
        user_id: &str,
        role_id: &str,
        pool_name: &str,
        journal_recorder: Option<Arc<dyn crate::vfs::VfsJournalRecorder>>,
    ) -> VfsResult<Arc<crate::vfs::scoped::ScopedVfsStorageEngine>> {
        let pool = if let Some(pool) = self.pools.get(pool_name) {
            Arc::clone(pool)
        } else {
            self.route_by_role(role_id)?
        };
        let task_handler = self.task_handler.as_ref().map(Arc::clone);
        let wal_manager = self.wal_manager.as_ref().map(Arc::clone);
        let engine = crate::vfs::scoped::ScopedVfsStorageEngine::new(
            db,
            user_id.to_string(),
            pool,
            journal_recorder,
            task_handler,
            wal_manager,
        )?;
        Ok(Arc::new(engine))
    }
    pub async fn cleanup_all_s3_multiparts(&self, max_age_secs: u64) -> usize {
        let mut total_cleaned = 0;
        // Cleanup based on KV state
        if let Ok(keys) = yh_fast_kv_storage_hub::api::helpers::scan_keys("s3:multipart:").await {
            for key in keys {
                if let Ok(Some(state_val)) =
                    yh_fast_kv_storage_hub::api::helpers::get_json::<serde_json::Value>(&key).await
                {
                    let created_at_str = state_val.get("created_at").and_then(|v| v.as_str());
                    let is_expired = if let Some(s) = created_at_str {
                        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                            (chrono::Utc::now() - dt.with_timezone(&chrono::Utc)).num_seconds()
                                > max_age_secs as i64
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    if is_expired {
                        //()
                        let _ = yh_fast_kv_storage_hub::api::helpers::del(&key).await;
                        total_cleaned += 1;
                    }
                }
            }
        }
        total_cleaned
    }
}
