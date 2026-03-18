//! Write-ahead log (WAL).
use crate::vfs::{VfsStorage, VfsStorageHub};
use sea_orm::sea_query::Index;
use sea_orm::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
pub mod entity;
/// Write-Ahead Log Operation Types
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalOperation {
    Write {
        path: String,
        size: u64,
    },
    Delete {
        path: String,
    },
    Move {
        src: String,
        dst: String,
    },
    Rename {
        old_path: String,
        new_path: String,
    },
    CreateDir {
        path: String,
    },
    RestoreTrash {
        trash_path: String,
        original_path: String,
    },
}
impl WalOperation {
    pub fn as_str(&self) -> &str {
        match self {
            WalOperation::Write { .. } => "WRITE",
            WalOperation::Delete { .. } => "DELETE",
            WalOperation::Move { .. } => "MOVE",
            WalOperation::Rename { .. } => "RENAME",
            WalOperation::CreateDir { .. } => "CREATE_DIR",
            WalOperation::RestoreTrash { .. } => "RESTORE_TRASH",
        }
    }
}
/// WAL Recovery Result
#[derive(Debug, Clone)]
pub struct WalRecoveryResult {
    pub recovered: usize,
    pub failed: usize,
    pub errors: Vec<String>,
}
/// Write-Ahead Log Manager
pub struct VfsWalManager {
    db: Arc<DatabaseConnection>,
}
impl VfsWalManager {
    pub fn new(db: Arc<DatabaseConnection>) -> Self {
        Self { db }
    }
    /// Initialize WAL table structure
    pub async fn init_tables(&self) -> Result<(), DbErr> {
        let backend = self.db.get_database_backend();
        let schema = Schema::new(backend);
        let stmt = schema
            .create_table_from_entity(entity::Entity)
            .if_not_exists()
            .to_owned();
        self.db.execute(backend.build(&stmt)).await?;

        let idx_user = Index::create()
            .name("idx_vfs_wal_user")
            .table(entity::Entity)
            .col(entity::Column::UserId)
            .if_not_exists()
            .to_owned();
        let idx_type = Index::create()
            .name("idx_vfs_wal_type")
            .table(entity::Entity)
            .col(entity::Column::OperationType)
            .if_not_exists()
            .to_owned();

        self.db.execute(backend.build(&idx_user)).await?;
        self.db.execute(backend.build(&idx_type)).await?;
        Ok(())
    }
    pub async fn log_operation(
        &self,
        user_id: &str,
        operation: WalOperation,
    ) -> Result<i64, DbErr> {
        let op_json =
            serde_json::to_string(&operation).map_err(|e| DbErr::Custom(e.to_string()))?;
        let active_model = entity::ActiveModel {
            user_id: Set(user_id.to_string()),
            operation_type: Set(operation.as_str().to_string()),
            operation_data: Set(op_json),
            created_at: Set(chrono::Utc::now().into()),
            ..Default::default()
        };
        let res = entity::Entity::insert(active_model).exec(&*self.db).await?;
        Ok(res.last_insert_id)
    }
    pub async fn complete_operation(&self, log_id: i64) -> Result<(), DbErr> {
        entity::Entity::delete_by_id(log_id).exec(&*self.db).await?;
        Ok(())
    }
    /// WAL ()
    pub async fn revoke_all(&self) -> Result<u64, DbErr> {
        let res = entity::Entity::delete_many().exec(&*self.db).await?;
        Ok(res.rows_affected)
    }
    pub async fn recover_all(&self, hub: Arc<VfsStorageHub>) -> Result<WalRecoveryResult, DbErr> {
        let rows = entity::Entity::find()
            .order_by_asc(entity::Column::Id)
            .all(&*self.db)
            .await?;
        if rows.is_empty() {
            return Ok(WalRecoveryResult {
                recovered: 0,
                failed: 0,
                errors: vec![],
            });
        }
        // Identify and lock affected users
        let mut affected_users: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for row in &rows {
            affected_users.insert(row.user_id.as_str());
        }
        let mut _guards = Vec::new();
        for uid in &affected_users {
            _guards.push(crate::vfs::enter_user_maintenance(uid));
        }
        // Sequential recovery
        let mut result = WalRecoveryResult {
            recovered: 0,
            failed: 0,
            errors: vec![],
        };
        for row in rows {
            match self
                .recover_one(row.id, &row.user_id, &row.operation_data, &hub)
                .await
            {
                Ok(_) => result.recovered += 1,
                Err(e) => {
                    result.failed += 1;
                    result.errors.push(format!("Log {}: {}", row.id, e));
                }
            }
        }
        Ok(result)
    }
    async fn recover_one(
        &self,
        log_id: i64,
        user_id: &str,
        op_data: &str,
        hub: &Arc<VfsStorageHub>,
    ) -> Result<(), String> {
        let op: WalOperation = serde_json::from_str(op_data).map_err(|e| e.to_string())?;
        let engine = hub
            .create_scoped_engine(Arc::clone(&self.db), user_id, "100", None)
            .await
            .map_err(|e| e.to_string())?;
        macro_rules! exists_or_log {
            ($path:expr) => {
                match engine.exists($path).await {
                    Ok(exists) => exists,
                    Err(err) => {
                        yh_console_log::yhlog(
                            "warn",
                            &format!("WAL: failed to check path existence [{}]: {}", $path, err),
                        );
                        false
                    }
                }
            };
        }

        match op {
            WalOperation::Write { path, .. } => {
                let tmp = format!("{}.tmp", path);
                // Only delete .tmp files
                if exists_or_log!(&tmp) {
                    let _ = engine.delete(&tmp).await;
                    yh_console_log::yhlog(
                        "info",
                        &format!("WAL: Cleaned orphaned temp file {}", tmp),
                    );
                }
                // For target files, don't blindly delete
                if exists_or_log!(&path) {
                    let corrupted_path =
                        format!("{}.corrupted_{}", path, chrono::Utc::now().timestamp());
                    let _ = engine.move_file(&path, &corrupted_path).await;
                    yh_console_log::yhlog(
                        "warn",
                        &format!(
                            "WAL: Preserved potentially incomplete file by renaming to {}",
                            corrupted_path
                        ),
                    );
                }
            }
            WalOperation::Delete { path } => {
                if exists_or_log!(&path) {
                    let _ = engine.delete(&path).await;
                }
            }
            WalOperation::Move { src, dst }
            | WalOperation::Rename {
                old_path: src,
                new_path: dst,
            } => {
                let src_exists = exists_or_log!(&src);
                let dst_exists = exists_or_log!(&dst);
                if src_exists && !dst_exists {
                    let _ = engine.move_file(&src, &dst).await;
                } else if dst_exists && src_exists {
                    // WAL
                    //sync_index
                }
            }
            WalOperation::CreateDir { path } => {
                if exists_or_log!(&path) {
                    match engine.list(&path).await {
                        Ok(entries) => {
                            if entries.is_empty() {
                                let _ = engine.delete(&path).await;
                            }
                        }
                        Err(err) => {
                            yh_console_log::yhlog(
                                "warn",
                                &format!(
                                    "WAL: failed to list dir [{}] during recovery: {}",
                                    path, err
                                ),
                            );
                        }
                    }
                }
            }
            WalOperation::RestoreTrash {
                trash_path,
                original_path,
            } => {
                let trash_exists = exists_or_log!(&trash_path);
                let orig_exists = exists_or_log!(&original_path);
                // If still in trash and target is free, continue restore
                if trash_exists && !orig_exists {
                    let _ = engine.move_file(&trash_path, &original_path).await;
                    yh_console_log::yhlog(
                        "info",
                        &format!(
                            "WAL: Resumed trash restore from {} to {}",
                            trash_path, original_path
                        ),
                    );
                } else if orig_exists && !trash_exists {
                    // Already restored
                } else if orig_exists && trash_exists {
                    // Both exist, potentially inconsistent, do nothing
                    yh_console_log::yhlog(
                        "warn",
                        &format!(
                            "WAL: Ambiguous state for restore {} -> {}, skipping physical op",
                            trash_path, original_path
                        ),
                    );
                }
            }
        }
        self.complete_operation(log_id)
            .await
            .map_err(|e| e.to_string())
    }
}
