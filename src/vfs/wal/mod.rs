//! Write-ahead log / operation journal.
use crate::business::services::{UserSettingsService, UserSettingsSnapshot};
use crate::vfs::scoped::ScopedVfsStorageEngine;
use crate::vfs::{VfsFileInfo, VfsStorage, VfsStorageHub};
use sea_orm::sea_query::{Alias, ColumnDef, Expr, Index, Table};
use sea_orm::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub mod entity;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalStatus {
    Pending,
    Recovering,
    PhysicalDone,
    MetadataDone,
    Completed,
    Failed,
}

impl WalStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            WalStatus::Pending => "pending",
            WalStatus::Recovering => "recovering",
            WalStatus::PhysicalDone => "physical_done",
            WalStatus::MetadataDone => "metadata_done",
            WalStatus::Completed => "completed",
            WalStatus::Failed => "failed",
        }
    }

    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "recovering" => WalStatus::Recovering,
            "physical_done" => WalStatus::PhysicalDone,
            "metadata_done" => WalStatus::MetadataDone,
            "completed" => WalStatus::Completed,
            "failed" => WalStatus::Failed,
            _ => WalStatus::Pending,
        }
    }

    pub fn is_active(self) -> bool {
        !matches!(self, WalStatus::Completed)
    }

    pub fn has_physical_done(self) -> bool {
        matches!(
            self,
            WalStatus::PhysicalDone | WalStatus::MetadataDone | WalStatus::Completed
        )
    }

    pub fn has_metadata_done(self) -> bool {
        matches!(self, WalStatus::MetadataDone | WalStatus::Completed)
    }
}

/// Write-Ahead Log Operation Types
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalProtectedWriteMeta {
    pub backend_key: String,
    pub physical_size: i64,
    pub protected_meta: String,
}

#[derive(Clone, Copy)]
enum WalColumnKind {
    TextNullable,
    TimestampNullable,
    TimestampNotNullDefaultCurrentTimestamp,
    StatusNotNullDefaultPending,
}

fn build_wal_column(column_name: &str, kind: WalColumnKind) -> ColumnDef {
    let mut column = ColumnDef::new(Alias::new(column_name));
    match kind {
        WalColumnKind::TextNullable => {
            column.text().null();
        }
        WalColumnKind::TimestampNullable => {
            column.timestamp_with_time_zone().null();
        }
        WalColumnKind::TimestampNotNullDefaultCurrentTimestamp => {
            column
                .timestamp_with_time_zone()
                .not_null()
                .default(Expr::current_timestamp());
        }
        WalColumnKind::StatusNotNullDefaultPending => {
            column.string().not_null().default("pending");
        }
    }
    column
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalOperation {
    Write {
        path: String,
        size: u64,
        #[serde(default)]
        protected: Option<WalProtectedWriteMeta>,
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
    MoveToTrash {
        path: String,
        trash_path: String,
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
            WalOperation::MoveToTrash { .. } => "MOVE_TO_TRASH",
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalIssueRecord {
    pub id: i64,
    pub user_id: String,
    pub operation_type: String,
    pub operation_data: String,
    pub status: String,
    pub failure_reason: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub completed_at: Option<String>,
}

#[derive(Debug, Clone)]
pub struct WalIssueListResult {
    pub items: Vec<WalIssueRecord>,
    pub total: u64,
    pub page: u64,
    pub page_size: u64,
}

#[derive(Debug, Clone, Default)]
pub struct WalQueryFilters<'a> {
    pub scope: Option<&'a str>,
    pub status: Option<&'a str>,
    pub user_id: Option<&'a str>,
    pub operation_type: Option<&'a str>,
    pub updated_from: Option<chrono::DateTime<chrono::Utc>>,
    pub updated_to: Option<chrono::DateTime<chrono::Utc>>,
}

/// Write-Ahead Log Manager
pub struct VfsWalManager {
    db: Arc<DatabaseConnection>,
}

impl VfsWalManager {
    pub fn new(db: Arc<DatabaseConnection>) -> Self {
        Self { db }
    }

    /// Initialize WAL table structure and auto-upgrade old schemas.
    pub async fn init_tables(&self) -> Result<(), DbErr> {
        let backend = self.db.get_database_backend();
        let schema = Schema::new(backend);
        let stmt = schema
            .create_table_from_entity(entity::Entity)
            .if_not_exists()
            .to_owned();
        self.db.execute(backend.build(&stmt)).await?;

        self.ensure_journal_columns().await?;

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
        let idx_status = Index::create()
            .name("idx_vfs_wal_status")
            .table(entity::Entity)
            .col(entity::Column::Status)
            .if_not_exists()
            .to_owned();

        self.db.execute(backend.build(&idx_user)).await?;
        self.db.execute(backend.build(&idx_type)).await?;
        self.db.execute(backend.build(&idx_status)).await?;
        Ok(())
    }

    pub async fn log_operation(
        &self,
        user_id: &str,
        operation: WalOperation,
    ) -> Result<i64, DbErr> {
        let op_json =
            serde_json::to_string(&operation).map_err(|e| DbErr::Custom(e.to_string()))?;
        let now = chrono::Utc::now();
        let active_model = entity::ActiveModel {
            user_id: Set(user_id.to_string()),
            operation_type: Set(operation.as_str().to_string()),
            operation_data: Set(op_json),
            status: Set(WalStatus::Pending.as_str().to_string()),
            failure_reason: Set(None),
            created_at: Set(now.into()),
            updated_at: Set(now.into()),
            completed_at: Set(None),
            ..Default::default()
        };
        let res = entity::Entity::insert(active_model).exec(&*self.db).await?;
        Ok(res.last_insert_id)
    }

    pub async fn mark_recovering(&self, log_id: i64) -> Result<(), DbErr> {
        self.transition_operation(log_id, WalStatus::Recovering, None)
            .await
    }

    pub async fn mark_physical_done(&self, log_id: i64) -> Result<(), DbErr> {
        self.transition_operation(log_id, WalStatus::PhysicalDone, None)
            .await
    }

    pub async fn fail_operation(&self, log_id: i64, reason: &str) -> Result<(), DbErr> {
        self.transition_operation(log_id, WalStatus::Failed, Some(reason))
            .await
    }

    pub async fn complete_operation(&self, log_id: i64) -> Result<(), DbErr> {
        let now = chrono::Utc::now();
        entity::Entity::update_many()
            .filter(entity::Column::Id.eq(log_id))
            .col_expr(
                entity::Column::Status,
                Expr::value(WalStatus::Completed.as_str()),
            )
            .col_expr(
                entity::Column::FailureReason,
                Expr::value(Option::<String>::None),
            )
            .col_expr(entity::Column::UpdatedAt, Expr::value(now))
            .col_expr(entity::Column::CompletedAt, Expr::value(Some(now)))
            .exec(&*self.db)
            .await?;
        Ok(())
    }

    pub async fn list_issue_records(&self, limit: u64) -> Result<Vec<WalIssueRecord>, DbErr> {
        let rows = entity::Entity::find()
            .filter(
                entity::Column::Status
                    .is_in([WalStatus::Failed.as_str(), WalStatus::Recovering.as_str()]),
            )
            .order_by_desc(entity::Column::UpdatedAt)
            .limit(limit)
            .all(&*self.db)
            .await?;
        Ok(rows.into_iter().map(Self::map_issue_record).collect())
    }

    pub async fn list_issue_records_paginated(
        &self,
        page: u64,
        page_size: u64,
        filters: WalQueryFilters<'_>,
    ) -> Result<WalIssueListResult, DbErr> {
        let page = page.max(1);
        let page_size = page_size.max(1);
        let condition = Self::build_filtered_condition(filters);

        let paginator = entity::Entity::find()
            .filter(condition)
            .order_by_desc(entity::Column::UpdatedAt)
            .paginate(&*self.db, page_size);
        let total = paginator.num_items().await?;
        let rows = paginator.fetch_page(page - 1).await?;

        Ok(WalIssueListResult {
            items: rows.into_iter().map(Self::map_issue_record).collect(),
            total,
            page,
            page_size,
        })
    }

    pub async fn list_issue_records_filtered(
        &self,
        filters: WalQueryFilters<'_>,
        limit: Option<u64>,
    ) -> Result<Vec<WalIssueRecord>, DbErr> {
        let mut query = entity::Entity::find()
            .filter(Self::build_filtered_condition(filters))
            .order_by_desc(entity::Column::UpdatedAt);
        if let Some(limit) = limit {
            query = query.limit(limit);
        }
        let rows = query.all(&*self.db).await?;
        Ok(rows.into_iter().map(Self::map_issue_record).collect())
    }

    pub async fn get_issue_record_by_id(
        &self,
        log_id: i64,
    ) -> Result<Option<WalIssueRecord>, DbErr> {
        let row = entity::Entity::find_by_id(log_id).one(&*self.db).await?;
        Ok(row.map(Self::map_issue_record))
    }

    pub async fn replay_issue(&self, log_id: i64, hub: Arc<VfsStorageHub>) -> Result<(), DbErr> {
        let row = entity::Entity::find_by_id(log_id)
            .one(&*self.db)
            .await?
            .ok_or_else(|| DbErr::RecordNotFound(format!("WAL log {} not found", log_id)))?;
        let status = WalStatus::parse(&row.status);
        if !status.is_active() {
            return Err(DbErr::Custom(format!(
                "WAL log {} is already completed and cannot be replayed",
                log_id
            )));
        }
        self.recover_one(&row, &hub).await.map_err(DbErr::Custom)
    }

    pub async fn mark_issue_handled(&self, log_id: i64, note: Option<&str>) -> Result<(), DbErr> {
        let row = entity::Entity::find_by_id(log_id)
            .one(&*self.db)
            .await?
            .ok_or_else(|| DbErr::RecordNotFound(format!("WAL log {} not found", log_id)))?;
        let handled_note = note
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| format!("ADMIN_MARKED_HANDLED: {}", value))
            .unwrap_or_else(|| "ADMIN_MARKED_HANDLED".to_string());
        let failure_reason = match row.failure_reason.as_deref() {
            Some(existing) if !existing.is_empty() => {
                Some(format!("{}\n{}", existing, handled_note))
            }
            _ => Some(handled_note),
        };
        let now = chrono::Utc::now();
        entity::Entity::update_many()
            .filter(entity::Column::Id.eq(log_id))
            .col_expr(
                entity::Column::Status,
                Expr::value(WalStatus::Completed.as_str()),
            )
            .col_expr(entity::Column::FailureReason, Expr::value(failure_reason))
            .col_expr(entity::Column::UpdatedAt, Expr::value(now))
            .col_expr(entity::Column::CompletedAt, Expr::value(Some(now)))
            .exec(&*self.db)
            .await?;
        Ok(())
    }

    /// Revoke all WAL journal rows.
    pub async fn revoke_all(&self) -> Result<u64, DbErr> {
        let res = entity::Entity::delete_many().exec(&*self.db).await?;
        Ok(res.rows_affected)
    }

    pub async fn recover_all(&self, hub: Arc<VfsStorageHub>) -> Result<WalRecoveryResult, DbErr> {
        let rows = entity::Entity::find()
            .filter(entity::Column::Status.ne(WalStatus::Completed.as_str()))
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

        let mut affected_users: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for row in &rows {
            affected_users.insert(row.user_id.as_str());
        }

        let mut _guards = Vec::new();
        for uid in &affected_users {
            _guards.push(crate::vfs::enter_user_maintenance(uid));
        }

        let mut result = WalRecoveryResult {
            recovered: 0,
            failed: 0,
            errors: vec![],
        };
        for row in rows {
            match self.recover_one(&row, &hub).await {
                Ok(_) => result.recovered += 1,
                Err(e) => {
                    result.failed += 1;
                    result.errors.push(format!("Log {}: {}", row.id, e));
                }
            }
        }
        Ok(result)
    }

    fn map_issue_record(row: entity::Model) -> WalIssueRecord {
        WalIssueRecord {
            id: row.id,
            user_id: row.user_id,
            operation_type: row.operation_type,
            operation_data: row.operation_data,
            status: row.status,
            failure_reason: row.failure_reason,
            created_at: chrono::DateTime::<chrono::Utc>::from(row.created_at).to_rfc3339(),
            updated_at: chrono::DateTime::<chrono::Utc>::from(row.updated_at).to_rfc3339(),
            completed_at: row
                .completed_at
                .map(|value| chrono::DateTime::<chrono::Utc>::from(value).to_rfc3339()),
        }
    }

    fn build_filtered_condition(filters: WalQueryFilters<'_>) -> Condition {
        let mut condition = Condition::all();

        if let Some(user_id) = filters
            .user_id
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            condition = condition.add(entity::Column::UserId.eq(user_id));
        }

        if let Some(operation_type) = filters
            .operation_type
            .map(str::trim)
            .filter(|value| !value.is_empty() && !value.eq_ignore_ascii_case("all"))
        {
            condition = condition.add(entity::Column::OperationType.eq(operation_type));
        }

        if let Some(updated_from) = filters.updated_from {
            condition = condition.add(entity::Column::UpdatedAt.gte(updated_from));
        }

        if let Some(updated_to) = filters.updated_to {
            condition = condition.add(entity::Column::UpdatedAt.lte(updated_to));
        }

        let normalized_status = filters.status.unwrap_or("all").trim().to_ascii_lowercase();
        let normalized_scope = filters
            .scope
            .unwrap_or("issues")
            .trim()
            .to_ascii_lowercase();
        if normalized_status.is_empty() || normalized_status == "all" {
            match normalized_scope.as_str() {
                "history" => {
                    condition =
                        condition.add(entity::Column::Status.eq(WalStatus::Completed.as_str()))
                }
                "all" => {}
                _ => {
                    condition = condition.add(
                        entity::Column::Status
                            .is_in([WalStatus::Failed.as_str(), WalStatus::Recovering.as_str()]),
                    )
                }
            }
        } else {
            condition = condition.add(entity::Column::Status.eq(normalized_status));
        }

        condition
    }

    async fn ensure_journal_columns(&self) -> Result<(), DbErr> {
        self.add_column_if_missing(
            "status",
            WalColumnKind::StatusNotNullDefaultPending,
        )
        .await?;
        self.add_column_if_missing(
            "failure_reason",
            WalColumnKind::TextNullable,
        )
        .await?;
        self.add_column_if_missing(
            "updated_at",
            WalColumnKind::TimestampNotNullDefaultCurrentTimestamp,
        )
        .await?;
        self.add_column_if_missing(
            "completed_at",
            WalColumnKind::TimestampNullable,
        )
        .await?;
        Ok(())
    }

    async fn add_column_if_missing(
        &self,
        column_name: &str,
        column_kind: WalColumnKind,
    ) -> Result<(), DbErr> {
        let backend = self.db.get_database_backend();
        let stmt = Table::alter()
            .table(Alias::new("yh_vfs_wal"))
            .add_column(build_wal_column(column_name, column_kind))
            .to_owned();
        match self.db.execute(backend.build(&stmt)).await {
            Ok(_) => Ok(()),
            Err(err) if Self::is_duplicate_column_error(&err, column_name) => Ok(()),
            Err(err) => Err(err),
        }
    }

    fn is_duplicate_column_error(err: &DbErr, column_name: &str) -> bool {
        let msg = err.to_string().to_ascii_lowercase();
        msg.contains("duplicate column")
            || msg.contains("duplicate column name")
            || (msg.contains("already exists") && msg.contains(&column_name.to_ascii_lowercase()))
    }

    async fn transition_operation(
        &self,
        log_id: i64,
        status: WalStatus,
        failure_reason: Option<&str>,
    ) -> Result<(), DbErr> {
        let now = chrono::Utc::now();
        let completed_at = if matches!(status, WalStatus::Completed) {
            Some(now)
        } else {
            None
        };
        entity::Entity::update_many()
            .filter(entity::Column::Id.eq(log_id))
            .col_expr(entity::Column::Status, Expr::value(status.as_str()))
            .col_expr(
                entity::Column::FailureReason,
                Expr::value(failure_reason.map(std::borrow::ToOwned::to_owned)),
            )
            .col_expr(entity::Column::UpdatedAt, Expr::value(now))
            .col_expr(entity::Column::CompletedAt, Expr::value(completed_at))
            .exec(&*self.db)
            .await?;
        Ok(())
    }

    async fn recover_one(
        &self,
        row: &entity::Model,
        hub: &Arc<VfsStorageHub>,
    ) -> Result<(), String> {
        let status = WalStatus::parse(&row.status);
        if !status.is_active() {
            return Ok(());
        }

        self.mark_recovering(row.id)
            .await
            .map_err(|e| e.to_string())?;
        let op: WalOperation =
            serde_json::from_str(&row.operation_data).map_err(|e| e.to_string())?;
        let engine = self
            .create_recovery_engine(row.user_id.as_str(), hub)
            .await?;

        let recover_res = match op {
            WalOperation::Write {
                path,
                size,
                protected,
                ..
            } => {
                self.recover_write(&engine, &path, size, protected.as_ref(), status)
                    .await
            }
            WalOperation::Delete { path } => self.recover_delete(&engine, &path, status).await,
            WalOperation::Move { src, dst } => self.recover_move(&engine, &src, &dst, status).await,
            WalOperation::Rename { old_path, new_path } => {
                self.recover_move(&engine, &old_path, &new_path, status)
                    .await
            }
            WalOperation::MoveToTrash { path, trash_path } => {
                self.recover_move_to_trash(&engine, &path, &trash_path, status)
                    .await
            }
            WalOperation::CreateDir { path } => {
                self.recover_create_dir(&engine, &path, status).await
            }
            WalOperation::RestoreTrash {
                trash_path,
                original_path,
            } => {
                self.recover_restore_trash(&engine, &trash_path, &original_path, status)
                    .await
            }
        };

        match recover_res {
            Ok(_) => self
                .complete_operation(row.id)
                .await
                .map_err(|e| e.to_string()),
            Err(err) => {
                let _ = self.fail_operation(row.id, &err).await;
                Err(err)
            }
        }
    }

    async fn create_recovery_engine(
        &self,
        user_id: &str,
        hub: &Arc<VfsStorageHub>,
    ) -> Result<Arc<ScopedVfsStorageEngine>, String> {
        let pool = hub
            .route_for_user(self.db.as_ref(), user_id, "100")
            .await
            .map_err(|e| e.to_string())?;
        let task_handler = hub.task_handler.as_ref().map(Arc::clone);
        let engine = ScopedVfsStorageEngine::new(
            Arc::clone(&self.db),
            user_id.to_string(),
            pool,
            None,
            task_handler,
            None,
        )
        .map_err(|e| e.to_string())?;
        Ok(Arc::new(engine))
    }

    async fn load_user_settings(
        &self,
        engine: &Arc<ScopedVfsStorageEngine>,
    ) -> Result<Option<UserSettingsSnapshot>, String> {
        UserSettingsService::get_user_settings(&self.db, engine.user_id.as_ref())
            .await
            .map(|settings| settings.as_ref().map(UserSettingsSnapshot::from))
            .map_err(|e| e.to_string())
    }

    async fn protected_root_for_path(
        &self,
        engine: &Arc<ScopedVfsStorageEngine>,
        path: &str,
    ) -> Result<Option<String>, String> {
        let Some(settings) = self.load_user_settings(engine).await? else {
            return Ok(None);
        };
        Ok(settings
            .protected_root_trimmed()
            .filter(|_| settings.matches_protected_root(path))
            .map(str::to_string))
    }

    async fn is_root_protected_path(
        &self,
        engine: &Arc<ScopedVfsStorageEngine>,
        path: &str,
    ) -> Result<bool, String> {
        Ok(self
            .protected_root_for_path(engine, path)
            .await?
            .is_some_and(|root| root == "/"))
    }

    async fn same_protected_domain(
        &self,
        engine: &Arc<ScopedVfsStorageEngine>,
        src: &str,
        dst: &str,
    ) -> Result<bool, String> {
        let src_root = self.protected_root_for_path(engine, src).await?;
        let dst_root = self.protected_root_for_path(engine, dst).await?;
        Ok(src_root.is_some() && src_root == dst_root)
    }

    fn to_physical_user_path(user_id: &str, logical_path: &str) -> String {
        let relative = logical_path.trim_start_matches('/');
        format!("{}/{}", user_id, relative)
    }

    async fn cleanup_orphan_temp(
        &self,
        engine: &Arc<ScopedVfsStorageEngine>,
        temp_path: &str,
    ) -> Result<(), String> {
        let physical_temp = Self::to_physical_user_path(engine.user_id.as_ref(), temp_path);
        if engine
            .pool
            .exists(&physical_temp)
            .await
            .map_err(|e| e.to_string())?
        {
            let _ = engine
                .pool
                .delete(&physical_temp)
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    async fn physical_move_without_wal(
        &self,
        engine: &Arc<ScopedVfsStorageEngine>,
        src: &str,
        dst: &str,
    ) -> Result<(), String> {
        let src_path = Self::to_physical_user_path(engine.user_id.as_ref(), src);
        let dst_path = Self::to_physical_user_path(engine.user_id.as_ref(), dst);
        engine
            .pool
            .move_file(&src_path, &dst_path)
            .await
            .map_err(|e| e.to_string())?;
        engine.cache.invalidate_parent_ls(src).await;
        engine.cache.invalidate_parent_ls(dst).await;
        Ok(())
    }

    async fn sync_index_for_path(
        &self,
        engine: &Arc<ScopedVfsStorageEngine>,
        path: &str,
    ) -> Result<(), String> {
        let info = engine.stat(path).await.map_err(|e| e.to_string())?;
        engine
            .index_service
            .upsert_file(engine.user_id.as_ref(), path, &info)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn recover_write(
        &self,
        engine: &Arc<ScopedVfsStorageEngine>,
        path: &str,
        size: u64,
        protected: Option<&WalProtectedWriteMeta>,
        status: WalStatus,
    ) -> Result<(), String> {
        let protected_root = self.protected_root_for_path(engine, path).await?;
        if protected_root.is_some() {
            let index_row = engine
                .index_service
                .get_file_metadata(engine.user_id.as_ref(), path)
                .await
                .map_err(|e| e.to_string())?;
            if status.has_metadata_done() {
                return Ok(());
            }
            if status.has_physical_done() {
                if index_row.is_some() {
                    return Ok(());
                }
                if let Some(meta) = protected {
                    let physical_exists = engine
                        .pool
                        .exists(&meta.backend_key)
                        .await
                        .map_err(|e| e.to_string())?;
                    if physical_exists {
                        let stat = engine.pool.stat(&meta.backend_key).await.map_err(|e| e.to_string())?;
                        let info = VfsFileInfo {
                            name: std::path::Path::new(path)
                                .file_name()
                                .and_then(|value| value.to_str())
                                .unwrap_or("")
                                .to_string()
                                .into(),
                            path: path.to_string().into(),
                            is_dir: false,
                            size,
                            modified: stat.modified,
                            favorite_color: 0,
                            has_active_share: None,
                            has_active_direct: None,
                            trashed_at: None,
                            original_path: None,
                        };
                        engine
                            .index_service
                            .upsert_file_with_location(
                                engine.user_id.as_ref(),
                                path,
                                &info,
                                Some(engine.pool.config.get_name()),
                                Some(engine.pool.get_backend_type().as_str()),
                                Some(meta.backend_key.as_str()),
                                Some(meta.physical_size),
                                Some(meta.protected_meta.as_str()),
                            )
                            .await
                            .map_err(|e| e.to_string())?;
                        return Ok(());
                    }
                }
                return Err(
                    "Protected write recovery requires manual backend_key repair because index metadata is missing"
                        .to_string(),
                );
            }
            if index_row.is_some() {
                return Ok(());
            }
            if let Some(meta) = protected {
                let physical_exists = engine
                    .pool
                    .exists(&meta.backend_key)
                    .await
                    .map_err(|e| e.to_string())?;
                if physical_exists {
                    let stat = engine.pool.stat(&meta.backend_key).await.map_err(|e| e.to_string())?;
                    let info = VfsFileInfo {
                        name: std::path::Path::new(path)
                            .file_name()
                            .and_then(|value| value.to_str())
                            .unwrap_or("")
                            .to_string()
                            .into(),
                        path: path.to_string().into(),
                        is_dir: false,
                        size,
                        modified: stat.modified,
                        favorite_color: 0,
                        has_active_share: None,
                        has_active_direct: None,
                        trashed_at: None,
                        original_path: None,
                    };
                    engine
                        .index_service
                        .upsert_file_with_location(
                            engine.user_id.as_ref(),
                            path,
                            &info,
                            Some(engine.pool.config.get_name()),
                            Some(engine.pool.get_backend_type().as_str()),
                            Some(meta.backend_key.as_str()),
                            Some(meta.physical_size),
                            Some(meta.protected_meta.as_str()),
                        )
                        .await
                        .map_err(|e| e.to_string())?;
                    return Ok(());
                }
            }
            return Err(
                "Protected write recovery requires manual backend_key repair because WAL does not store blob locator"
                    .to_string(),
            );
        }
        let tmp_path = format!("{}.tmp", path);
        let target_exists = engine.exists(path).await.map_err(|e| e.to_string())?;
        let temp_exists = engine.exists(&tmp_path).await.map_err(|e| e.to_string())?;

        if status.has_metadata_done() {
            return Ok(());
        }
        if status.has_physical_done() {
            if target_exists {
                return self.sync_index_for_path(engine, path).await;
            }
            return engine
                .index_service
                .delete_file(engine.user_id.as_ref(), path)
                .await
                .map_err(|e| e.to_string());
        }

        if target_exists {
            if temp_exists {
                self.cleanup_orphan_temp(engine, &tmp_path).await?;
            }
            return self.sync_index_for_path(engine, path).await;
        }

        if temp_exists {
            self.cleanup_orphan_temp(engine, &tmp_path).await?;
        }

        engine
            .index_service
            .delete_file(engine.user_id.as_ref(), path)
            .await
            .map_err(|e| e.to_string())
    }

    async fn recover_delete(
        &self,
        engine: &Arc<ScopedVfsStorageEngine>,
        path: &str,
        status: WalStatus,
    ) -> Result<(), String> {
        if status.has_metadata_done() {
            return Ok(());
        }
        if status.has_physical_done() {
            return engine
                .index_service
                .delete_file(engine.user_id.as_ref(), path)
                .await
                .map_err(|e| e.to_string());
        }
        if engine.exists(path).await.map_err(|e| e.to_string())? {
            let _ = engine.delete(path).await.map_err(|e| e.to_string())?;
            return Ok(());
        }
        engine
            .index_service
            .delete_file(engine.user_id.as_ref(), path)
            .await
            .map_err(|e| e.to_string())
    }

    async fn recover_move(
        &self,
        engine: &Arc<ScopedVfsStorageEngine>,
        src: &str,
        dst: &str,
        status: WalStatus,
    ) -> Result<(), String> {
        if self.same_protected_domain(engine, src, dst).await? {
            if status.has_metadata_done() {
                return Ok(());
            }
            let src_exists = engine.exists(src).await.map_err(|e| e.to_string())?;
            let dst_exists = engine.exists(dst).await.map_err(|e| e.to_string())?;
            return match (src_exists, dst_exists) {
                (true, false) => engine
                    .index_service
                    .move_file(engine.user_id.as_ref(), src, dst)
                    .await
                    .map_err(|e| e.to_string()),
                (false, true) => Ok(()),
                (false, false) => Err(format!(
                    "PROTECTED MOVE recovery ambiguous: both source '{}' and destination '{}' are missing",
                    src, dst
                )),
                (true, true) => Err(format!(
                    "PROTECTED MOVE recovery ambiguous: both source '{}' and destination '{}' exist",
                    src, dst
                )),
            };
        }
        if status.has_metadata_done() {
            return Ok(());
        }
        if status.has_physical_done() {
            return engine
                .index_service
                .move_file(engine.user_id.as_ref(), src, dst)
                .await
                .map_err(|e| e.to_string());
        }
        let src_exists = engine.exists(src).await.map_err(|e| e.to_string())?;
        let dst_exists = engine.exists(dst).await.map_err(|e| e.to_string())?;
        match (src_exists, dst_exists) {
            (true, false) => {
                self.physical_move_without_wal(engine, src, dst).await?;
                Ok(())
            }
            (false, true) => engine
                .index_service
                .move_file(engine.user_id.as_ref(), src, dst)
                .await
                .map_err(|e| e.to_string()),
            (false, false) => Err(format!(
                "MOVE recovery ambiguous: both source '{}' and destination '{}' are missing",
                src, dst
            )),
            (true, true) => Err(format!(
                "MOVE recovery ambiguous: both source '{}' and destination '{}' exist",
                src, dst
            )),
        }
    }

    async fn recover_create_dir(
        &self,
        engine: &Arc<ScopedVfsStorageEngine>,
        path: &str,
        status: WalStatus,
    ) -> Result<(), String> {
        if status.has_metadata_done() {
            return Ok(());
        }
        if status.has_physical_done() {
            return self.sync_index_for_path(engine, path).await;
        }
        if !engine.exists(path).await.map_err(|e| e.to_string())? {
            let _ = engine.create_dir(path).await.map_err(|e| e.to_string())?;
            return Ok(());
        }
        self.sync_index_for_path(engine, path).await
    }

    async fn recover_move_to_trash(
        &self,
        engine: &Arc<ScopedVfsStorageEngine>,
        path: &str,
        trash_path: &str,
        status: WalStatus,
    ) -> Result<(), String> {
        if self.is_root_protected_path(engine, path).await? {
            if status.has_metadata_done() {
                return Ok(());
            }
            let src_exists = engine.exists(path).await.map_err(|e| e.to_string())?;
            let trash_exists = engine.exists(trash_path).await.map_err(|e| e.to_string())?;
            return match (src_exists, trash_exists) {
                (true, false) => engine
                    .index_service
                    .trash_file(engine.user_id.as_ref(), path, trash_path)
                    .await
                    .map_err(|e| e.to_string()),
                (false, true) => Ok(()),
                (false, false) => Err(format!(
                    "PROTECTED MOVE_TO_TRASH recovery ambiguous: both source '{}' and trash '{}' are missing",
                    path, trash_path
                )),
                (true, true) => Err(format!(
                    "PROTECTED MOVE_TO_TRASH recovery ambiguous: both source '{}' and trash '{}' exist",
                    path, trash_path
                )),
            };
        }
        if status.has_metadata_done() {
            return Ok(());
        }
        if status.has_physical_done() {
            return engine
                .index_service
                .trash_file(engine.user_id.as_ref(), path, trash_path)
                .await
                .map_err(|e| e.to_string());
        }
        let src_exists = engine.exists(path).await.map_err(|e| e.to_string())?;
        let trash_exists = engine.exists(trash_path).await.map_err(|e| e.to_string())?;
        match (src_exists, trash_exists) {
            (true, false) => {
                self.physical_move_without_wal(engine, path, trash_path)
                    .await?;
                engine
                    .index_service
                    .trash_file(engine.user_id.as_ref(), path, trash_path)
                    .await
                    .map_err(|e| e.to_string())
            }
            (false, true) => engine
                .index_service
                .trash_file(engine.user_id.as_ref(), path, trash_path)
                .await
                .map_err(|e| e.to_string()),
            (false, false) => Err(format!(
                "MOVE_TO_TRASH recovery ambiguous: both source '{}' and trash '{}' are missing",
                path, trash_path
            )),
            (true, true) => Err(format!(
                "MOVE_TO_TRASH recovery ambiguous: both source '{}' and trash '{}' exist",
                path, trash_path
            )),
        }
    }

    async fn recover_restore_trash(
        &self,
        engine: &Arc<ScopedVfsStorageEngine>,
        trash_path: &str,
        original_path: &str,
        status: WalStatus,
    ) -> Result<(), String> {
        if self.is_root_protected_path(engine, original_path).await? {
            if status.has_metadata_done() {
                return Ok(());
            }
            let trash_exists = engine.exists(trash_path).await.map_err(|e| e.to_string())?;
            let original_exists = engine
                .exists(original_path)
                .await
                .map_err(|e| e.to_string())?;
            return match (trash_exists, original_exists) {
                (true, false) => engine
                    .index_service
                    .restore_file(engine.user_id.as_ref(), trash_path, original_path)
                    .await
                    .map_err(|e| e.to_string()),
                (false, true) => Ok(()),
                (false, false) => Err(format!(
                    "PROTECTED RESTORE_TRASH recovery ambiguous: both '{}' and '{}' are missing",
                    trash_path, original_path
                )),
                (true, true) => Err(format!(
                    "PROTECTED RESTORE_TRASH recovery ambiguous: both '{}' and '{}' exist",
                    trash_path, original_path
                )),
            };
        }
        if status.has_metadata_done() {
            return Ok(());
        }
        if status.has_physical_done() {
            return engine
                .index_service
                .restore_file(engine.user_id.as_ref(), trash_path, original_path)
                .await
                .map_err(|e| e.to_string());
        }
        let trash_exists = engine.exists(trash_path).await.map_err(|e| e.to_string())?;
        let original_exists = engine
            .exists(original_path)
            .await
            .map_err(|e| e.to_string())?;
        match (trash_exists, original_exists) {
            (true, false) => {
                self.physical_move_without_wal(engine, trash_path, original_path)
                    .await?;
                engine
                    .index_service
                    .restore_file(engine.user_id.as_ref(), trash_path, original_path)
                    .await
                    .map_err(|e| e.to_string())
            }
            (false, true) => engine
                .index_service
                .restore_file(engine.user_id.as_ref(), trash_path, original_path)
                .await
                .map_err(|e| e.to_string()),
            (false, false) => Err(format!(
                "RESTORE_TRASH recovery ambiguous: both '{}' and '{}' are missing",
                trash_path, original_path
            )),
            (true, true) => Err(format!(
                "RESTORE_TRASH recovery ambiguous: both '{}' and '{}' exist",
                trash_path, original_path
            )),
        }
    }
}

#[allow(dead_code)]
fn _build_vfs_info(path: &str, info: &VfsFileInfo) -> VfsFileInfo {
    VfsFileInfo {
        path: path.into(),
        ..info.clone()
    }
}
