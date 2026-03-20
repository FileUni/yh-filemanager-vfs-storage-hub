// VFS Write-Ahead Log Entity
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "yh_vfs_wal")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    // User ID
    pub user_id: String,
    // Operation Type
    pub operation_type: String,
    // Operation Data (JSON)
    #[sea_orm(column_type = "Text")]
    pub operation_data: String,
    // Operation Status
    pub status: String,
    // Last Failure Reason
    #[sea_orm(column_type = "Text", nullable)]
    pub failure_reason: Option<String>,
    // Creation Time
    pub created_at: DateTimeWithTimeZone,
    // Last Update Time
    pub updated_at: DateTimeWithTimeZone,
    // Completion Time
    pub completed_at: Option<DateTimeWithTimeZone>,
}
#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}
impl ActiveModelBehavior for ActiveModel {}
