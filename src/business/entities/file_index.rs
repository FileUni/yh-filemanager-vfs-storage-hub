use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "yh_vfs_file_index")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub user_id: String,
    #[sea_orm(column_type = "Text")]
    pub parent_path: String,
    pub name: String,
    #[sea_orm(column_type = "Text")]
    pub path: String,
    pub is_dir: bool,
    // Physical Storage
    pub storage_id: Option<String>,
    pub backend_type: Option<String>,
    #[sea_orm(column_type = "Text", nullable)]
    pub backend_key: Option<String>,
    // Attributes
    pub size: i64,
    pub physical_size: Option<i64>,
    pub etag: Option<String>,
    #[sea_orm(column_type = "Text", nullable)]
    pub protected_meta: Option<String>,
    // File Timestamps
    pub file_created_at: Option<DateTimeWithTimeZone>,
    pub file_updated_at: Option<DateTimeWithTimeZone>,
    pub file_trashed_at: Option<DateTimeWithTimeZone>,
    // Row Timestamps
    pub row_created_at: DateTimeWithTimeZone,
    pub row_updated_at: DateTimeWithTimeZone,
    pub row_deleted_at: Option<DateTimeWithTimeZone>,
    // Business Logic
    pub favorite_color: i32,
    pub original_path: Option<String>,
    #[sea_orm(column_type = "Text", nullable)]
    pub remark: Option<String>,
}
#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::file_share::Entity")]
    Shares,
}
impl Related<super::file_share::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Shares.def()
    }
}
impl ActiveModelBehavior for ActiveModel {}
