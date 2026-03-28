use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "yh_vfs_file_shares")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    // Foreign Keys
    pub file_index_id: String,
    pub user_id: String,
    pub password: Option<String>,
    pub expire_at: Option<DateTimeWithTimeZone>,
    pub view_count: i64,
    pub max_downloads: Option<i64>,
    pub is_public: bool,
    pub enable_direct: bool,
    pub can_upload: bool,
    pub can_update_no_create: bool,
    pub can_delete: bool,
    pub note: Option<String>,
    pub label: Option<String>,
    #[sea_orm(column_type = "Text", nullable)]
    pub attributes: Option<String>,
    pub hide_download: bool,
    #[sea_orm(column_type = "Text", nullable)]
    pub snapshot_path: Option<String>,
    pub snapshot_name: Option<String>,
    pub snapshot_is_dir: Option<bool>,
    pub is_deleted: bool,
    pub created_at: DateTimeWithTimeZone,
}
#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::file_index::Entity",
        from = "(Column::FileIndexId, Column::UserId)",
        to = "(super::file_index::Column::Id, super::file_index::Column::UserId)",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    FileIndex,
}
impl Related<super::file_index::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::FileIndex.def()
    }
}
impl ActiveModelBehavior for ActiveModel {}
