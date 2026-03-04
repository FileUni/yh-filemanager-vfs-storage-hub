use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize, ToSchema)]
#[sea_orm(table_name = "yh_vfs_shares")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String, // ID
    pub user_id: String,
    pub file_path: String,
    pub file_name: String,
    pub is_dir: bool,
    pub password: Option<String>,
    #[schema(value_type = Option<String>)]
    pub expire_at: Option<DateTimeWithTimeZone>,
    pub view_count: i64,
    pub is_public: bool, // Whether public (no password required)
    #[schema(value_type = String)]
    pub created_at: DateTimeWithTimeZone,
}
#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}
impl ActiveModelBehavior for ActiveModel {}
