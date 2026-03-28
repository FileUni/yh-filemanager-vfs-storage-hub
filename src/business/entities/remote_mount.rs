use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "yh_vfs_remote_mounts")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    pub user_id: String,
    pub name: String,
    pub driver: String,
    pub root: String,
    pub mount_dir: String,
    pub sync_peer_dir: Option<String>,
    pub sync_mode: i16,
    pub sync_interval_minutes: i64,
    pub sync_timeout_secs: i64,
    pub enable: bool,
    pub options_json: String,
    pub last_sync_at: Option<chrono::DateTime<chrono::Utc>>,
    pub next_sync_at: Option<chrono::DateTime<chrono::Utc>>,
    pub last_sync_status: Option<String>,
    pub last_error: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
