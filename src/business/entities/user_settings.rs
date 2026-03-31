use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize, ToSchema)]
#[sea_orm(table_name = "yh_vfs_user_settings")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub user_id: String,
    pub pool_name: String,
    pub base_dir: String,   // ()
    pub storage_quota: i64, // , 0
    pub storage_used: i64,
    pub thumbnail_disable_text: bool,
    pub thumbnail_disable_markdown: bool,
    pub thumbnail_disable_pdf: bool,
    pub thumbnail_disable_image: bool,
    pub thumbnail_disable_video: bool,
    pub thumbnail_disable_audio: bool,
    pub thumbnail_disable_office: bool,
    pub thumbnail_disable_tex: bool,
    pub sftp_enable_password: bool, // Whether to allow SFTP password login
    pub s3_access_key: Option<String>, // S3 independent access key ID
    pub s3_secret_key: Option<String>, // S3 independent access key Secret
    pub protected_root: Option<String>,
    pub protected_mode: Option<String>,
    pub protected_key_slot_id: Option<String>,
    pub protected_wrapped_key: Option<String>,
    pub protected_enabled_at: Option<chrono::DateTime<chrono::Utc>>,
    pub protected_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}
#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}
impl ActiveModelBehavior for ActiveModel {}
