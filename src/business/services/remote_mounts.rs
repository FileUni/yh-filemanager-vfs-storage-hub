use super::super::entities::remote_mount;
use super::{VfsCommonError, VfsCommonResult};
use chrono::{Duration, Utc};
use sea_orm::*;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteMountSyncMode {
    PeerToMountKeep = 1,
    PeerToMountMirror = 2,
    MountToPeerKeep = 3,
    MountToPeerMirror = 4,
    BidirectionalKeep = 5,
}

impl RemoteMountSyncMode {
    pub fn from_i16(value: i16) -> Option<Self> {
        match value {
            1 => Some(Self::PeerToMountKeep),
            2 => Some(Self::PeerToMountMirror),
            3 => Some(Self::MountToPeerKeep),
            4 => Some(Self::MountToPeerMirror),
            5 => Some(Self::BidirectionalKeep),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RemoteMountSnapshot {
    pub model: remote_mount::Model,
    pub options: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct NewRemoteMount {
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
    pub options: HashMap<String, String>,
}

#[derive(Debug, Clone, Default)]
pub struct RemoteMountUpdatePatch {
    pub name: Option<String>,
    pub driver: Option<String>,
    pub root: Option<String>,
    pub mount_dir: Option<String>,
    pub sync_peer_dir: Option<Option<String>>,
    pub sync_mode: Option<i16>,
    pub sync_interval_minutes: Option<i64>,
    pub sync_timeout_secs: Option<i64>,
    pub enable: Option<bool>,
    pub options: Option<HashMap<String, String>>,
    pub reset_schedule: bool,
}

pub struct RemoteMountService;

impl RemoteMountService {
    fn encode_options(options: &HashMap<String, String>) -> VfsCommonResult<String> {
        serde_json::to_string(options).map_err(|e| VfsCommonError::Internal(e.to_string()))
    }

    fn decode_options(raw: &str) -> HashMap<String, String> {
        serde_json::from_str(raw).unwrap_or_default()
    }

    fn schedule_time(
        interval_minutes: i64,
        enabled: bool,
        sync_peer_dir: Option<&str>,
    ) -> Option<chrono::DateTime<chrono::Utc>> {
        if !enabled || sync_peer_dir.is_none() {
            return None;
        }
        Some(Utc::now() + Duration::minutes(interval_minutes.max(1)))
    }

    pub async fn list_user_mounts(
        db: &DatabaseConnection,
        user_id: &str,
    ) -> VfsCommonResult<Vec<RemoteMountSnapshot>> {
        let rows = remote_mount::Entity::find()
            .filter(remote_mount::Column::UserId.eq(user_id))
            .order_by_asc(remote_mount::Column::CreatedAt)
            .all(db)
            .await?;
        Ok(rows
            .into_iter()
            .map(|model| RemoteMountSnapshot {
                options: Self::decode_options(&model.options_json),
                model,
            })
            .collect())
    }

    pub async fn list_all_mounts(db: &DatabaseConnection) -> VfsCommonResult<Vec<RemoteMountSnapshot>> {
        let rows = remote_mount::Entity::find()
            .order_by_asc(remote_mount::Column::UserId)
            .order_by_asc(remote_mount::Column::CreatedAt)
            .all(db)
            .await?;
        Ok(rows
            .into_iter()
            .map(|model| RemoteMountSnapshot {
                options: Self::decode_options(&model.options_json),
                model,
            })
            .collect())
    }

    pub async fn count_user_mounts(db: &DatabaseConnection, user_id: &str) -> VfsCommonResult<u64> {
        remote_mount::Entity::find()
            .filter(remote_mount::Column::UserId.eq(user_id))
            .count(db)
            .await
            .map_err(VfsCommonError::from)
    }

    pub async fn get_user_mount(
        db: &DatabaseConnection,
        user_id: &str,
        mount_id: &str,
    ) -> VfsCommonResult<Option<RemoteMountSnapshot>> {
        let row = remote_mount::Entity::find_by_id(mount_id.to_string())
            .filter(remote_mount::Column::UserId.eq(user_id))
            .one(db)
            .await?;
        Ok(row.map(|model| RemoteMountSnapshot {
            options: Self::decode_options(&model.options_json),
            model,
        }))
    }

    pub async fn create_mount(
        db: &DatabaseConnection,
        mount: NewRemoteMount,
    ) -> VfsCommonResult<remote_mount::Model> {
        let now = Utc::now();
        let options_json = Self::encode_options(&mount.options)?;
        let next_sync_at = Self::schedule_time(
            mount.sync_interval_minutes,
            mount.enable,
            mount.sync_peer_dir.as_deref(),
        );
        remote_mount::ActiveModel {
            id: Set(mount.id),
            user_id: Set(mount.user_id),
            name: Set(mount.name),
            driver: Set(mount.driver),
            root: Set(mount.root),
            mount_dir: Set(mount.mount_dir),
            sync_peer_dir: Set(mount.sync_peer_dir),
            sync_mode: Set(mount.sync_mode),
            sync_interval_minutes: Set(mount.sync_interval_minutes),
            sync_timeout_secs: Set(mount.sync_timeout_secs),
            enable: Set(mount.enable),
            options_json: Set(options_json),
            last_sync_at: Set(None),
            next_sync_at: Set(next_sync_at),
            last_sync_status: Set(None),
            last_error: Set(None),
            created_at: Set(now),
            updated_at: Set(now),
        }
        .insert(db)
        .await
        .map_err(VfsCommonError::from)
    }

    pub async fn update_mount(
        db: &DatabaseConnection,
        user_id: &str,
        mount_id: &str,
        patch: RemoteMountUpdatePatch,
    ) -> VfsCommonResult<Option<remote_mount::Model>> {
        let Some(existing) = remote_mount::Entity::find_by_id(mount_id.to_string())
            .filter(remote_mount::Column::UserId.eq(user_id))
            .one(db)
            .await?
        else {
            return Ok(None);
        };

        let mut active: remote_mount::ActiveModel = existing.clone().into();
        let mut next_sync_peer_dir = existing.sync_peer_dir.clone();
        let mut next_sync_interval_minutes = existing.sync_interval_minutes;
        let mut next_enable = existing.enable;
        if let Some(value) = patch.name {
            active.name = Set(value);
        }
        if let Some(value) = patch.driver {
            active.driver = Set(value);
        }
        if let Some(value) = patch.root {
            active.root = Set(value);
        }
        if let Some(value) = patch.mount_dir {
            active.mount_dir = Set(value);
        }
        if let Some(value) = patch.sync_peer_dir {
            next_sync_peer_dir = value.clone();
            active.sync_peer_dir = Set(value);
        }
        if let Some(value) = patch.sync_mode {
            active.sync_mode = Set(value);
        }
        if let Some(value) = patch.sync_interval_minutes {
            next_sync_interval_minutes = value;
            active.sync_interval_minutes = Set(value);
        }
        if let Some(value) = patch.sync_timeout_secs {
            active.sync_timeout_secs = Set(value);
        }
        if let Some(value) = patch.enable {
            next_enable = value;
            active.enable = Set(value);
        }
        if let Some(options) = patch.options {
            active.options_json = Set(Self::encode_options(&options)?);
        }
        if patch.reset_schedule {
            active.next_sync_at = Set(Self::schedule_time(
                next_sync_interval_minutes,
                next_enable,
                next_sync_peer_dir.as_deref(),
            ));
            active.last_sync_status = Set(None);
            active.last_error = Set(None);
        }
        active.updated_at = Set(Utc::now());
        active
            .update(db)
            .await
            .map(Some)
            .map_err(VfsCommonError::from)
    }

    pub async fn delete_mount(
        db: &DatabaseConnection,
        user_id: &str,
        mount_id: &str,
    ) -> VfsCommonResult<bool> {
        let result = remote_mount::Entity::delete_many()
            .filter(remote_mount::Column::Id.eq(mount_id))
            .filter(remote_mount::Column::UserId.eq(user_id))
            .exec(db)
            .await?;
        Ok(result.rows_affected > 0)
    }

    pub async fn list_due_mounts(
        db: &DatabaseConnection,
        now: chrono::DateTime<chrono::Utc>,
    ) -> VfsCommonResult<Vec<RemoteMountSnapshot>> {
        let rows = remote_mount::Entity::find()
            .filter(remote_mount::Column::Enable.eq(true))
            .filter(remote_mount::Column::SyncPeerDir.is_not_null())
            .filter(
                Condition::any()
                    .add(remote_mount::Column::NextSyncAt.is_null())
                    .add(remote_mount::Column::NextSyncAt.lte(now)),
            )
            .order_by_asc(remote_mount::Column::UpdatedAt)
            .all(db)
            .await?;
        Ok(rows
            .into_iter()
            .map(|model| RemoteMountSnapshot {
                options: Self::decode_options(&model.options_json),
                model,
            })
            .collect())
    }

    pub async fn mark_sync_running(db: &DatabaseConnection, mount_id: &str) -> VfsCommonResult<()> {
        remote_mount::Entity::update_many()
            .col_expr(
                remote_mount::Column::LastSyncStatus,
                sea_orm::sea_query::Expr::value(Some("running")),
            )
            .col_expr(
                remote_mount::Column::LastError,
                sea_orm::sea_query::Expr::value(Option::<String>::None),
            )
            .col_expr(
                remote_mount::Column::UpdatedAt,
                sea_orm::sea_query::Expr::value(Utc::now()),
            )
            .filter(remote_mount::Column::Id.eq(mount_id))
            .exec(db)
            .await?;
        Ok(())
    }

    pub async fn mark_sync_finished(
        db: &DatabaseConnection,
        mount_id: &str,
        status: &str,
        error: Option<String>,
        next_sync_at: Option<chrono::DateTime<chrono::Utc>>,
    ) -> VfsCommonResult<()> {
        let now = Utc::now();
        remote_mount::Entity::update_many()
            .col_expr(
                remote_mount::Column::LastSyncAt,
                sea_orm::sea_query::Expr::value(Some(now)),
            )
            .col_expr(
                remote_mount::Column::LastSyncStatus,
                sea_orm::sea_query::Expr::value(Some(status.to_string())),
            )
            .col_expr(
                remote_mount::Column::LastError,
                sea_orm::sea_query::Expr::value(error),
            )
            .col_expr(
                remote_mount::Column::NextSyncAt,
                sea_orm::sea_query::Expr::value(next_sync_at),
            )
            .col_expr(
                remote_mount::Column::UpdatedAt,
                sea_orm::sea_query::Expr::value(now),
            )
            .filter(remote_mount::Column::Id.eq(mount_id))
            .exec(db)
            .await?;
        Ok(())
    }
}
