use super::super::entities::user_settings;
use super::VfsCommonResult;
use chrono::Utc;
use sea_orm::*;

#[derive(Debug, Clone, Default)]
pub struct UserSettingsUpdatePatch {
    pub pool_name: Option<String>,
    pub base_dir: Option<String>,
    pub storage_quota: Option<i64>,
    pub storage_used: Option<i64>,
    pub thumbnail_disable_text: Option<bool>,
    pub thumbnail_disable_markdown: Option<bool>,
    pub thumbnail_disable_pdf: Option<bool>,
    pub thumbnail_disable_image: Option<bool>,
    pub thumbnail_disable_video: Option<bool>,
    pub thumbnail_disable_audio: Option<bool>,
    pub thumbnail_disable_office: Option<bool>,
    pub thumbnail_disable_tex: Option<bool>,
    pub sftp_enable_password: Option<bool>,
    pub s3_access_key: Option<Option<String>>,
    pub s3_secret_key: Option<Option<String>>,
    pub protected_root: Option<Option<String>>,
    pub protected_mode: Option<Option<String>>,
    pub protected_key_slot_id: Option<Option<String>>,
    pub protected_wrapped_key: Option<Option<String>>,
    pub protected_enabled_at: Option<Option<chrono::DateTime<chrono::Utc>>>,
    pub protected_updated_at: Option<Option<chrono::DateTime<chrono::Utc>>>,
}

#[derive(Debug, Clone)]
pub struct S3CredentialLookup {
    pub user_id: String,
    pub secret_key: Option<String>,
}

#[derive(Debug, Clone)]
pub struct UserSettingsSnapshot {
    pub user_id: String,
    pub pool_name: String,
    pub base_dir: String,
    pub storage_quota: i64,
    pub storage_used: i64,
    pub thumbnail_disable_text: bool,
    pub thumbnail_disable_markdown: bool,
    pub thumbnail_disable_pdf: bool,
    pub thumbnail_disable_image: bool,
    pub thumbnail_disable_video: bool,
    pub thumbnail_disable_audio: bool,
    pub thumbnail_disable_office: bool,
    pub thumbnail_disable_tex: bool,
    pub sftp_enable_password: bool,
    pub s3_access_key: Option<String>,
    pub s3_secret_key: Option<String>,
    pub protected_root: Option<String>,
    pub protected_mode: Option<String>,
    pub protected_key_slot_id: Option<String>,
    pub protected_wrapped_key: Option<String>,
    pub protected_enabled_at: Option<chrono::DateTime<chrono::Utc>>,
    pub protected_updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<&user_settings::Model> for UserSettingsSnapshot {
    fn from(model: &user_settings::Model) -> Self {
        Self {
            user_id: model.user_id.clone(),
            pool_name: model.pool_name.clone(),
            base_dir: model.base_dir.clone(),
            storage_quota: model.storage_quota,
            storage_used: model.storage_used,
            thumbnail_disable_text: model.thumbnail_disable_text,
            thumbnail_disable_markdown: model.thumbnail_disable_markdown,
            thumbnail_disable_pdf: model.thumbnail_disable_pdf,
            thumbnail_disable_image: model.thumbnail_disable_image,
            thumbnail_disable_video: model.thumbnail_disable_video,
            thumbnail_disable_audio: model.thumbnail_disable_audio,
            thumbnail_disable_office: model.thumbnail_disable_office,
            thumbnail_disable_tex: model.thumbnail_disable_tex,
            sftp_enable_password: model.sftp_enable_password,
            s3_access_key: model.s3_access_key.clone(),
            s3_secret_key: model.s3_secret_key.clone(),
            protected_root: model.protected_root.clone(),
            protected_mode: model.protected_mode.clone(),
            protected_key_slot_id: model.protected_key_slot_id.clone(),
            protected_wrapped_key: model.protected_wrapped_key.clone(),
            protected_enabled_at: model.protected_enabled_at,
            protected_updated_at: model.protected_updated_at,
        }
    }
}

impl UserSettingsSnapshot {
    pub fn protected_root_trimmed(&self) -> Option<&str> {
        self.protected_root
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
    }

    pub fn is_protected_subdir_root(&self) -> bool {
        self.protected_root_trimmed()
            .is_some_and(|root| root != "/")
    }

    pub fn matches_protected_root(&self, path: &str) -> bool {
        let Some(root) = self.protected_root_trimmed() else {
            return false;
        };
        let normalized_path = path.trim_end_matches('/');
        let normalized_path = if normalized_path.is_empty() {
            "/"
        } else {
            normalized_path
        };
        let normalized_root = root.trim_end_matches('/');
        let normalized_root = if normalized_root.is_empty() {
            "/"
        } else {
            normalized_root
        };
        if normalized_root == "/" {
            return true;
        }
        normalized_path == normalized_root
            || normalized_path.starts_with(&format!("{}/", normalized_root))
    }
}

pub struct UserSettingsService;
impl UserSettingsService {
    ///()
    pub async fn upsert_user_settings(
        db: &DatabaseConnection,
        user_id: &str,
        pool_name: &str,
        base_dir: &str,
        storage_quota: i64,
    ) -> VfsCommonResult<user_settings::Model> {
        let now = Utc::now();
        let model = user_settings::ActiveModel {
            user_id: Set(user_id.to_string()),
            pool_name: Set(pool_name.to_string()),
            base_dir: Set(base_dir.to_string()),
            storage_quota: Set(storage_quota),
            storage_used: Set(0),
            thumbnail_disable_text: Set(false),
            thumbnail_disable_markdown: Set(false),
            thumbnail_disable_pdf: Set(false),
            thumbnail_disable_image: Set(false),
            thumbnail_disable_video: Set(false),
            thumbnail_disable_audio: Set(false),
            thumbnail_disable_office: Set(false),
            thumbnail_disable_tex: Set(false),
            sftp_enable_password: Set(true),
            s3_access_key: Set(None),
            s3_secret_key: Set(None),
            protected_root: Set(None),
            protected_mode: Set(None),
            protected_key_slot_id: Set(None),
            protected_wrapped_key: Set(None),
            protected_enabled_at: Set(None),
            protected_updated_at: Set(None),
            created_at: Set(now),
            updated_at: Set(now),
        };
        // Use ON CONFLICT for atomicity
        let res = user_settings::Entity::insert(model)
            .on_conflict(
                sea_orm::sea_query::OnConflict::column(user_settings::Column::UserId)
                    .update_column(user_settings::Column::PoolName)
                    .update_column(user_settings::Column::BaseDir)
                    .update_column(user_settings::Column::StorageQuota)
                    .update_column(user_settings::Column::UpdatedAt)
                    .to_owned(),
            )
            .exec_with_returning(db)
            .await?;
        Ok(res)
    }
    pub async fn get_user_settings(
        db: &DatabaseConnection,
        user_id: &str,
    ) -> VfsCommonResult<Option<user_settings::Model>> {
        let settings = user_settings::Entity::find_by_id(user_id).one(db).await?;
        Ok(settings)
    }
    pub async fn update_user_settings_patch(
        db: &DatabaseConnection,
        user_id: &str,
        patch: &UserSettingsUpdatePatch,
    ) -> VfsCommonResult<()> {
        use sea_orm::sea_query::Expr;

        let mut update =
            user_settings::Entity::update_many().filter(user_settings::Column::UserId.eq(user_id));

        if let Some(pool_name) = patch.pool_name.as_deref() {
            update = update.col_expr(user_settings::Column::PoolName, Expr::value(pool_name));
        }
        if let Some(base_dir) = patch.base_dir.as_deref() {
            update = update.col_expr(user_settings::Column::BaseDir, Expr::value(base_dir));
        }
        if let Some(storage_quota) = patch.storage_quota {
            update = update.col_expr(
                user_settings::Column::StorageQuota,
                Expr::value(storage_quota),
            );
        }
        if let Some(storage_used) = patch.storage_used {
            update = update.col_expr(
                user_settings::Column::StorageUsed,
                Expr::value(storage_used),
            );
        }
        if let Some(value) = patch.thumbnail_disable_text {
            update = update.col_expr(
                user_settings::Column::ThumbnailDisableText,
                Expr::value(value),
            );
        }
        if let Some(value) = patch.thumbnail_disable_markdown {
            update = update.col_expr(
                user_settings::Column::ThumbnailDisableMarkdown,
                Expr::value(value),
            );
        }
        if let Some(value) = patch.thumbnail_disable_pdf {
            update = update.col_expr(
                user_settings::Column::ThumbnailDisablePdf,
                Expr::value(value),
            );
        }
        if let Some(value) = patch.thumbnail_disable_image {
            update = update.col_expr(
                user_settings::Column::ThumbnailDisableImage,
                Expr::value(value),
            );
        }
        if let Some(value) = patch.thumbnail_disable_video {
            update = update.col_expr(
                user_settings::Column::ThumbnailDisableVideo,
                Expr::value(value),
            );
        }
        if let Some(value) = patch.thumbnail_disable_audio {
            update = update.col_expr(
                user_settings::Column::ThumbnailDisableAudio,
                Expr::value(value),
            );
        }
        if let Some(value) = patch.thumbnail_disable_office {
            update = update.col_expr(
                user_settings::Column::ThumbnailDisableOffice,
                Expr::value(value),
            );
        }
        if let Some(value) = patch.thumbnail_disable_tex {
            update = update.col_expr(
                user_settings::Column::ThumbnailDisableTex,
                Expr::value(value),
            );
        }
        if let Some(value) = patch.sftp_enable_password {
            update = update.col_expr(
                user_settings::Column::SftpEnablePassword,
                Expr::value(value),
            );
        }
        if let Some(value) = &patch.s3_access_key {
            update = update.col_expr(
                user_settings::Column::S3AccessKey,
                Expr::value(value.as_deref()),
            );
        }
        if let Some(value) = &patch.s3_secret_key {
            update = update.col_expr(
                user_settings::Column::S3SecretKey,
                Expr::value(value.as_deref()),
            );
        }
        if let Some(value) = &patch.protected_root {
            update = update.col_expr(
                user_settings::Column::ProtectedRoot,
                Expr::value(value.as_deref()),
            );
        }
        if let Some(value) = &patch.protected_mode {
            update = update.col_expr(
                user_settings::Column::ProtectedMode,
                Expr::value(value.as_deref()),
            );
        }
        if let Some(value) = &patch.protected_key_slot_id {
            update = update.col_expr(
                user_settings::Column::ProtectedKeySlotId,
                Expr::value(value.as_deref()),
            );
        }
        if let Some(value) = &patch.protected_wrapped_key {
            update = update.col_expr(
                user_settings::Column::ProtectedWrappedKey,
                Expr::value(value.as_deref()),
            );
        }
        if let Some(value) = &patch.protected_enabled_at {
            update = update.col_expr(
                user_settings::Column::ProtectedEnabledAt,
                Expr::value(*value),
            );
        }
        if let Some(value) = &patch.protected_updated_at {
            update = update.col_expr(
                user_settings::Column::ProtectedUpdatedAt,
                Expr::value(*value),
            );
        }

        update = update.col_expr(user_settings::Column::UpdatedAt, Expr::value(Utc::now()));
        update.exec(db).await?;
        Ok(())
    }
    pub async fn update_storage_quota(
        db: &DatabaseConnection,
        user_id: &str,
        new_quota: i64,
    ) -> VfsCommonResult<()> {
        let settings = user_settings::ActiveModel {
            user_id: Set(user_id.to_string()),
            storage_quota: Set(new_quota),
            updated_at: Set(Utc::now()),
            ..Default::default()
        };
        settings.update(db).await?;
        Ok(())
    }
    /// Update used storage (Atomic SQL operation)
    pub async fn update_storage_used(
        db: &DatabaseConnection,
        user_id: &str,
        delta: i64,
    ) -> VfsCommonResult<()> {
        use sea_orm::sea_query::Expr;
        // Use atomic update to prevent race conditions
        user_settings::Entity::update_many()
            .col_expr(
                user_settings::Column::StorageUsed,
                Expr::col(user_settings::Column::StorageUsed).add(delta),
            )
            .col_expr(user_settings::Column::UpdatedAt, Expr::value(Utc::now()))
            .filter(user_settings::Column::UserId.eq(user_id))
            .exec(db)
            .await?;
        Ok(())
    }
    pub async fn check_quota_exceeded(
        db: &DatabaseConnection,
        user_id: &str,
        additional_size: i64,
    ) -> VfsCommonResult<bool> {
        if let Some(settings) = user_settings::Entity::find_by_id(user_id).one(db).await? {
            if settings.storage_quota == 0 {
                return Ok(false);
            }
            let new_total = settings.storage_used + additional_size;
            return Ok(new_total > settings.storage_quota);
        }
        Ok(false)
    }
    pub async fn list_all_settings(
        db: &DatabaseConnection,
    ) -> VfsCommonResult<Vec<user_settings::Model>> {
        let settings = user_settings::Entity::find().all(db).await?;
        Ok(settings)
    }
    pub async fn delete_user_settings(
        db: &DatabaseConnection,
        user_id: &str,
    ) -> VfsCommonResult<()> {
        user_settings::Entity::delete_by_id(user_id)
            .exec(db)
            .await?;
        Ok(())
    }
    pub async fn find_s3_credentials_by_access_key(
        db: &DatabaseConnection,
        access_key: &str,
    ) -> VfsCommonResult<Option<S3CredentialLookup>> {
        use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QuerySelect};

        let row = user_settings::Entity::find()
            .select_only()
            .column(user_settings::Column::UserId)
            .column(user_settings::Column::S3SecretKey)
            .filter(user_settings::Column::S3AccessKey.eq(access_key))
            .into_tuple::<(String, Option<String>)>()
            .one(db)
            .await?;

        Ok(row.map(|(user_id, secret_key)| S3CredentialLookup {
            user_id,
            secret_key,
        }))
    }
    /// Regenerate S3 keys
    pub async fn regenerate_s3_keys(
        db: &DatabaseConnection,
        user_id: &str,
    ) -> VfsCommonResult<(String, String)> {
        use rand::{Rng, distributions::Alphanumeric};
        let access_key: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(16)
            .map(char::from)
            .collect();
        let secret_key: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(32)
            .map(char::from)
            .collect();
        let settings = user_settings::Entity::find_by_id(user_id)
            .one(db)
            .await?
            .ok_or_else(|| sea_orm::DbErr::RecordNotFound("User settings not found".to_string()))?;
        let patch = UserSettingsUpdatePatch {
            s3_access_key: Some(Some(access_key.clone())),
            s3_secret_key: Some(Some(secret_key.clone())),
            ..Default::default()
        };
        Self::update_user_settings_patch(db, settings.user_id.as_str(), &patch).await?;
        Ok((access_key, secret_key))
    }
}

#[cfg(test)]
mod tests {
    use super::UserSettingsSnapshot;

    fn snapshot(root: Option<&str>) -> UserSettingsSnapshot {
        UserSettingsSnapshot {
            user_id: "user-a".to_string(),
            pool_name: "default".to_string(),
            base_dir: "/".to_string(),
            storage_quota: 0,
            storage_used: 0,
            thumbnail_disable_image: false,
            thumbnail_disable_video: false,
            thumbnail_disable_audio: false,
            thumbnail_disable_pdf: false,
            thumbnail_disable_text: false,
            thumbnail_disable_markdown: false,
            thumbnail_disable_office: false,
            thumbnail_disable_tex: false,
            sftp_enable_password: true,
            s3_access_key: None,
            s3_secret_key: None,
            protected_root: root.map(str::to_string),
            protected_mode: None,
            protected_key_slot_id: None,
            protected_wrapped_key: None,
            protected_enabled_at: None,
            protected_updated_at: None,
        }
    }

    #[test]
    fn root_protected_matches_all_user_paths() {
        let settings = snapshot(Some("/"));
        assert!(settings.matches_protected_root("/"));
        assert!(settings.matches_protected_root("/docs/a.txt"));
        assert!(settings.matches_protected_root("/.thumbs/file.webp"));
    }

    #[test]
    fn subdir_protected_matches_only_subtree() {
        let settings = snapshot(Some("/private"));
        assert!(settings.matches_protected_root("/private"));
        assert!(settings.matches_protected_root("/private/docs/a.txt"));
        assert!(!settings.matches_protected_root("/public/docs/a.txt"));
    }
}
