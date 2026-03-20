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
}

#[derive(Debug, Clone)]
pub struct S3CredentialLookup {
    pub user_id: String,
    pub secret_key: Option<String>,
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
