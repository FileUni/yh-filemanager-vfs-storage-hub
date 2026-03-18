use super::super::entities::user_settings;
use super::VfsCommonResult;
use chrono::Utc;
use sea_orm::*;
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
        user_settings::Entity::update_many()
            .col_expr(
                user_settings::Column::S3AccessKey,
                sea_orm::sea_query::Expr::value(Some(access_key.as_str())),
            )
            .col_expr(
                user_settings::Column::S3SecretKey,
                sea_orm::sea_query::Expr::value(Some(secret_key.as_str())),
            )
            .col_expr(
                user_settings::Column::UpdatedAt,
                sea_orm::sea_query::Expr::value(Utc::now()),
            )
            .filter(user_settings::Column::UserId.eq(settings.user_id.as_str()))
            .exec(db)
            .await?;
        Ok((access_key, secret_key))
    }
}
