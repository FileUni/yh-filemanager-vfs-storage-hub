use super::VfsCommonResult;
use crate::business::entities::{file_index, file_share};
use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
};
use chrono::Utc;
use rand::{Rng, distributions::Alphanumeric};
use sea_orm::{sea_query::Expr, *};
pub struct ShareService;

#[derive(Debug, Clone, Copy, Default)]
pub struct SharePermissions {
    pub enable_direct: bool,
    pub can_upload: bool,
    pub can_update_no_create: bool,
    pub can_delete: bool,
}

#[derive(Debug)]
pub struct CreateShareParams<'a> {
    pub user_id: &'a str,
    pub file_index_id: &'a str,
    pub password: Option<String>,
    pub expire_days: Option<u64>,
    pub max_downloads: Option<i64>,
    pub permissions: SharePermissions,
    pub note: Option<String>,
    pub label: Option<String>,
    pub attributes: Option<String>,
    pub hide_download: bool,
}

#[derive(Debug)]
pub struct UpdateShareParams<'a> {
    pub user_id: &'a str,
    pub id: &'a str,
    pub password: Option<Option<String>>, // Some(Some(p)) = new password, Some(None) = remove password, None = no change
    pub expire_days: Option<Option<u64>>,
    pub max_downloads: Option<Option<i64>>,
    pub permissions: UpdateSharePermissions,
    pub note: Option<Option<String>>,
    pub label: Option<Option<String>>,
    pub attributes: Option<Option<String>>,
    pub hide_download: Option<bool>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct UpdateSharePermissions {
    pub enable_direct: Option<bool>,
    pub can_upload: Option<bool>,
    pub can_update_no_create: Option<bool>,
    pub can_delete: Option<bool>,
}
/// Share query options
#[derive(Debug, Clone, Default)]
pub struct ShareQueryOptions {
    pub page: u64,
    pub page_size: u64,
    pub keyword: Option<String>,
    pub sort_by: Option<String>,
    pub order: Option<String>,
    pub has_password: Option<bool>,
    pub enable_direct: Option<bool>,
}
impl ShareService {
    pub fn fallback_file_from_share_snapshot(
        share: &file_share::Model,
    ) -> Option<file_index::Model> {
        let path = share.snapshot_path.clone()?;
        let name = share.snapshot_name.clone().unwrap_or_else(|| {
            std::path::Path::new(&path)
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or("file")
                .to_string()
        });
        let parent_path = std::path::Path::new(&path)
            .parent()
            .and_then(|value| value.to_str())
            .filter(|value| !value.is_empty())
            .unwrap_or("/")
            .to_string();
        Some(file_index::Model {
            id: share.file_index_id.clone(),
            user_id: share.user_id.clone(),
            parent_path,
            name,
            path,
            is_dir: share.snapshot_is_dir.unwrap_or(false),
            storage_id: None,
            backend_type: None,
            backend_key: None,
            size: 0,
            etag: None,
            file_created_at: None,
            file_updated_at: None,
            file_trashed_at: None,
            row_created_at: share.created_at,
            row_updated_at: share.created_at,
            row_deleted_at: None,
            favorite_color: 0,
            original_path: None,
            remark: None,
        })
    }

    fn hash_password(password: &str) -> VfsCommonResult<String> {
        let mut rng = rand::thread_rng();
        let salt = SaltString::generate(&mut rng);
        let argon2 = Argon2::default();
        Ok(argon2
            .hash_password(password.as_bytes(), &salt)
            .map_err(|e| DbErr::Custom(format!("Password hashing failed: {}", e)))?
            .to_string())
    }
    ///()
    pub async fn create_share(
        db: &DatabaseConnection,
        params: CreateShareParams<'_>,
    ) -> VfsCommonResult<file_share::Model> {
        //8 ID
        let id: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(8)
            .map(char::from)
            .collect();
        let expire_at = params
            .expire_days
            .map(|days| Utc::now() + chrono::Duration::days(days as i64))
            .map(|dt| dt.into());
        let is_public = params.password.is_none();
        let password_hash = if let Some(pwd) = params.password {
            Some(Self::hash_password(&pwd)?)
        } else {
            None
        };
        let snapshot = file_index::Entity::find()
            .filter(file_index::Column::UserId.eq(params.user_id))
            .filter(file_index::Column::Id.eq(params.file_index_id))
            .one(db)
            .await?;
        let model = file_share::ActiveModel {
            id: Set(id),
            file_index_id: Set(params.file_index_id.to_string()),
            user_id: Set(params.user_id.to_string()),
            password: Set(password_hash),
            expire_at: Set(expire_at),
            is_public: Set(is_public),
            view_count: Set(0),
            max_downloads: Set(params.max_downloads),
            enable_direct: Set(params.permissions.enable_direct),
            can_upload: Set(params.permissions.can_upload),
            can_update_no_create: Set(params.permissions.can_update_no_create),
            can_delete: Set(params.permissions.can_delete),
            note: Set(params.note),
            label: Set(params.label),
            attributes: Set(params.attributes),
            hide_download: Set(params.hide_download),
            snapshot_path: Set(snapshot.as_ref().map(|file| file.path.clone())),
            snapshot_name: Set(snapshot.as_ref().map(|file| file.name.clone())),
            snapshot_is_dir: Set(snapshot.as_ref().map(|file| file.is_dir)),
            is_deleted: Set(false),
            created_at: Set(Utc::now().into()),
        };
        Ok(model.insert(db).await?)
    }
    /// Update share
    pub async fn update_share(
        db: &DatabaseConnection,
        params: UpdateShareParams<'_>,
    ) -> VfsCommonResult<file_share::Model> {
        let mut share: file_share::ActiveModel = file_share::Entity::find_by_id(params.id)
            .filter(file_share::Column::UserId.eq(params.user_id))
            .filter(file_share::Column::IsDeleted.eq(false))
            .one(db)
            .await?
            .ok_or(DbErr::Custom("Share not found".to_owned()))?
            .into();
        if let Some(pwd_opt) = params.password {
            match pwd_opt {
                Some(pwd) if !pwd.is_empty() => {
                    share.password = Set(Some(Self::hash_password(&pwd)?));
                    share.is_public = Set(false);
                }
                _ => {
                    // Explicit null or empty string removes password
                    share.password = Set(None);
                    share.is_public = Set(true);
                }
            }
        }
        if let Some(days_opt) = params.expire_days {
            if let Some(days) = days_opt {
                let expire_at = Utc::now() + chrono::Duration::days(days as i64);
                share.expire_at = Set(Some(expire_at.into()));
            } else {
                share.expire_at = Set(None);
            }
        }
        if let Some(max_opt) = params.max_downloads {
            share.max_downloads = Set(max_opt);
        }
        if let Some(v) = params.permissions.enable_direct {
            share.enable_direct = Set(v);
        }
        if let Some(v) = params.permissions.can_upload {
            share.can_upload = Set(v);
        }
        if let Some(v) = params.permissions.can_update_no_create {
            share.can_update_no_create = Set(v);
        }
        if let Some(v) = params.permissions.can_delete {
            share.can_delete = Set(v);
        }
        if let Some(note) = params.note {
            share.note = Set(note);
        }
        if let Some(label) = params.label {
            share.label = Set(label);
        }
        if let Some(attributes) = params.attributes {
            share.attributes = Set(attributes);
        }
        if let Some(hide_download) = params.hide_download {
            share.hide_download = Set(hide_download);
        }
        Ok(share.update(db).await?)
    }
    pub fn verify_password(stored_hash: &str, provided_password: &str) -> bool {
        let Ok(parsed_hash) = PasswordHash::new(stored_hash) else {
            return false;
        };
        Argon2::default()
            .verify_password(provided_password.as_bytes(), &parsed_hash)
            .is_ok()
    }
    #[allow(clippy::manual_unwrap_or)]
    pub async fn list_user_shares_paginated(
        db: &DatabaseConnection,
        user_id: &str,
        options: ShareQueryOptions,
    ) -> VfsCommonResult<(Vec<(file_share::Model, Option<file_index::Model>)>, u64)> {
        let mut query = file_share::Entity::find()
            .find_also_related(file_index::Entity)
            .filter(file_share::Column::UserId.eq(user_id))
            .filter(file_share::Column::IsDeleted.eq(false));
        // Search filter
        if let Some(kw) = &options.keyword
            && !kw.is_empty()
        {
            // Search on related file name or path
            query = query.filter(
                file_index::Column::Name
                    .contains(kw)
                    .or(file_index::Column::Path.contains(kw)),
            );
        }
        // Filter: has password
        if let Some(true) = options.has_password {
            query = query.filter(file_share::Column::Password.is_not_null());
        } else if let Some(false) = options.has_password {
            query = query.filter(file_share::Column::Password.is_null());
        }
        // Filter: enable direct
        if let Some(true) = options.enable_direct {
            query = query.filter(file_share::Column::EnableDirect.eq(true));
        } else if let Some(false) = options.enable_direct {
            query = query.filter(file_share::Column::EnableDirect.eq(false));
        }
        // Sorting
        let mut query = query.order_by_desc(file_index::Column::IsDir);
        let order = match options.order.as_deref() {
            Some(order) => order,
            None => "desc",
        };
        query = match options.sort_by.as_deref() {
            Some("view_count") => {
                if order == "asc" {
                    query.order_by_asc(file_share::Column::ViewCount)
                } else {
                    query.order_by_desc(file_share::Column::ViewCount)
                }
            }
            Some("expire_at") => {
                if order == "asc" {
                    query.order_by_asc(file_share::Column::ExpireAt)
                } else {
                    query.order_by_desc(file_share::Column::ExpireAt)
                }
            }
            Some("name") => {
                if order == "asc" {
                    query.order_by_asc(file_index::Column::Name)
                } else {
                    query.order_by_desc(file_index::Column::Name)
                }
            }
            Some("path") => {
                if order == "asc" {
                    query.order_by_asc(file_index::Column::Path)
                } else {
                    query.order_by_desc(file_index::Column::Path)
                }
            }
            Some("size") => {
                if order == "asc" {
                    query.order_by_asc(file_index::Column::Size)
                } else {
                    query.order_by_desc(file_index::Column::Size)
                }
            }
            _ => {
                if order == "asc" {
                    query.order_by_asc(file_share::Column::CreatedAt)
                } else {
                    query.order_by_desc(file_share::Column::CreatedAt)
                }
            }
        };
        let paginator = query.paginate(db, options.page_size);
        let total = paginator.num_items().await?;
        let items = paginator.fetch_page(options.page.saturating_sub(1)).await?;
        Ok((
            items
                .into_iter()
                .map(|(share, file)| {
                    let file = file.or_else(|| Self::fallback_file_from_share_snapshot(&share));
                    (share, file)
                })
                .collect(),
            total,
        ))
    }
    pub async fn get_share(
        db: &DatabaseConnection,
        id: &str,
    ) -> VfsCommonResult<Option<(file_share::Model, Option<file_index::Model>)>> {
        Ok(file_share::Entity::find_by_id(id)
            .find_also_related(file_index::Entity)
            .filter(file_share::Column::IsDeleted.eq(false))
            .one(db)
            .await?
            .map(|(share, file)| {
                let file = file.or_else(|| Self::fallback_file_from_share_snapshot(&share));
                (share, file)
            }))
    }
    pub async fn increment_view_count(db: &DatabaseConnection, id: &str) -> VfsCommonResult<()> {
        file_share::Entity::update_many()
            .col_expr(
                file_share::Column::ViewCount,
                Expr::col(file_share::Column::ViewCount).add(1),
            )
            .filter(file_share::Column::Id.eq(id))
            .filter(file_share::Column::IsDeleted.eq(false))
            .exec(db)
            .await?;
        Ok(())
    }
    /// Delete share
    pub async fn delete_share(
        db: &DatabaseConnection,
        user_id: &str,
        id: &str,
    ) -> VfsCommonResult<()> {
        file_share::Entity::update_many()
            .col_expr(file_share::Column::IsDeleted, Expr::value(true))
            .filter(file_share::Column::Id.eq(id))
            .filter(file_share::Column::UserId.eq(user_id))
            .exec(db)
            .await?;
        Ok(())
    }
    /// Delete all shares for a user
    pub async fn delete_all_user_shares(
        db: &DatabaseConnection,
        user_id: &str,
    ) -> VfsCommonResult<()> {
        file_share::Entity::update_many()
            .col_expr(file_share::Column::IsDeleted, Expr::value(true))
            .filter(file_share::Column::UserId.eq(user_id))
            .filter(file_share::Column::IsDeleted.eq(false))
            .exec(db)
            .await?;
        Ok(())
    }
}
