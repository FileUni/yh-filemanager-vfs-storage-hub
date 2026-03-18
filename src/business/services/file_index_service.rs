use crate::business::entities::file_index;
use crate::vfs::VfsPaginationParams;
use futures::StreamExt;
use sea_orm::sea_query::{Expr, Func, SimpleExpr};
use sea_orm::*;
use std::sync::Arc;
#[derive(Clone)]
pub struct FileIndexService {
    db: Arc<DatabaseConnection>,
}
impl FileIndexService {
    pub fn new(db: Arc<DatabaseConnection>) -> Self {
        Self { db }
    }
    /// Insert or update file entry
    pub async fn add_or_update_entry(
        &self,
        entry: file_index::ActiveModel,
    ) -> Result<file_index::Model, DbErr> {
        file_index::Entity::insert(entry)
            .on_conflict(
                sea_orm::sea_query::OnConflict::columns(vec![
                    file_index::Column::UserId,
                    file_index::Column::Path,
                ])
                .update_columns(vec![
                    file_index::Column::ParentPath,
                    file_index::Column::Name,
                    file_index::Column::IsDir,
                    file_index::Column::Size,
                    file_index::Column::Etag,
                    file_index::Column::FileUpdatedAt,
                    file_index::Column::RowUpdatedAt,
                    file_index::Column::RowDeletedAt,
                    file_index::Column::FileTrashedAt,
                    file_index::Column::OriginalPath,
                    //favorite_color is intentionally excluded to preserve user metadata during physical sync
                ])
                .value(
                    file_index::Column::RowDeletedAt,
                    Option::<chrono::NaiveDateTime>::None,
                )
                .value(
                    file_index::Column::FileTrashedAt,
                    Option::<chrono::NaiveDateTime>::None,
                )
                .value(file_index::Column::OriginalPath, Option::<String>::None)
                .to_owned(),
            )
            .exec_with_returning(&*self.db)
            .await
    }
    /// High-level wrapper: upsert file index
    pub async fn upsert_file(
        &self,
        user_id: &str,
        logical_path: &str,
        info: &crate::vfs::VfsFileInfo,
    ) -> Result<file_index::Model, DbErr> {
        let parent = std::path::Path::new(logical_path)
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|| "/".to_string());
        let parent = if parent.is_empty() {
            "/".to_string()
        } else {
            parent
        };
        let now = chrono::Utc::now();
        let active = file_index::ActiveModel {
            id: Set(uuid::Uuid::new_v4().to_string()),
            user_id: Set(user_id.to_string()),
            parent_path: Set(parent),
            name: Set(info.name.to_string()),
            path: Set(logical_path.to_string()),
            is_dir: Set(info.is_dir),
            size: Set(info.size as i64),
            file_updated_at: Set(info.modified.map(|dt| dt.into())),
            row_updated_at: Set(now.into()),
            ..Default::default()
        };
        self.add_or_update_entry(active).await
    }
    /// List files in directory
    pub async fn list_files(
        &self,
        user_id: &str,
        parent_path: &str,
    ) -> Result<Vec<file_index::Model>, DbErr> {
        file_index::Entity::find()
            .filter(file_index::Column::UserId.eq(user_id))
            .filter(file_index::Column::ParentPath.eq(parent_path))
            .filter(file_index::Column::RowDeletedAt.is_null())
            .filter(file_index::Column::FileTrashedAt.is_null())
            .all(&*self.db)
            .await
    }
    /// Stream files in directory (pagination implementation for ownable stream)
    pub fn list_files_stream(
        &self,
        user_id: String,
        parent_path: String,
    ) -> impl futures::Stream<Item = Result<file_index::Model, DbErr>> + 'static {
        let db = Arc::clone(&self.db);
        futures::stream::unfold(
            (user_id, parent_path, 1u64, true),
            move |(uid, pp, page, has_more)| {
                let db = Arc::clone(&db);
                async move {
                    if !has_more {
                        return None;
                    }
                    let offset = (page - 1) * 100;
                    let res = file_index::Entity::find()
                        .filter(file_index::Column::UserId.eq(uid.as_str()))
                        .filter(file_index::Column::ParentPath.eq(pp.as_str()))
                        .filter(file_index::Column::RowDeletedAt.is_null())
                        .filter(file_index::Column::FileTrashedAt.is_null())
                        .order_by_desc(file_index::Column::IsDir)
                        .order_by_asc(file_index::Column::Name)
                        .limit(100)
                        .offset(offset)
                        .all(&*db)
                        .await;
                    match res {
                        Ok(items) => {
                            let count = items.len();
                            let next_has_more = count == 100;
                            let next_state = (uid, pp, page + 1, next_has_more);
                            let results: Vec<Result<file_index::Model, DbErr>> =
                                items.into_iter().map(Ok).collect();
                            Some((futures::stream::iter(results), next_state))
                        }
                        Err(e) => {
                            Some((futures::stream::iter(vec![Err(e)]), (uid, pp, page, false)))
                        }
                    }
                }
            },
        )
        .flatten()
    }
    /// Count files in directory
    pub async fn count_files(&self, user_id: &str, parent_path: &str) -> Result<i64, DbErr> {
        let count = file_index::Entity::find()
            .filter(file_index::Column::UserId.eq(user_id))
            .filter(file_index::Column::ParentPath.eq(parent_path))
            .filter(file_index::Column::RowDeletedAt.is_null())
            .filter(file_index::Column::FileTrashedAt.is_null())
            .count(&*self.db)
            .await?;
        Ok(count as i64)
    }
    /// List files in directory (paginated)
    pub async fn list_files_paginated(
        &self,
        user_id: &str,
        parent_path: &str,
        page: i64,
        page_size: i64,
    ) -> Result<(Vec<file_index::Model>, i64), DbErr> {
        let offset = (page - 1) * page_size;
        let files = file_index::Entity::find()
            .filter(file_index::Column::UserId.eq(user_id))
            .filter(file_index::Column::ParentPath.eq(parent_path))
            .filter(file_index::Column::RowDeletedAt.is_null())
            .filter(file_index::Column::FileTrashedAt.is_null())
            .order_by_desc(file_index::Column::IsDir) // Folders first
            .order_by_asc(file_index::Column::Name) // ，
            .limit(page_size as u64)
            .offset(offset as u64)
            .all(&*self.db)
            .await?;
        let total = self.count_files(user_id, parent_path).await?;
        Ok((files, total))
    }
    /// List files in directory (paginated, with sorting and search)
    #[allow(clippy::manual_unwrap_or)]
    pub async fn list_files_paginated_with_sort(
        &self,
        user_id: &str,
        parent_path: &str,
        params: VfsPaginationParams<'_>,
    ) -> Result<(Vec<file_index::Model>, i64), DbErr> {
        let offset = (params.page - 1) * params.page_size;
        let mut query = file_index::Entity::find()
            .filter(file_index::Column::UserId.eq(user_id))
            .filter(file_index::Column::ParentPath.eq(parent_path))
            .filter(file_index::Column::RowDeletedAt.is_null())
            .filter(file_index::Column::FileTrashedAt.is_null());
        // Search filter
        if let Some(kw) = params.keyword
            && !kw.is_empty()
        {
            let kw_str: &str = kw;
            query = query.filter(
                file_index::Column::Name
                    .contains(kw_str)
                    .or(file_index::Column::Path.contains(kw_str)),
            );
        }
        // Sorting
        let sort_col = match params.sort_by {
            Some("size") => file_index::Column::Size,
            Some("modified") => file_index::Column::FileUpdatedAt,
            Some("path") => file_index::Column::Path,
            Some("created_at") => file_index::Column::RowUpdatedAt,
            _ => file_index::Column::Name,
        };
        let order = match params.order {
            Some(order) => order,
            None => "asc",
        };
        let is_desc = order == "desc";
        query = query.order_by_desc(file_index::Column::IsDir);
        query = if is_desc {
            query.order_by_desc(sort_col)
        } else {
            query.order_by_asc(sort_col)
        };
        let files = query
            .limit(params.page_size as u64)
            .offset(offset as u64)
            .all(&*self.db)
            .await?;
        let total = self
            .count_files_with_filter(user_id, parent_path, params.keyword)
            .await?;
        Ok((files, total))
    }
    /// Count files (with search filter)
    async fn count_files_with_filter(
        &self,
        user_id: &str,
        parent_path: &str,
        keyword: Option<&str>,
    ) -> Result<i64, DbErr> {
        let mut query = file_index::Entity::find()
            .filter(file_index::Column::UserId.eq(user_id))
            .filter(file_index::Column::ParentPath.eq(parent_path))
            .filter(file_index::Column::RowDeletedAt.is_null())
            .filter(file_index::Column::FileTrashedAt.is_null());
        if let Some(kw) = keyword
            && !kw.is_empty()
        {
            let kw_str: &str = kw;
            query = query.filter(
                file_index::Column::Name
                    .contains(kw_str)
                    .or(file_index::Column::Path.contains(kw_str)),
            );
        }
        let count = query.count(&*self.db).await?;
        Ok(count as i64)
    }
    /// Recursively list all files in directory
    pub async fn list_files_recursive(
        &self,
        user_id: &str,
        parent_path: &str,
    ) -> Result<Vec<file_index::Model>, DbErr> {
        let mut query = file_index::Entity::find()
            .filter(file_index::Column::UserId.eq(user_id))
            .filter(file_index::Column::RowDeletedAt.is_null())
            .filter(file_index::Column::FileTrashedAt.is_null());
        if parent_path != "/" {
            let prefix = format!("{}/", parent_path.trim_end_matches('/'));
            query = query.filter(
                file_index::Column::Path
                    .eq(parent_path)
                    .or(file_index::Column::Path.starts_with(prefix)),
            );
        }
        query.all(&*self.db).await
    }
    /// Search files
    pub async fn search_files(
        &self,
        user_id: &str,
        query: &str,
    ) -> Result<Vec<file_index::Model>, DbErr> {
        file_index::Entity::find()
            .filter(file_index::Column::UserId.eq(user_id))
            .filter(file_index::Column::Name.contains(query))
            .filter(file_index::Column::RowDeletedAt.is_null())
            .filter(file_index::Column::FileTrashedAt.is_null())
            .all(&*self.db)
            .await
    }
    /// Count search results
    pub async fn count_search_files(&self, user_id: &str, query: &str) -> Result<i64, DbErr> {
        let count = file_index::Entity::find()
            .filter(file_index::Column::UserId.eq(user_id))
            .filter(file_index::Column::Name.contains(query))
            .filter(file_index::Column::RowDeletedAt.is_null())
            .filter(file_index::Column::FileTrashedAt.is_null())
            .count(&*self.db)
            .await?;
        Ok(count as i64)
    }
    /// Search files (paginated)
    pub async fn search_files_paginated(
        &self,
        user_id: &str,
        query: &str,
        page: i64,
        page_size: i64,
    ) -> Result<(Vec<file_index::Model>, i64), DbErr> {
        let offset = (page - 1) * page_size;
        let files = file_index::Entity::find()
            .filter(file_index::Column::UserId.eq(user_id))
            .filter(file_index::Column::Name.contains(query))
            .filter(file_index::Column::RowDeletedAt.is_null())
            .filter(file_index::Column::FileTrashedAt.is_null())
            .order_by_desc(file_index::Column::IsDir) // Folders first
            .order_by_asc(file_index::Column::Name)
            .limit(page_size as u64)
            .offset(offset as u64)
            .all(&*self.db)
            .await?;
        let total = self.count_search_files(user_id, query).await?;
        Ok((files, total))
    }
    /// Get file metadata (Stat)
    pub async fn get_file_metadata(
        &self,
        user_id: &str,
        path: &str,
    ) -> Result<Option<file_index::Model>, DbErr> {
        file_index::Entity::find()
            .filter(file_index::Column::UserId.eq(user_id))
            .filter(file_index::Column::Path.eq(path))
            .filter(file_index::Column::RowDeletedAt.is_null())
            .one(&*self.db)
            .await
    }
    /// Get total size (recursive) - Use SQL Sum
    pub async fn get_total_size(&self, user_id: &str, path_prefix: &str) -> Result<i64, DbErr> {
        let condition = file_index::Column::Path
            .eq(path_prefix)
            .or(file_index::Column::Path.starts_with(format!("{}/", path_prefix)));
        let sum_expr: SimpleExpr = Func::sum(Expr::col(file_index::Column::Size)).into();
        let res = file_index::Entity::find()
            .filter(file_index::Column::UserId.eq(user_id))
            .filter(condition)
            .filter(file_index::Column::RowDeletedAt.is_null())
            .filter(file_index::Column::FileTrashedAt.is_null())
            .select_only()
            .column_as(sum_expr, "total_size")
            .into_tuple::<Option<i64>>()
            .one(&*self.db)
            .await?;
        match res.flatten() {
            Some(total) => Ok(total),
            None => Ok(0),
        }
    }
    /// Move to trash (recursively update all child items)
    pub async fn trash_file(
        &self,
        user_id: &str,
        old_path: &str,
        new_path: &str,
    ) -> Result<(), DbErr> {
        let now = chrono::Utc::now();
        let src_prefix = format!("{}/", old_path);
        let txn = self.db.begin().await?;
        // Get top-level node name
        let new_name = if let Some(name) = new_path.split('/').next_back() {
            name.to_string()
        } else {
            String::new()
        };
        // Update top-level node
        file_index::Entity::update_many()
            .filter(file_index::Column::UserId.eq(user_id))
            .filter(file_index::Column::Path.eq(old_path))
            .col_expr(file_index::Column::Path, Expr::value(new_path))
            .col_expr(file_index::Column::Name, Expr::value(new_name))
            .col_expr(file_index::Column::ParentPath, Expr::value("/.recycle_bin"))
            .col_expr(file_index::Column::FileTrashedAt, Expr::value(now))
            .col_expr(file_index::Column::OriginalPath, Expr::value(old_path))
            .col_expr(file_index::Column::RowUpdatedAt, Expr::value(now))
            .exec(&txn)
            .await?;
        // Recursive batch update using SeaQuery replace for multi-db support
        let condition = file_index::Column::UserId
            .eq(user_id)
            .and(file_index::Column::Path.like(format!("{}%", src_prefix)));
        // SeaORM update_many Expr REPLACE
        // REPLACE en PostgreSQL SQLite
        // OriginalPath
        // SQL SET original_path = path
        file_index::Entity::update_many()
            .filter(condition)
            .col_expr(
                file_index::Column::OriginalPath,
                Expr::col(file_index::Column::Path).into(),
            )
            .col_expr(
                file_index::Column::Path,
                Expr::cust_with_exprs(
                    "REPLACE(path, $1, $2)",
                    vec![old_path.into(), new_path.into()],
                ),
            )
            .col_expr(
                file_index::Column::ParentPath,
                Expr::cust_with_exprs(
                    "REPLACE(parent_path, $1, $2)",
                    vec![old_path.into(), new_path.into()],
                ),
            )
            .col_expr(file_index::Column::FileTrashedAt, Expr::value(now))
            .col_expr(file_index::Column::RowUpdatedAt, Expr::value(now))
            .exec(&txn)
            .await?;
        txn.commit().await?;
        Ok(())
    }
    /// Restore from trash (recursively update all child items)
    pub async fn restore_file(
        &self,
        user_id: &str,
        trash_path: &str,
        restored_path: &str,
    ) -> Result<(), DbErr> {
        let now = chrono::Utc::now();
        let trash_prefix = format!("{}/", trash_path);
        let txn = self.db.begin().await?;
        // Get top-level node name
        let new_name = if let Some(name) = restored_path.split('/').next_back() {
            name.to_string()
        } else {
            String::new()
        };
        // Update top-level node
        file_index::Entity::update_many()
            .filter(file_index::Column::UserId.eq(user_id))
            .filter(file_index::Column::Path.eq(trash_path))
            .col_expr(file_index::Column::Path, Expr::value(restored_path))
            .col_expr(file_index::Column::Name, Expr::value(new_name))
            .col_expr(
                file_index::Column::FileTrashedAt,
                Expr::value(Option::<chrono::DateTime<chrono::FixedOffset>>::None),
            )
            .col_expr(
                file_index::Column::OriginalPath,
                Expr::value(Option::<String>::None),
            )
            .col_expr(file_index::Column::RowUpdatedAt, Expr::value(now))
            .exec(&txn)
            .await?;
        // Recursively process children
        let condition = file_index::Column::UserId
            .eq(user_id)
            .and(file_index::Column::Path.like(format!("{}%", trash_prefix)));
        file_index::Entity::update_many()
            .filter(condition)
            .col_expr(
                file_index::Column::Path,
                Expr::cust_with_exprs(
                    "REPLACE(path, $1, $2)",
                    vec![trash_path.into(), restored_path.into()],
                ),
            )
            .col_expr(
                file_index::Column::ParentPath,
                Expr::cust_with_exprs(
                    "REPLACE(parent_path, $1, $2)",
                    vec![trash_path.into(), restored_path.into()],
                ),
            )
            .col_expr(
                file_index::Column::FileTrashedAt,
                Expr::value(Option::<chrono::DateTime<chrono::FixedOffset>>::None),
            )
            .col_expr(
                file_index::Column::OriginalPath,
                Expr::value(Option::<String>::None),
            )
            .col_expr(file_index::Column::RowUpdatedAt, Expr::value(now))
            .exec(&txn)
            .await?;
        txn.commit().await?;
        Ok(())
    }
    /// Physical delete (completely remove from index, including child items)
    pub async fn delete_file(&self, user_id: &str, path: &str) -> Result<(), DbErr> {
        let prefix = format!("{}/", path);
        let txn = self.db.begin().await?;
        file_index::Entity::delete_many()
            .filter(file_index::Column::UserId.eq(user_id))
            .filter(
                file_index::Column::Path
                    .eq(path)
                    .or(file_index::Column::Path.starts_with(&prefix)),
            )
            .exec(&txn)
            .await?;
        txn.commit().await?;
        Ok(())
    }
    /// Move/rename file
    pub async fn move_file(
        &self,
        user_id: &str,
        src_path: &str,
        dst_path: &str,
    ) -> Result<(), DbErr> {
        let now = chrono::Utc::now();
        let src_prefix = format!("{}/", src_path);
        let txn = self.db.begin().await?;
        // Resolve new metadata
        let new_name = if let Some(name) = dst_path.split('/').next_back() {
            name.to_string()
        } else {
            String::new()
        };
        let new_parent = if let Some((parent, _)) = dst_path.rsplit_once('/') {
            parent.to_string()
        } else {
            String::new()
        };
        let new_parent = if new_parent.is_empty() {
            "/".to_string()
        } else {
            new_parent
        };
        // Update top-level node
        let mut update = file_index::Entity::update_many()
            .filter(file_index::Column::UserId.eq(user_id))
            .filter(file_index::Column::Path.eq(src_path))
            .col_expr(file_index::Column::Path, Expr::value(dst_path))
            .col_expr(file_index::Column::Name, Expr::value(new_name))
            .col_expr(file_index::Column::ParentPath, Expr::value(new_parent))
            .col_expr(file_index::Column::RowUpdatedAt, Expr::value(now));
        if !dst_path.starts_with("/.recycle_bin") {
            update = update
                .col_expr(
                    file_index::Column::FileTrashedAt,
                    Expr::value(Option::<chrono::DateTime<chrono::FixedOffset>>::None),
                )
                .col_expr(
                    file_index::Column::OriginalPath,
                    Expr::value(Option::<String>::None),
                );
        }
        update.exec(&txn).await?;
        // Recursive batch update
        let condition = file_index::Column::UserId
            .eq(user_id)
            .and(file_index::Column::Path.like(format!("{}%", src_prefix)));
        let mut update_many = file_index::Entity::update_many()
            .filter(condition)
            .col_expr(
                file_index::Column::Path,
                Expr::cust_with_exprs(
                    "REPLACE(path, $1, $2)",
                    vec![src_path.into(), dst_path.into()],
                ),
            )
            .col_expr(
                file_index::Column::ParentPath,
                Expr::cust_with_exprs(
                    "REPLACE(parent_path, $1, $2)",
                    vec![src_path.into(), dst_path.into()],
                ),
            )
            .col_expr(file_index::Column::RowUpdatedAt, Expr::value(now));
        if !dst_path.starts_with("/.recycle_bin") {
            update_many = update_many
                .col_expr(
                    file_index::Column::FileTrashedAt,
                    Expr::value(Option::<chrono::DateTime<chrono::FixedOffset>>::None),
                )
                .col_expr(
                    file_index::Column::OriginalPath,
                    Expr::value(Option::<String>::None),
                );
        }
        update_many.exec(&txn).await?;
        txn.commit().await?;
        Ok(())
    }
    /// Set favorite color
    pub async fn set_favorite_color(
        &self,
        user_id: &str,
        path: &str,
        color: i32,
    ) -> Result<(), DbErr> {
        file_index::Entity::update_many()
            .col_expr(file_index::Column::FavoriteColor, Expr::value(color))
            .filter(file_index::Column::UserId.eq(user_id))
            .filter(file_index::Column::Path.eq(path))
            .filter(file_index::Column::RowDeletedAt.is_null())
            .exec(&*self.db)
            .await?;
        Ok(())
    }
    /// List all favorite items
    pub async fn list_favorites(
        &self,
        user_id: &str,
        color_filter: Option<i32>,
    ) -> Result<Vec<file_index::Model>, DbErr> {
        let mut query = file_index::Entity::find()
            .filter(file_index::Column::UserId.eq(user_id))
            .filter(file_index::Column::FavoriteColor.gt(0))
            .filter(file_index::Column::FileTrashedAt.is_null()) // Only show non-trashed items
            .filter(file_index::Column::RowDeletedAt.is_null());
        if let Some(color) = color_filter {
            query = query.filter(file_index::Column::FavoriteColor.eq(color));
        }
        query.all(&*self.db).await
    }
    /// List favorite items (paginated, with sorting and search)
    #[allow(clippy::manual_unwrap_or)]
    pub async fn list_favorites_paginated(
        &self,
        user_id: &str,
        params: VfsPaginationParams<'_>,
        color_filter: Option<i32>,
    ) -> Result<(Vec<file_index::Model>, i64), DbErr> {
        let offset = (params.page - 1) * params.page_size;
        let mut query = file_index::Entity::find()
            .filter(file_index::Column::UserId.eq(user_id))
            .filter(file_index::Column::FavoriteColor.gt(0))
            .filter(file_index::Column::FileTrashedAt.is_null())
            .filter(file_index::Column::RowDeletedAt.is_null());
        // Color filter
        if let Some(color) = color_filter
            && color > 0
        {
            query = query.filter(file_index::Column::FavoriteColor.eq(color));
        }
        // Search filter
        if let Some(kw) = params.keyword
            && !kw.is_empty()
        {
            let kw_str: &str = kw;
            query = query.filter(
                file_index::Column::Name
                    .contains(kw_str)
                    .or(file_index::Column::Path.contains(kw_str)),
            );
        }
        // Sorting
        let sort_col = match params.sort_by {
            Some("size") => file_index::Column::Size,
            Some("modified") => file_index::Column::FileUpdatedAt,
            Some("path") => file_index::Column::Path,
            Some("created_at") => file_index::Column::RowUpdatedAt,
            _ => file_index::Column::Name,
        };
        let order = match params.order {
            Some(order) => order,
            None => "asc",
        };
        let is_desc = order == "desc";
        query = query.order_by_desc(file_index::Column::IsDir);
        query = if is_desc {
            query.order_by_desc(sort_col)
        } else {
            query.order_by_asc(sort_col)
        };
        let files = query
            .limit(params.page_size as u64)
            .offset(offset as u64)
            .all(&*self.db)
            .await?;
        let total = self
            .count_favorites_with_filter(user_id, params.keyword, color_filter)
            .await?;
        Ok((files, total))
    }
    /// Count favorite items (with search and color filter)
    async fn count_favorites_with_filter(
        &self,
        user_id: &str,
        keyword: Option<&str>,
        color_filter: Option<i32>,
    ) -> Result<i64, DbErr> {
        let mut query = file_index::Entity::find()
            .filter(file_index::Column::UserId.eq(user_id))
            .filter(file_index::Column::FavoriteColor.gt(0))
            .filter(file_index::Column::FileTrashedAt.is_null())
            .filter(file_index::Column::RowDeletedAt.is_null());
        if let Some(color) = color_filter
            && color > 0
        {
            query = query.filter(file_index::Column::FavoriteColor.eq(color));
        }
        if let Some(kw) = keyword
            && !kw.is_empty()
        {
            let kw_str: &str = kw;
            query = query.filter(
                file_index::Column::Name
                    .contains(kw_str)
                    .or(file_index::Column::Path.contains(kw_str)),
            );
        }
        let count = query.count(&*self.db).await?;
        Ok(count as i64)
    }
    /// List files in trash (top-level only)
    pub async fn list_trash(&self, user_id: &str) -> Result<Vec<file_index::Model>, DbErr> {
        file_index::Entity::find()
            .filter(file_index::Column::UserId.eq(user_id))
            .filter(file_index::Column::FileTrashedAt.is_not_null())
            .filter(file_index::Column::ParentPath.eq("/.recycle_bin"))
            .filter(file_index::Column::RowDeletedAt.is_null())
            .all(&*self.db)
            .await
    }
    /// Count trash files
    pub async fn count_trash_files(&self, user_id: &str) -> Result<i64, DbErr> {
        let count = file_index::Entity::find()
            .filter(file_index::Column::UserId.eq(user_id))
            .filter(file_index::Column::FileTrashedAt.is_not_null())
            .filter(file_index::Column::ParentPath.eq("/.recycle_bin"))
            .filter(file_index::Column::RowDeletedAt.is_null())
            .count(&*self.db)
            .await?;
        Ok(count as i64)
    }
    /// List files in trash (paginated)
    pub async fn list_trash_paginated(
        &self,
        user_id: &str,
        page: i64,
        page_size: i64,
    ) -> Result<(Vec<file_index::Model>, i64), DbErr> {
        let offset = (page - 1) * page_size;
        let files = file_index::Entity::find()
            .filter(file_index::Column::UserId.eq(user_id))
            .filter(file_index::Column::FileTrashedAt.is_not_null())
            .filter(file_index::Column::ParentPath.eq("/.recycle_bin"))
            .filter(file_index::Column::RowDeletedAt.is_null())
            .order_by_desc(file_index::Column::IsDir) // Folders first
            .order_by_desc(file_index::Column::FileTrashedAt)
            .limit(page_size as u64)
            .offset(offset as u64)
            .all(&*self.db)
            .await?;
        let total = self.count_trash_files(user_id).await?;
        Ok((files, total))
    }
    /// List files in trash (paginated, with sorting and search)
    #[allow(clippy::manual_unwrap_or)]
    pub async fn list_trash_paginated_with_sort(
        &self,
        user_id: &str,
        params: VfsPaginationParams<'_>,
    ) -> Result<(Vec<file_index::Model>, i64), DbErr> {
        let offset = (params.page - 1) * params.page_size;
        let mut query = file_index::Entity::find()
            .filter(file_index::Column::UserId.eq(user_id))
            .filter(file_index::Column::FileTrashedAt.is_not_null())
            .filter(file_index::Column::ParentPath.eq("/.recycle_bin"))
            .filter(file_index::Column::RowDeletedAt.is_null());
        // Search filter
        if let Some(kw) = params.keyword
            && !kw.is_empty()
        {
            let kw_str: &str = kw;
            query = query.filter(
                file_index::Column::Name
                    .contains(kw_str)
                    .or(file_index::Column::Path.contains(kw_str))
                    .or(file_index::Column::OriginalPath.contains(kw_str)),
            );
        }
        // Sorting
        let sort_col = match params.sort_by {
            Some("name") => file_index::Column::Name,
            Some("path") => file_index::Column::Path,
            Some("original_path") => file_index::Column::OriginalPath,
            Some("size") => file_index::Column::Size,
            Some("modified") => file_index::Column::FileUpdatedAt,
            _ => file_index::Column::FileTrashedAt,
        };
        let order = match params.order {
            Some(order) => order,
            None => "desc",
        };
        let is_desc = order == "desc";
        query = query.order_by_desc(file_index::Column::IsDir);
        query = if is_desc {
            query.order_by_desc(sort_col)
        } else {
            query.order_by_asc(sort_col)
        };
        let files = query
            .limit(params.page_size as u64)
            .offset(offset as u64)
            .all(&*self.db)
            .await?;
        let total = self
            .count_trash_files_with_filter(user_id, params.keyword)
            .await?;
        Ok((files, total))
    }
    /// Count trash files (with search filter)
    async fn count_trash_files_with_filter(
        &self,
        user_id: &str,
        keyword: Option<&str>,
    ) -> Result<i64, DbErr> {
        let mut query = file_index::Entity::find()
            .filter(file_index::Column::UserId.eq(user_id))
            .filter(file_index::Column::FileTrashedAt.is_not_null())
            .filter(file_index::Column::ParentPath.eq("/.recycle_bin"))
            .filter(file_index::Column::RowDeletedAt.is_null());
        if let Some(kw) = keyword
            && !kw.is_empty()
        {
            let kw_str: &str = kw;
            query = query.filter(
                file_index::Column::Name
                    .contains(kw_str)
                    .or(file_index::Column::Path.contains(kw_str))
                    .or(file_index::Column::OriginalPath.contains(kw_str)),
            );
        }
        let count = query.count(&*self.db).await?;
        Ok(count as i64)
    }
    /// Batch get active share status for files
    pub async fn get_active_shares_for_files(
        &self,
        file_ids: &[String],
    ) -> Result<std::collections::HashMap<String, (bool, bool)>, DbErr> {
        use crate::business::entities::file_share;
        if file_ids.is_empty() {
            return Ok(std::collections::HashMap::new());
        }
        let now = chrono::Utc::now();
        // Query all relevant shares
        let shares = file_share::Entity::find()
            .filter(file_share::Column::FileIndexId.is_in(file_ids))
            .all(&*self.db)
            .await?;
        let mut res = std::collections::HashMap::new();
        for share in shares {
            // Verify if share is active
            let is_expired = match share.expire_at {
                Some(expire_at) => expire_at < now,
                None => false,
            };
            let is_limit_reached = match share.max_downloads {
                Some(max_downloads) => max_downloads > 0 && share.view_count >= max_downloads,
                None => false,
            };
            if !is_expired && !is_limit_reached {
                let share_enable_direct = share.enable_direct;
                let share_file_index_id = share.file_index_id;
                let entry = res.entry(share_file_index_id).or_insert((false, false));
                entry.0 = true;
                if share_enable_direct {
                    entry.1 = true;
                }
            }
        }
        Ok(res)
    }
    /// Sync trash share status
    /// Soft-delete related shares when file is trashed
    pub async fn sync_trash_share_status(
        &self,
        user_id: &str,
        file_index_id: &str,
    ) -> Result<(), DbErr> {
        use crate::business::entities::file_share;
        file_share::Entity::update_many()
            .col_expr(file_share::Column::IsDeleted, Expr::value(true))
            .filter(file_share::Column::UserId.eq(user_id))
            .filter(file_share::Column::FileIndexId.eq(file_index_id))
            .exec(&*self.db)
            .await?;
        Ok(())
    }
    /// Update physical storage ID (tiered migration)
    pub async fn update_storage_id(
        &self,
        user_id: &str,
        path: &str,
        target_storage_id: &str,
    ) -> Result<(), DbErr> {
        file_index::Entity::update_many()
            .col_expr(
                file_index::Column::StorageId,
                Expr::value(target_storage_id),
            )
            .filter(file_index::Column::UserId.eq(user_id))
            .filter(file_index::Column::Path.eq(path))
            .filter(file_index::Column::RowDeletedAt.is_null())
            .exec(&*self.db)
            .await?;
        Ok(())
    }
    /// Batch sync directory items (backfill index)
    pub async fn sync_directory_items(
        &self,
        _user_id: &str,
        _parent_path: &str,
        items: Vec<file_index::ActiveModel>,
    ) -> Result<(), DbErr> {
        if items.is_empty() {
            return Ok(());
        }
        file_index::Entity::insert_many(items)
            .on_conflict(
                sea_orm::sea_query::OnConflict::columns(vec![
                    file_index::Column::UserId,
                    file_index::Column::Path,
                ])
                .update_columns(vec![
                    file_index::Column::ParentPath,
                    file_index::Column::Name,
                    file_index::Column::IsDir,
                    file_index::Column::Size,
                    file_index::Column::FileUpdatedAt,
                    file_index::Column::RowUpdatedAt,
                    //favorite_color is intentionally excluded
                ])
                .to_owned(),
            )
            .exec(&*self.db)
            .await?;
        Ok(())
    }
    /// Find expired trash files
    pub async fn find_expired_trash(
        &self,
        retention_days: u32,
    ) -> Result<Vec<file_index::Model>, DbErr> {
        if retention_days == 0 {
            return Ok(vec![]);
        }
        let expired_at = chrono::Utc::now() - chrono::Duration::days(retention_days as i64);
        file_index::Entity::find()
            .filter(file_index::Column::FileTrashedAt.lt(expired_at))
            .filter(file_index::Column::ParentPath.eq("/.recycle_bin"))
            .filter(file_index::Column::RowDeletedAt.is_null())
            .all(&*self.db)
            .await
    }
    /// Complete directory sync (Batch Upsert + Physical Pruning)
    /// Uses timestamp-based pruning to avoid performance degradation from large NOT IN clauses
    pub async fn sync_directory_optimized(
        &self,
        user_id: &str,
        parent_path: &str,
        items: Vec<file_index::ActiveModel>,
    ) -> Result<(), DbErr> {
        let parent_path = if parent_path == "/" {
            "/"
        } else {
            parent_path.trim_end_matches('/')
        };
        if items.is_empty() {
            // If physical side is empty, clear all index records in this directory
            file_index::Entity::delete_many()
                .filter(file_index::Column::UserId.eq(user_id))
                .filter(file_index::Column::ParentPath.eq(parent_path))
                .exec(&*self.db)
                .await?;
            return Ok(());
        }
        // Get timestamp before synchronization starts
        let sync_start = chrono::Utc::now() - chrono::Duration::seconds(1);
        let txn = self.db.begin().await?;
        // Execute Batch Upsert in chunks
        for chunk in items.chunks(500) {
            file_index::Entity::insert_many(chunk.to_vec())
                .on_conflict(
                    sea_orm::sea_query::OnConflict::columns(vec![
                        file_index::Column::UserId,
                        file_index::Column::Path,
                    ])
                    .update_columns(vec![
                        file_index::Column::ParentPath,
                        file_index::Column::Name,
                        file_index::Column::IsDir,
                        file_index::Column::Size,
                        file_index::Column::FileUpdatedAt,
                        file_index::Column::RowUpdatedAt,
                    ])
                    .value(
                        file_index::Column::RowDeletedAt,
                        Expr::value(Option::<chrono::DateTime<chrono::FixedOffset>>::None),
                    )
                    .to_owned(),
                )
                .exec(&txn)
                .await?;
        }
        // Prune stale rows (row_updated_at < sync_start).
        file_index::Entity::delete_many()
            .filter(file_index::Column::UserId.eq(user_id))
            .filter(file_index::Column::ParentPath.eq(parent_path))
            .filter(file_index::Column::RowUpdatedAt.lt(sync_start))
            .exec(&txn)
            .await?;
        txn.commit().await?;
        Ok(())
    }
    /// Complete directory sync (Upsert + Pruning)
    /// Mark items that exist in database but not in physical storage as row_deleted_at
    #[deprecated(note = "Use sync_directory_optimized for better performance and scalability")]
    pub async fn sync_directory_complete(
        &self,
        user_id: &str,
        parent_path: &str,
        physical_items: Vec<file_index::ActiveModel>,
    ) -> Result<(), DbErr> {
        self.sync_directory_optimized(user_id, parent_path, physical_items)
            .await
    }
}
