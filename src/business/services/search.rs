use super::FileIndexService;
use crate::business::entities::file_index;
use crate::business::services::nextcloud::{
    derive_nextcloud_file_numeric_id, nextcloud_is_image, nextcloud_is_media, nextcloud_is_video,
};
use crate::vfs::{VfsError, VfsStorage};
use sea_orm::sea_query::{Expr, Func};
use sea_orm::{
    ColumnTrait, Condition, DatabaseConnection, DbErr, EntityTrait, PaginatorTrait, QueryFilter,
    QueryOrder, QuerySelect,
};
use std::cmp::Ordering;
use std::sync::Arc;
use thiserror::Error;

const IMAGE_EXTENSIONS: &[&str] = &[
    "jpg", "jpeg", "png", "gif", "webp", "bmp", "svg", "psd", "ai", "heic", "heif", "avif",
];
const VIDEO_EXTENSIONS: &[&str] = &[
    "mp4", "mov", "mkv", "avi", "webm", "m4v", "3gp", "mpg", "mpeg",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexedSearchMediaKind {
    Image,
    Video,
    Media,
}

#[derive(Debug, Clone, Copy)]
pub struct IndexedSearchQuery<'a> {
    pub root_path: &'a str,
    pub keyword: Option<&'a str>,
    pub include_path_keyword: bool,
    pub include_original_path_keyword: bool,
    pub favorites_only: bool,
    pub trashed_only: bool,
    pub favorite_color: Option<i32>,
    pub nextcloud_file_numeric_id: Option<u64>,
    pub media_kind: Option<IndexedSearchMediaKind>,
    pub modified_between: Option<(i64, i64)>,
    pub sort_by: Option<&'a str>,
    pub order: Option<&'a str>,
    pub page: i64,
    pub page_size: i64,
    pub heal_if_empty: bool,
}

#[derive(Debug, Error)]
pub enum IndexedSearchError {
    #[error("Database error: {0}")]
    Database(#[from] DbErr),
    #[error("Storage error: {0}")]
    Storage(#[from] VfsError),
}

pub struct IndexedSearchService {
    db: Arc<DatabaseConnection>,
}

impl IndexedSearchService {
    pub fn new(db: Arc<DatabaseConnection>) -> Self {
        Self { db }
    }

    pub async fn search_with_storage(
        &self,
        storage: &dyn VfsStorage,
        user_id: &str,
        query: IndexedSearchQuery<'_>,
    ) -> Result<(Vec<file_index::Model>, i64), IndexedSearchError> {
        let index_service = FileIndexService::new(Arc::clone(&self.db));
        if query.nextcloud_file_numeric_id.is_none() {
            let (items, total) = self.load_index_page_matches(user_id, query).await?;
            if total > 0 || !query.heal_if_empty || query.favorites_only || query.trashed_only {
                return Ok((items, total));
            }
        }
        let mut items = self
            .load_index_matches(&index_service, user_id, query)
            .await?;
        if items.is_empty() && query.heal_if_empty && !query.favorites_only && !query.trashed_only {
            items = self
                .heal_matches_from_storage(&index_service, storage, user_id, query)
                .await?;
        }
        sort_items(&mut items, query.sort_by, query.order);
        let total = items.len() as i64;
        let page_size = query.page_size.max(1) as usize;
        let offset = ((query.page.max(1) - 1) * query.page_size.max(1)) as usize;
        let page_items = items.into_iter().skip(offset).take(page_size).collect();
        Ok((page_items, total))
    }

    async fn load_index_page_matches(
        &self,
        user_id: &str,
        query: IndexedSearchQuery<'_>,
    ) -> Result<(Vec<file_index::Model>, i64), DbErr> {
        let page = query.page.max(1);
        let page_size = query.page_size.max(1);
        let offset = page.saturating_sub(1).saturating_mul(page_size) as u64;
        let limit = page_size as u64;
        let condition = build_index_search_condition(user_id, &query);
        let total = file_index::Entity::find()
            .filter(condition.clone())
            .count(&*self.db)
            .await? as i64;
        if total == 0 {
            return Ok((Vec::new(), 0));
        }
        let items = apply_index_search_sort(file_index::Entity::find().filter(condition), &query)
            .limit(limit)
            .offset(offset)
            .all(&*self.db)
            .await?;
        Ok((items, total))
    }

    async fn load_index_matches(
        &self,
        index_service: &FileIndexService,
        user_id: &str,
        query: IndexedSearchQuery<'_>,
    ) -> Result<Vec<file_index::Model>, DbErr> {
        let items = if query.trashed_only {
            index_service.list_trash(user_id).await?
        } else if query.favorites_only {
            index_service
                .list_favorites(user_id, query.favorite_color)
                .await?
        } else if query.keyword.is_some()
            && !query.include_path_keyword
            && !query.include_original_path_keyword
            && query.root_path == "/"
            && query.nextcloud_file_numeric_id.is_none()
            && query.media_kind.is_none()
            && query.modified_between.is_none()
        {
            index_service
                .search_files(user_id, query.keyword.unwrap_or_default())
                .await?
        } else {
            index_service
                .list_files_recursive(user_id, query.root_path)
                .await?
        };
        Ok(items
            .into_iter()
            .filter(|item| indexed_item_matches(user_id, item, &query))
            .collect())
    }

    async fn heal_matches_from_storage(
        &self,
        index_service: &FileIndexService,
        storage: &dyn VfsStorage,
        user_id: &str,
        query: IndexedSearchQuery<'_>,
    ) -> Result<Vec<file_index::Model>, IndexedSearchError> {
        let entries = storage.list_recursive(query.root_path).await?;
        let mut out = Vec::new();
        for entry in entries
            .into_iter()
            .filter(|entry| vfs_entry_matches(user_id, entry, &query))
        {
            let indexed = index_service
                .upsert_file(user_id, entry.path.as_ref(), &entry)
                .await?;
            out.push(indexed);
        }
        Ok(out)
    }
}

fn build_index_search_condition(user_id: &str, query: &IndexedSearchQuery<'_>) -> Condition {
    let mut condition = Condition::all()
        .add(file_index::Column::UserId.eq(user_id))
        .add(file_index::Column::RowDeletedAt.is_null());

    if query.trashed_only {
        condition = condition
            .add(file_index::Column::FileTrashedAt.is_not_null())
            .add(file_index::Column::ParentPath.eq("/.recycle_bin"));
    } else {
        condition = condition.add(file_index::Column::FileTrashedAt.is_null());
    }

    if query.favorites_only {
        condition = condition.add(file_index::Column::FavoriteColor.gt(0));
        if let Some(color) = query.favorite_color
            && color > 0
        {
            condition = condition.add(file_index::Column::FavoriteColor.eq(color));
        }
    }

    if query.root_path != "/" {
        let prefix = format!("{}/", query.root_path.trim_end_matches('/'));
        condition = condition.add(
            Condition::any()
                .add(file_index::Column::Path.eq(query.root_path))
                .add(file_index::Column::Path.starts_with(prefix)),
        );
    }

    if let Some(keyword) = query.keyword {
        condition = condition.add(keyword_condition(
            keyword,
            query.include_path_keyword,
            query.include_original_path_keyword,
        ));
    }

    if let Some(kind) = query.media_kind {
        condition = condition.add(media_kind_condition(kind));
    }

    if let Some((start, end)) = query.modified_between
        && let (Some(start_dt), Some(end_dt)) = (
            chrono::DateTime::<chrono::Utc>::from_timestamp(start, 0),
            chrono::DateTime::<chrono::Utc>::from_timestamp(end, 0),
        )
    {
        condition = condition.add(
            Condition::any()
                .add(
                    Condition::all()
                        .add(file_index::Column::FileUpdatedAt.is_not_null())
                        .add(file_index::Column::FileUpdatedAt.gt(start_dt))
                        .add(file_index::Column::FileUpdatedAt.lt(end_dt)),
                )
                .add(
                    Condition::all()
                        .add(file_index::Column::FileUpdatedAt.is_null())
                        .add(file_index::Column::RowUpdatedAt.gt(start_dt))
                        .add(file_index::Column::RowUpdatedAt.lt(end_dt)),
                ),
        );
    }

    condition
}

fn keyword_condition(
    keyword: &str,
    include_path_keyword: bool,
    include_original_path_keyword: bool,
) -> Condition {
    let pattern = format!("%{}%", keyword.to_ascii_lowercase());
    let mut condition = Condition::any().add(lower_like(file_index::Column::Name, &pattern));
    if include_path_keyword {
        condition = condition.add(lower_like(file_index::Column::Path, &pattern));
    }
    if include_original_path_keyword {
        condition = condition.add(lower_like(file_index::Column::OriginalPath, &pattern));
    }
    condition
}

fn media_kind_condition(kind: IndexedSearchMediaKind) -> Condition {
    let mut condition = Condition::any();
    let image_iter = IMAGE_EXTENSIONS.iter().copied();
    let video_iter = VIDEO_EXTENSIONS.iter().copied();
    let extensions: Box<dyn Iterator<Item = &str>> = match kind {
        IndexedSearchMediaKind::Image => Box::new(image_iter),
        IndexedSearchMediaKind::Video => Box::new(video_iter),
        IndexedSearchMediaKind::Media => Box::new(image_iter.chain(video_iter)),
    };
    for extension in extensions {
        condition = condition.add(lower_like(
            file_index::Column::Path,
            format!("%.{}", extension).as_str(),
        ));
    }
    condition
}

fn lower_like(column: file_index::Column, pattern: &str) -> sea_orm::sea_query::SimpleExpr {
    Expr::expr(Func::lower(Expr::col(column))).like(pattern)
}

fn apply_index_search_sort(
    mut query_builder: sea_orm::Select<file_index::Entity>,
    query: &IndexedSearchQuery<'_>,
) -> sea_orm::Select<file_index::Entity> {
    let is_desc = matches!(query.order, Some("desc"));
    query_builder = query_builder.order_by_desc(file_index::Column::IsDir);
    let query_builder = match query.sort_by.unwrap_or("name") {
        "size" => apply_order(query_builder, file_index::Column::Size, is_desc),
        "modified" => {
            let query_builder =
                apply_order(query_builder, file_index::Column::FileUpdatedAt, is_desc);
            apply_order(query_builder, file_index::Column::RowUpdatedAt, is_desc)
        }
        "trashed_at" => apply_order(query_builder, file_index::Column::FileTrashedAt, is_desc),
        "original_path" => apply_order(query_builder, file_index::Column::OriginalPath, is_desc),
        "path" => apply_order(query_builder, file_index::Column::Path, is_desc),
        _ => apply_order(query_builder, file_index::Column::Name, is_desc),
    };
    if matches!(query.sort_by, Some("path")) {
        query_builder
    } else {
        apply_order(query_builder, file_index::Column::Path, is_desc)
    }
}

fn apply_order(
    query_builder: sea_orm::Select<file_index::Entity>,
    column: file_index::Column,
    is_desc: bool,
) -> sea_orm::Select<file_index::Entity> {
    if is_desc {
        query_builder.order_by_desc(column)
    } else {
        query_builder.order_by_asc(column)
    }
}

fn indexed_item_matches(
    user_id: &str,
    item: &file_index::Model,
    query: &IndexedSearchQuery<'_>,
) -> bool {
    if !path_matches_root(item.path.as_str(), query.root_path) {
        return false;
    }
    if query.trashed_only && item.file_trashed_at.is_none() {
        return false;
    }
    if query.favorites_only {
        if item.favorite_color <= 0 {
            return false;
        }
        if let Some(color) = query.favorite_color
            && color > 0
            && item.favorite_color != color
        {
            return false;
        }
    }
    if let Some(keyword) = query.keyword
        && !matches_keyword(
            item.name.as_str(),
            item.path.as_str(),
            item.original_path.as_deref(),
            keyword,
            query.include_path_keyword,
            query.include_original_path_keyword,
        )
    {
        return false;
    }
    if let Some(target_id) = query.nextcloud_file_numeric_id
        && derive_nextcloud_file_numeric_id(user_id, item.path.as_str()) != target_id
    {
        return false;
    }
    if let Some(kind) = query.media_kind
        && !matches_media_kind(item.path.as_str(), kind)
    {
        return false;
    }
    if let Some((start, end)) = query.modified_between {
        let ts = item
            .file_updated_at
            .as_ref()
            .map(|value| value.timestamp())
            .unwrap_or_else(|| item.row_updated_at.timestamp());
        if ts <= start || ts >= end {
            return false;
        }
    }
    true
}

fn vfs_entry_matches(
    user_id: &str,
    entry: &crate::vfs::VfsFileInfo,
    query: &IndexedSearchQuery<'_>,
) -> bool {
    if !path_matches_root(entry.path.as_ref(), query.root_path) {
        return false;
    }
    if query.favorites_only || query.trashed_only {
        return false;
    }
    if let Some(keyword) = query.keyword
        && !matches_keyword(
            entry.name.as_ref(),
            entry.path.as_ref(),
            entry.original_path.as_deref(),
            keyword,
            query.include_path_keyword,
            query.include_original_path_keyword,
        )
    {
        return false;
    }
    if let Some(target_id) = query.nextcloud_file_numeric_id
        && derive_nextcloud_file_numeric_id(user_id, entry.path.as_ref()) != target_id
    {
        return false;
    }
    if let Some(kind) = query.media_kind
        && !matches_media_kind(entry.path.as_ref(), kind)
    {
        return false;
    }
    if let Some((start, end)) = query.modified_between {
        let Some(ts) = entry.modified.as_ref().map(|value| value.timestamp()) else {
            return false;
        };
        if ts <= start || ts >= end {
            return false;
        }
    }
    true
}

fn path_matches_root(path: &str, root_path: &str) -> bool {
    if root_path == "/" {
        return true;
    }
    path == root_path
        || path
            .strip_prefix(root_path)
            .is_some_and(|rest| rest.starts_with('/'))
}

fn matches_keyword(
    name: &str,
    path: &str,
    original_path: Option<&str>,
    keyword: &str,
    include_path_keyword: bool,
    include_original_path_keyword: bool,
) -> bool {
    let needle = keyword.to_lowercase();
    name.to_lowercase().contains(&needle)
        || (include_path_keyword && path.to_lowercase().contains(&needle))
        || (include_original_path_keyword
            && original_path.is_some_and(|value| value.to_lowercase().contains(&needle)))
}

fn matches_media_kind(path: &str, kind: IndexedSearchMediaKind) -> bool {
    match kind {
        IndexedSearchMediaKind::Image => nextcloud_is_image(path),
        IndexedSearchMediaKind::Video => nextcloud_is_video(path),
        IndexedSearchMediaKind::Media => nextcloud_is_media(path),
    }
}

fn sort_items(items: &mut [file_index::Model], sort_by: Option<&str>, order: Option<&str>) {
    let is_desc = matches!(order, Some("desc"));
    let sort_field = sort_by.unwrap_or("name");
    items.sort_by(|a, b| {
        let dir_cmp = b.is_dir.cmp(&a.is_dir);
        if dir_cmp != Ordering::Equal {
            return dir_cmp;
        }
        let cmp = match sort_field {
            "size" => a.size.cmp(&b.size),
            "modified" => a
                .file_updated_at
                .unwrap_or(a.row_updated_at)
                .cmp(&b.file_updated_at.unwrap_or(b.row_updated_at)),
            "trashed_at" => a.file_trashed_at.cmp(&b.file_trashed_at),
            "original_path" => a.original_path.cmp(&b.original_path),
            "path" => a.path.cmp(&b.path),
            _ => a.name.cmp(&b.name),
        };
        let cmp = if cmp == Ordering::Equal && sort_field != "path" {
            a.path.cmp(&b.path)
        } else {
            cmp
        };
        if is_desc { cmp.reverse() } else { cmp }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_matches_root_accepts_recursive_children() {
        assert!(path_matches_root("/Docs/demo.md", "/Docs"));
        assert!(path_matches_root("/Docs", "/Docs"));
        assert!(!path_matches_root("/Media/demo.md", "/Docs"));
    }

    #[test]
    fn matches_keyword_honors_path_flag() {
        assert!(matches_keyword(
            "demo.md",
            "/Notes/demo.md",
            None,
            "demo",
            false,
            false
        ));
        assert!(!matches_keyword(
            "demo.md",
            "/Notes/demo.md",
            None,
            "notes",
            false,
            false
        ));
        assert!(matches_keyword(
            "demo.md",
            "/Notes/demo.md",
            None,
            "notes",
            true,
            false
        ));
    }

    #[test]
    fn matches_keyword_can_check_original_path() {
        assert!(matches_keyword(
            "trash-demo.md",
            "/.recycle_bin/123_demo.md",
            Some("/Docs/demo.md"),
            "docs",
            false,
            true,
        ));
    }
}
