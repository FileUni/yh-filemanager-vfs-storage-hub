use super::FileIndexService;
use crate::business::entities::file_index;
use crate::business::services::nextcloud::{
    derive_nextcloud_file_numeric_id, nextcloud_is_image, nextcloud_is_media, nextcloud_is_video,
};
use crate::vfs::{VfsError, VfsStorage};
use sea_orm::{DatabaseConnection, DbErr};
use std::cmp::Ordering;
use std::sync::Arc;
use thiserror::Error;

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
