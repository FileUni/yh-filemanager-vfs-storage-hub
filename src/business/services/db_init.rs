use crate::business::entities::{file_index, file_share, ssh_keys, user_settings};
use sea_orm::sea_query::Index;
use sea_orm::{ConnectionTrait, DbErr, Schema};
use std::sync::Arc;
use yh_console_log::yhlog;
/// VFS
pub async fn init_vfs_tables(db: &Arc<sea_orm::DatabaseConnection>) -> Result<(), DbErr> {
    yhlog("info", "Starting VFS Storage Hub DB initialization...");
    let backend = db.get_database_backend();
    let schema = Schema::new(backend);

    // Tables.
    let stmt_user_settings = schema
        .create_table_from_entity(user_settings::Entity)
        .if_not_exists()
        .to_owned();
    db.execute(backend.build(&stmt_user_settings)).await?;
    let stmt_ssh_keys = schema
        .create_table_from_entity(ssh_keys::Entity)
        .if_not_exists()
        .to_owned();
    db.execute(backend.build(&stmt_ssh_keys)).await?;

    let stmt_file_index = schema
        .create_table_from_entity(file_index::Entity)
        .if_not_exists()
        .to_owned();
    db.execute(backend.build(&stmt_file_index)).await?;

    // Indexes.
    let idx_vfs_list = Index::create()
        .name("idx_vfs_list")
        .table(file_index::Entity)
        .col(file_index::Column::UserId)
        .col(file_index::Column::ParentPath)
        .col(file_index::Column::RowDeletedAt)
        .if_not_exists()
        .to_owned();
    let idx_vfs_search = Index::create()
        .name("idx_vfs_search")
        .table(file_index::Entity)
        .col(file_index::Column::UserId)
        .col(file_index::Column::Name)
        .if_not_exists()
        .to_owned();
    let idx_vfs_recycle = Index::create()
        .name("idx_vfs_recycle")
        .table(file_index::Entity)
        .col(file_index::Column::UserId)
        .col(file_index::Column::FileTrashedAt)
        .if_not_exists()
        .to_owned();
    let idx_vfs_list_live_sort = Index::create()
        .name("idx_vfs_list_live_sort")
        .table(file_index::Entity)
        .col(file_index::Column::UserId)
        .col(file_index::Column::ParentPath)
        .col(file_index::Column::RowDeletedAt)
        .col(file_index::Column::FileTrashedAt)
        .col(file_index::Column::IsDir)
        .col(file_index::Column::Name)
        .if_not_exists()
        .to_owned();
    let idx_vfs_favorites_live_sort = Index::create()
        .name("idx_vfs_favorites_live_sort")
        .table(file_index::Entity)
        .col(file_index::Column::UserId)
        .col(file_index::Column::FavoriteColor)
        .col(file_index::Column::RowDeletedAt)
        .col(file_index::Column::FileTrashedAt)
        .col(file_index::Column::IsDir)
        .col(file_index::Column::Name)
        .if_not_exists()
        .to_owned();
    let idx_vfs_trash_live_sort = Index::create()
        .name("idx_vfs_trash_live_sort")
        .table(file_index::Entity)
        .col(file_index::Column::UserId)
        .col(file_index::Column::ParentPath)
        .col(file_index::Column::RowDeletedAt)
        .col(file_index::Column::FileTrashedAt)
        .col(file_index::Column::IsDir)
        .col(file_index::Column::Name)
        .if_not_exists()
        .to_owned();
    let idx_vfs_path_prefix = Index::create()
        .name("idx_vfs_path_prefix")
        .table(file_index::Entity)
        .col(file_index::Column::UserId)
        .col(file_index::Column::Path)
        .if_not_exists()
        .to_owned();

    // Unique index for upsert target (user_id, path).
    // Many write-through code paths rely on `ON CONFLICT(user_id, path)`.
    // Without this constraint SQLite will reject the statement and the index will stay empty.
    let uidx_vfs_user_path = Index::create()
        .name("uidx_vfs_user_path")
        .table(file_index::Entity)
        .col(file_index::Column::UserId)
        .col(file_index::Column::Path)
        .unique()
        .if_not_exists()
        .to_owned();

    db.execute(backend.build(&idx_vfs_list)).await?;
    db.execute(backend.build(&idx_vfs_search)).await?;
    db.execute(backend.build(&idx_vfs_recycle)).await?;
    db.execute(backend.build(&idx_vfs_list_live_sort)).await?;
    db.execute(backend.build(&idx_vfs_favorites_live_sort))
        .await?;
    db.execute(backend.build(&idx_vfs_trash_live_sort)).await?;
    db.execute(backend.build(&idx_vfs_path_prefix)).await?;
    db.execute(backend.build(&uidx_vfs_user_path)).await?;

    let stmt_file_shares = schema
        .create_table_from_entity(file_share::Entity)
        .if_not_exists()
        .to_owned();
    db.execute(backend.build(&stmt_file_shares)).await?;

    // WAL.
    let wal_manager = crate::vfs::wal::VfsWalManager::new(Arc::clone(db));
    wal_manager.init_tables().await?;
    yhlog("info", "VFS Storage Hub DB initialization completed.");
    Ok(())
}
