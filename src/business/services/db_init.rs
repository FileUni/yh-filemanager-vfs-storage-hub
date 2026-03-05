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
    // =========================================================================================
    //1. SSH
    // =========================================================================================
    // yh_vfs_user_settings
    let stmt_user_settings = schema
        .create_table_from_entity(user_settings::Entity)
        .if_not_exists()
        .to_owned();
    db.execute(backend.build(&stmt_user_settings)).await?;
    // yh_vfs_ssh_keys
    let stmt_ssh_keys = schema
        .create_table_from_entity(ssh_keys::Entity)
        .if_not_exists()
        .to_owned();
    db.execute(backend.build(&stmt_ssh_keys)).await?;
    // =========================================================================================
    //2. (yh_vfs_file_index)
    // =========================================================================================
    let stmt_file_index = schema
        .create_table_from_entity(file_index::Entity)
        .if_not_exists()
        .to_owned();
    db.execute(backend.build(&stmt_file_index)).await?;

    //2.2 Create common indexes
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
    let idx_vfs_path_prefix = Index::create()
        .name("idx_vfs_path_prefix")
        .table(file_index::Entity)
        .col(file_index::Column::UserId)
        .col(file_index::Column::Path)
        .if_not_exists()
        .to_owned();

    db.execute(backend.build(&idx_vfs_list)).await?;
    db.execute(backend.build(&idx_vfs_search)).await?;
    db.execute(backend.build(&idx_vfs_recycle)).await?;
    db.execute(backend.build(&idx_vfs_path_prefix)).await?;
    // =========================================================================================
    //3. (yh_vfs_file_shares)
    // =========================================================================================
    let stmt_file_shares = schema
        .create_table_from_entity(file_share::Entity)
        .if_not_exists()
        .to_owned();
    db.execute(backend.build(&stmt_file_shares)).await?;
    // =========================================================================================
    //4. (yh_vfs_wal)
    // =========================================================================================
    let wal_manager = crate::vfs::wal::VfsWalManager::new(Arc::clone(db));
    wal_manager.init_tables().await?;
    yhlog("info", "VFS Storage Hub DB initialization completed.");
    Ok(())
}
