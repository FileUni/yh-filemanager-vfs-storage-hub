use crate::business::entities::{file_index, file_share, remote_mount, ssh_keys, user_settings};
use sea_orm::sea_query::{Alias, ColumnDef, Index, Table};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DbErr, EntityTrait, QueryFilter, Schema,
};
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

    let stmt_remote_mount = schema
        .create_table_from_entity(remote_mount::Entity)
        .if_not_exists()
        .to_owned();
    db.execute(backend.build(&stmt_remote_mount)).await?;

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
    let idx_vfs_remote_mount_user = Index::create()
        .name("idx_vfs_remote_mount_user")
        .table(remote_mount::Entity)
        .col(remote_mount::Column::UserId)
        .if_not_exists()
        .to_owned();
    let idx_vfs_remote_mount_due = Index::create()
        .name("idx_vfs_remote_mount_due")
        .table(remote_mount::Entity)
        .col(remote_mount::Column::Enable)
        .col(remote_mount::Column::NextSyncAt)
        .if_not_exists()
        .to_owned();
    let uidx_vfs_remote_mount_user_dir = Index::create()
        .name("uidx_vfs_remote_mount_user_dir")
        .table(remote_mount::Entity)
        .col(remote_mount::Column::UserId)
        .col(remote_mount::Column::MountDir)
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
    db.execute(backend.build(&idx_vfs_remote_mount_user))
        .await?;
    db.execute(backend.build(&idx_vfs_remote_mount_due)).await?;
    db.execute(backend.build(&uidx_vfs_remote_mount_user_dir))
        .await?;

    let stmt_file_shares = schema
        .create_table_from_entity(file_share::Entity)
        .if_not_exists()
        .to_owned();
    db.execute(backend.build(&stmt_file_shares)).await?;
    ensure_file_index_columns(db).await?;
    ensure_user_settings_columns(db).await?;
    ensure_file_share_columns(db).await?;

    // WAL.
    let wal_manager = crate::vfs::wal::VfsWalManager::new(Arc::clone(db));
    wal_manager.init_tables().await?;
    yhlog("info", "VFS Storage Hub DB initialization completed.");
    Ok(())
}

async fn ensure_user_settings_columns(db: &Arc<sea_orm::DatabaseConnection>) -> Result<(), DbErr> {
    add_user_settings_column_if_missing(db, "thumbnail_directory_mode", AddColumnKind::TextNullable)
        .await?;
    add_user_settings_column_if_missing(
        db,
        "show_thumbnail_directories",
        AddColumnKind::BoolNotNullDefaultFalse,
    )
    .await?;
    add_user_settings_column_if_missing(db, "protected_root", AddColumnKind::TextNullable).await?;
    add_user_settings_column_if_missing(db, "protected_mode", AddColumnKind::TextNullable).await?;
    add_user_settings_column_if_missing(db, "protected_key_slot_id", AddColumnKind::TextNullable)
        .await?;
    add_user_settings_column_if_missing(db, "protected_wrapped_key", AddColumnKind::TextNullable)
        .await?;
    add_user_settings_column_if_missing(
        db,
        "protected_enabled_at",
        AddColumnKind::TimestampNullable,
    )
    .await?;
    add_user_settings_column_if_missing(
        db,
        "protected_updated_at",
        AddColumnKind::TimestampNullable,
    )
    .await?;
    Ok(())
}

async fn ensure_file_index_columns(db: &Arc<sea_orm::DatabaseConnection>) -> Result<(), DbErr> {
    add_file_index_column_if_missing(db, "physical_size", AddColumnKind::BigIntNullable).await?;
    add_file_index_column_if_missing(db, "protected_meta", AddColumnKind::TextNullable).await?;
    Ok(())
}

async fn ensure_file_share_columns(db: &Arc<sea_orm::DatabaseConnection>) -> Result<(), DbErr> {
    add_file_share_column_if_missing(db, "note", AddColumnKind::TextNullable).await?;
    add_file_share_column_if_missing(db, "label", AddColumnKind::TextNullable).await?;
    add_file_share_column_if_missing(db, "attributes", AddColumnKind::TextNullable).await?;
    add_file_share_column_if_missing(db, "hide_download", AddColumnKind::BoolNotNullDefaultFalse)
        .await?;
    add_file_share_column_if_missing(db, "snapshot_path", AddColumnKind::TextNullable).await?;
    add_file_share_column_if_missing(db, "snapshot_name", AddColumnKind::TextNullable).await?;
    add_file_share_column_if_missing(db, "snapshot_is_dir", AddColumnKind::BoolNullable).await?;
    backfill_file_share_snapshots(db).await?;
    Ok(())
}

async fn backfill_file_share_snapshots(db: &Arc<sea_orm::DatabaseConnection>) -> Result<(), DbErr> {
    use crate::business::entities::{file_index, file_share};

    let rows = file_share::Entity::find()
        .find_also_related(file_index::Entity)
        .filter(file_share::Column::SnapshotPath.is_null())
        .all(db.as_ref())
        .await?;
    for (share, file) in rows {
        let Some(file) = file else {
            continue;
        };
        let mut active = file_share::ActiveModel::from(share);
        active.snapshot_path = sea_orm::ActiveValue::Set(Some(file.path));
        active.snapshot_name = sea_orm::ActiveValue::Set(Some(file.name));
        active.snapshot_is_dir = sea_orm::ActiveValue::Set(Some(file.is_dir));
        let _ = active.update(db.as_ref()).await?;
    }
    Ok(())
}

async fn add_file_share_column_if_missing(
    db: &Arc<sea_orm::DatabaseConnection>,
    column_name: &str,
    column_kind: AddColumnKind,
) -> Result<(), DbErr> {
    match execute_add_column_if_missing(db, "yh_vfs_file_shares", column_name, column_kind).await {
        Ok(_) => Ok(()),
        Err(err) => {
            let msg = err.to_string().to_ascii_lowercase();
            if msg.contains("duplicate column")
                || msg.contains("duplicate column name")
                || (msg.contains("already exists")
                    && msg.contains(&column_name.to_ascii_lowercase()))
            {
                Ok(())
            } else {
                Err(err)
            }
        }
    }
}

async fn add_user_settings_column_if_missing(
    db: &Arc<sea_orm::DatabaseConnection>,
    column_name: &str,
    column_kind: AddColumnKind,
) -> Result<(), DbErr> {
    match execute_add_column_if_missing(db, "yh_vfs_user_settings", column_name, column_kind).await
    {
        Ok(_) => Ok(()),
        Err(err) => {
            let msg = err.to_string().to_ascii_lowercase();
            if msg.contains("duplicate column")
                || msg.contains("duplicate column name")
                || (msg.contains("already exists")
                    && msg.contains(&column_name.to_ascii_lowercase()))
            {
                Ok(())
            } else {
                Err(err)
            }
        }
    }
}

async fn add_file_index_column_if_missing(
    db: &Arc<sea_orm::DatabaseConnection>,
    column_name: &str,
    column_kind: AddColumnKind,
) -> Result<(), DbErr> {
    match execute_add_column_if_missing(db, "yh_vfs_file_index", column_name, column_kind).await {
        Ok(_) => Ok(()),
        Err(err) => {
            let msg = err.to_string().to_ascii_lowercase();
            if msg.contains("duplicate column")
                || msg.contains("duplicate column name")
                || (msg.contains("already exists")
                    && msg.contains(&column_name.to_ascii_lowercase()))
            {
                Ok(())
            } else {
                Err(err)
            }
        }
    }
}

#[derive(Clone, Copy)]
enum AddColumnKind {
    TextNullable,
    BoolNullable,
    BoolNotNullDefaultFalse,
    BigIntNullable,
    TimestampNullable,
}

fn build_optional_column(column_name: &str, column_kind: AddColumnKind) -> ColumnDef {
    let mut column = ColumnDef::new(Alias::new(column_name));
    match column_kind {
        AddColumnKind::TextNullable => {
            column.text().null();
        }
        AddColumnKind::BoolNullable => {
            column.boolean().null();
        }
        AddColumnKind::BoolNotNullDefaultFalse => {
            column.boolean().not_null().default(false);
        }
        AddColumnKind::BigIntNullable => {
            column.big_integer().null();
        }
        AddColumnKind::TimestampNullable => {
            column.timestamp_with_time_zone().null();
        }
    }
    column
}

async fn execute_add_column_if_missing(
    db: &Arc<sea_orm::DatabaseConnection>,
    table_name: &str,
    column_name: &str,
    column_kind: AddColumnKind,
) -> Result<(), DbErr> {
    let backend = db.get_database_backend();
    let stmt = Table::alter()
        .table(Alias::new(table_name))
        .add_column(build_optional_column(column_name, column_kind))
        .to_owned();
    db.execute(backend.build(&stmt)).await.map(|_| ())
}
