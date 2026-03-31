use crate::business::entities::{file_index, file_share, remote_mount, ssh_keys, user_settings};
use sea_orm::sea_query::Index;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DbBackend, DbErr, EntityTrait, QueryFilter,
    Schema, Statement,
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
    add_user_settings_column_if_missing(
        db,
        "protected_root",
        "protected_root TEXT NULL",
        "protected_root TEXT NULL",
        "protected_root TEXT NULL",
    )
    .await?;
    add_user_settings_column_if_missing(
        db,
        "protected_mode",
        "protected_mode TEXT NULL",
        "protected_mode TEXT NULL",
        "protected_mode TEXT NULL",
    )
    .await?;
    add_user_settings_column_if_missing(
        db,
        "protected_key_slot_id",
        "protected_key_slot_id TEXT NULL",
        "protected_key_slot_id TEXT NULL",
        "protected_key_slot_id TEXT NULL",
    )
    .await?;
    add_user_settings_column_if_missing(
        db,
        "protected_wrapped_key",
        "protected_wrapped_key TEXT NULL",
        "protected_wrapped_key TEXT NULL",
        "protected_wrapped_key TEXT NULL",
    )
    .await?;
    add_user_settings_column_if_missing(
        db,
        "protected_enabled_at",
        "protected_enabled_at TEXT NULL",
        "protected_enabled_at TIMESTAMPTZ NULL",
        "protected_enabled_at TIMESTAMP NULL",
    )
    .await?;
    add_user_settings_column_if_missing(
        db,
        "protected_updated_at",
        "protected_updated_at TEXT NULL",
        "protected_updated_at TIMESTAMPTZ NULL",
        "protected_updated_at TIMESTAMP NULL",
    )
    .await?;
    Ok(())
}

async fn ensure_file_index_columns(db: &Arc<sea_orm::DatabaseConnection>) -> Result<(), DbErr> {
    add_file_index_column_if_missing(
        db,
        "physical_size",
        "physical_size BIGINT NULL",
        "physical_size BIGINT NULL",
        "physical_size BIGINT NULL",
    )
    .await?;
    add_file_index_column_if_missing(
        db,
        "protected_meta",
        "protected_meta TEXT NULL",
        "protected_meta TEXT NULL",
        "protected_meta TEXT NULL",
    )
    .await?;
    Ok(())
}

async fn ensure_file_share_columns(db: &Arc<sea_orm::DatabaseConnection>) -> Result<(), DbErr> {
    add_file_share_column_if_missing(
        db,
        "note",
        "note TEXT NULL",
        "note TEXT NULL",
        "note TEXT NULL",
    )
    .await?;
    add_file_share_column_if_missing(
        db,
        "label",
        "label TEXT NULL",
        "label TEXT NULL",
        "label TEXT NULL",
    )
    .await?;
    add_file_share_column_if_missing(
        db,
        "attributes",
        "attributes TEXT NULL",
        "attributes TEXT NULL",
        "attributes TEXT NULL",
    )
    .await?;
    add_file_share_column_if_missing(
        db,
        "hide_download",
        "hide_download INTEGER NOT NULL DEFAULT 0",
        "hide_download BOOLEAN NOT NULL DEFAULT FALSE",
        "hide_download BOOLEAN NOT NULL DEFAULT FALSE",
    )
    .await?;
    add_file_share_column_if_missing(
        db,
        "snapshot_path",
        "snapshot_path TEXT NULL",
        "snapshot_path TEXT NULL",
        "snapshot_path TEXT NULL",
    )
    .await?;
    add_file_share_column_if_missing(
        db,
        "snapshot_name",
        "snapshot_name TEXT NULL",
        "snapshot_name TEXT NULL",
        "snapshot_name TEXT NULL",
    )
    .await?;
    add_file_share_column_if_missing(
        db,
        "snapshot_is_dir",
        "snapshot_is_dir INTEGER NULL",
        "snapshot_is_dir BOOLEAN NULL",
        "snapshot_is_dir BOOLEAN NULL",
    )
    .await?;
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
    sqlite_def: &str,
    postgres_def: &str,
    mysql_def: &str,
) -> Result<(), DbErr> {
    let backend = db.get_database_backend();
    let sql = match backend {
        DbBackend::Sqlite => format!("ALTER TABLE yh_vfs_file_shares ADD COLUMN {}", sqlite_def),
        DbBackend::Postgres => {
            format!(
                "ALTER TABLE yh_vfs_file_shares ADD COLUMN IF NOT EXISTS {}",
                postgres_def
            )
        }
        DbBackend::MySql => format!("ALTER TABLE yh_vfs_file_shares ADD COLUMN {}", mysql_def),
    };
    match db.execute(Statement::from_string(backend, sql)).await {
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
    sqlite_def: &str,
    postgres_def: &str,
    mysql_def: &str,
) -> Result<(), DbErr> {
    let backend = db.get_database_backend();
    let sql = match backend {
        DbBackend::Sqlite => format!("ALTER TABLE yh_vfs_user_settings ADD COLUMN {}", sqlite_def),
        DbBackend::Postgres => {
            format!(
                "ALTER TABLE yh_vfs_user_settings ADD COLUMN IF NOT EXISTS {}",
                postgres_def
            )
        }
        DbBackend::MySql => format!("ALTER TABLE yh_vfs_user_settings ADD COLUMN {}", mysql_def),
    };
    match db.execute(Statement::from_string(backend, sql)).await {
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
    sqlite_def: &str,
    postgres_def: &str,
    mysql_def: &str,
) -> Result<(), DbErr> {
    let backend = db.get_database_backend();
    let sql = match backend {
        DbBackend::Sqlite => format!("ALTER TABLE yh_vfs_file_index ADD COLUMN {}", sqlite_def),
        DbBackend::Postgres => {
            format!(
                "ALTER TABLE yh_vfs_file_index ADD COLUMN IF NOT EXISTS {}",
                postgres_def
            )
        }
        DbBackend::MySql => format!("ALTER TABLE yh_vfs_file_index ADD COLUMN {}", mysql_def),
    };
    match db.execute(Statement::from_string(backend, sql)).await {
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
