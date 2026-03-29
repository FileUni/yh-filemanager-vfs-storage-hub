# VFS Storage Hub Knowledge Base
Role: unified storage abstraction and governance core.
Backend: OpenDAL for content IO, SeaORM for control-plane metadata.

## Structure
```text
src/
├── vfs/        # hub, connector, pool, wal, scoped engine
├── business/   # yh_vfs_* entities and stable services
└── utils/      # temp files and cache helpers
```

## Core Rules
1. API, WebDAV, FTP, SFTP, S3, and Nextcloud file IO must go through `VfsStorage` or a scoped engine.
2. Create user storage only with `create_scoped_engine(...)`; upper layers work with logical paths, not physical paths.
3. OpenDAL is the data plane only; the file index is the truth for browse, search, trash, favorite, and share state.
4. Only the scoped engine may record or complete WAL; upper layers must not call WAL manager methods directly.
5. Check quota before write, and update quota only after physical success.
6. If physical IO succeeds but index update fails, log it and repair later; do not roll back the file in-band.
7. Delete means a journaled move into `/.recycle_bin/`; `/.thumbs` stays in the user tree and stays hidden by default.
8. `/.virtual/tmp` is logical temp space only; internal scratch files belong to `utils/temp_file.rs`.
9. `enable_write_cache` stays `false`; current scoped streaming writes are the supported safe path.
10. WAL rows are journals with states such as `pending`, `physical_done`, `metadata_done`, `completed`, and `failed`.

## Flow
1. Validate the logical path in the scoped engine.
2. Run quota, maintenance, and path policy checks.
3. Record WAL for recoverable mutations.
4. Execute physical IO through OpenDAL.
5. Update file index and related metadata.
6. Mark WAL completed after full success.

## Nextcloud Boundary
- Nextcloud compatibility is an upper-layer concern; VFS remains the storage truth.
- Nextcloud-specific IDs, custom props, and protocol views must use dedicated helpers or fields, not VFS primary semantics.
- Nextcloud preview and thumbnail behavior must reuse the shared VFS thumbnail service, not a separate storage layout.
- Nextcloud and WebDAV handlers may translate protocol behavior, but must not bypass VFS for files, quota, trash, or metadata sync.

## Reading Order
1. `src/vfs/connector.rs`
2. `src/vfs/pool.rs`
3. `src/vfs/scoped/internal.rs`
4. `src/vfs/scoped/write.rs`
5. `src/vfs/scoped/read.rs`
6. `src/business/services/file_index_service.rs` and `src/business/services/user_settings.rs`
