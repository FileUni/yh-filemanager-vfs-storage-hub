# VFS Storage Hub Rules
Role: unified storage abstraction and governance core.
Backend: OpenDAL for content IO, SeaORM for control-plane metadata.

## Layout
```text
src/
├── vfs/        # hub, scoped engine, OpenDAL adapters, WAL
├── business/   # yh_vfs_* entities, file index, share, thumbnail, nextcloud
└── utils/      # temp files and cache helpers
```

## Rules
1. API, WebDAV, FTP, SFTP, S3, and Nextcloud file IO must go through `VfsStorage` or a scoped engine.
2. Create per-user storage only with `create_scoped_engine(...)`.
3. Upper layers may use logical paths only; never build physical paths.
4. OpenDAL is the data plane only; the file index is the truth for browse, search, trash, favorite, and share state.
5. If a stable VFS service exists, upper layers must not query `yh_vfs_*` tables directly.
6. Only the scoped engine may record or complete WAL.
7. Check quota before write; update quota only after physical success.
8. If physical IO succeeds and metadata sync fails, log loudly and repair later; never roll back the file in-band.
9. Delete means a journaled move into `/.recycle_bin/`.
10. `/.thumbs` stays inside the user tree and stays hidden by default.
11. `/.virtual/tmp` is logical temp space only; internal scratch files belong to `utils/temp_file.rs`.
12. `enable_write_cache` stays `false`.
13. WAL rows are journals with states such as `pending`, `physical_done`, `metadata_done`, `completed`, and `failed`.

## Mutation Flow
1. Validate the logical path.
2. Run quota, maintenance, and path policy checks.
3. Record WAL for recoverable mutations.
4. Execute physical IO through OpenDAL.
5. Update file index and related metadata.
6. Mark WAL completed after full success.

## Nextcloud Boundary
1. Nextcloud compatibility lives in protocol layers; VFS remains the storage truth.
2. Nextcloud-specific IDs, props, and protocol views must use dedicated helpers or fields, not VFS primary semantics.
3. Nextcloud preview and thumbnail behavior must reuse the shared VFS thumbnail service.
4. Nextcloud and WebDAV handlers may translate protocol behavior, but must not bypass VFS for files, quota, trash, or metadata sync.

## Reading Order
1. `src/vfs/connector.rs`
2. `src/vfs/pool.rs`
3. `src/vfs/scoped/internal.rs`
4. `src/vfs/scoped/write.rs`
5. `src/vfs/scoped/read.rs`
6. `src/business/services/file_index_service.rs`
7. `src/business/services/user_settings.rs`
