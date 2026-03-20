# VFS Storage Hub Knowledge Base

Role: Unified storage abstraction layer and governance core
Backend: OpenDAL for content IO; SeaORM for control-plane metadata

## Structure

```text
src/
├── vfs/
│   ├── hub.rs           # Central entry point (VfsStorageHub)
│   ├── connector.rs     # OpenDAL operator construction and base layers
│   ├── pool.rs          # Thin OpenDAL-backed storage adapter
│   ├── wal/             # WAL / recovery table and replay logic
│   └── scoped/          # User-isolated engine
│       ├── read.rs      # Read path, stat/list/index self-heal
│       ├── write.rs     # Mutations, WAL, quota, write-through index
│       ├── ops.rs       # Trash, favorite, sync, business ops
│       ├── batch.rs     # Persistent async batch tasks
│       └── internal.rs  # Path security, temp/scratch, cache helpers
├── business/
│   ├── entities/        # Database models (yh_vfs_*)
│   └── services/        # Stable business/data access services
└── utils/
    ├── temp_file.rs     # Internal scratch temp manager
    └── cache.rs         # Optional KV-based response cache helper
```

## Critical Mechanisms

| Mechanism | Responsibility | Rule |
|-----------|----------------|------|
| Scoped Engine | Path isolation | Always create storage via `create_scoped_engine(...)` |
| OpenDAL Pool | Physical content IO | Only data plane, never business truth |
| Backup Connector | Read-only failover | Only read/stat/list fallback; writes are never mirrored |
| File Index | Metadata truth for browse/search/trash/favorite/share state | Physical success after write-through update |
| WAL | Operation journal for physical mutations | Only VFS scoped engine can record or complete WAL |
| Recycle Bin | Safety for user deletes | Delete means dedicated `MOVE_TO_TRASH` journal + move into `/.recycle_bin/` |
| Thumbnail Cache | User-owned per-directory cache | `/.thumbs` stays inside user directory tree |
| Scratch Temp | Internal transient workspace | Local temp manager only, never exposed as physical storage path |

## Governance Rules

1. Single truth for physical IO: API, WebDAV, FTP, SFTP, S3 and all upper layers must go through VFS Hub.
2. Single truth for WAL: upper layers must never call `VfsWalManager::log_operation` or `complete_operation` directly.
3. No physical path concat in upper layers: upper protocols may work only with logical paths and scoped engines.
4. Control-plane metadata stays in services: if a stable service API exists, upper layers must not query `yh_vfs_*` tables directly.
5. Quota check happens before physical write; quota usage update happens only after successful physical mutation.
6. If DB index update fails after physical success, log loudly and repair later; never rollback the physical file in-band.
7. `wal_min_size_bytes` only applies to regular file writes; delete/move/create-dir control operations are still WAL-tracked.
8. WAL rows are not deleted on success anymore; they transition through journal states such as `pending`, `physical_done`, `metadata_done`, `completed`, and `failed`.
9. `enable_write_cache` is reserved and must remain `false`; scoped engine streaming writes already provide the supported safe staging path.
10. `/.thumbs` is intentionally mixed into user directories for user-controlled cleanup, but should stay hidden from normal listings unless a dedicated UI chooses to expose it.
11. `/.virtual/tmp` is a logical temp namespace; internal upload/render/decompress scratch files belong to `utils/temp_file.rs`, not to user-visible storage.

## Data Flow

1. Request enters scoped engine and passes logical path validation.
2. Scoped engine performs quota / maintenance / path policy checks.
3. For mutations, scoped engine records WAL before the physical operation that needs recovery.
4. OpenDAL executes the physical content operation.
5. Scoped engine updates file index and related control-plane metadata.
6. WAL is completed only after the mutation finishes successfully.

## Practical Reading Order

1. `src/vfs/connector.rs` for OpenDAL construction rules
2. `src/vfs/pool.rs` for raw IO semantics
3. `src/vfs/scoped/internal.rs` for path, temp, WAL helper rules
4. `src/vfs/scoped/write.rs` for mutation orchestration
5. `src/vfs/scoped/read.rs` for index-first browse and stat logic
6. `src/business/services/file_index_service.rs` for metadata truth and share/trash helpers
7. `src/business/services/user_settings.rs` for quota, S3 keys and user settings boundary

## What Upper Layers Should Assume

- `VfsStorage` is the stable contract; protocol crates should prefer it over direct table access.
- WAL, quota, temp scratch, index write-through and path isolation are internal VFS responsibilities.
- Thumbnail generation may live outside this crate, but thumbnail storage policy and hidden-path governance must follow VFS rules.
- `src/vfs/cache/read_cache.rs`: read cache is acceleration only; use TTL plus access-biased eviction, and control thumbnail/extension bypass through policy instead of scattered `if` branches.
- `src/vfs/cache/write_cache.rs`: write cache is admission-only write-back; hot path stays DB-light, same-path mutations must force flush first, and successful flushes are reconciled by directory-batched index sync plus user-batched quota sync instead of per-file DB writes.
- Deadline-exceeded pending writes spill to local abnormal storage before journal audit; pending bytes must never be capacity-evicted, and config examples for read/write cache must stay `enable = false`.
