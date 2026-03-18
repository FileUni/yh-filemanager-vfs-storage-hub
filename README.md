# yh-filemanager-vfs-storage-hub Module Description

This document is the sole detailed documentation for `crates/yh-filemanager-vfs-storage-hub`. Content is verified based on current source implementation (`src/`).

## 1. Module Positioning and Boundaries

- Module Positioning: Unified storage governance core (VFS Hub) for `fileuni`.
- Core Responsibilities:
 - Unified physical file operation entry point (across API/FTP/SFTP/WebDAV/S3 semantic layers).
 - User scope isolation (`create_scoped_engine`).
 - Metadata index write-through and self-healing (`yh_vfs_file_index`).
 - WAL write-ahead log and crash recovery (`yh_vfs_wal`).
 - Quota check and atomic update (`yh_vfs_user_settings`).
 - Recycle bin, favorites, sharing, batch tasks, compression/decompression, and other business capabilities.
- Key Constraint: Upper layer严禁 concatenating physical paths; must operate through scoped engine.

## 2. Key Paths and Module Division of Labor

- Core Entry: `src/vfs/hub.rs`
- Scoped Engine: `src/vfs/scoped/`
- WAL: `src/vfs/wal/`
- Index Service: `src/business/services/file_index_service.rs`
- Quota/User Settings: `src/business/services/user_settings.rs`
- Compression/Decompression: `src/utils/compression.rs`
- Temp File Governance: `src/utils/temp_file.rs`
- Maintenance Tasks: `src/vfs/maintenance/mod.rs`
- Storage Backend Connector: `src/vfs/connector.rs`

## 3. Feature List (By Implementation Status)

### 3.1 Storage Backend Types

- Implemented drivers (OpenDAL): `fs`, `s3`, `webdav`, `memory`.
- Current `connector.rs` implemented OpenDAL backend driver build items do not include `ftp`/`sftp`.

### 3.2 Storage Strategy and User Roles

- Policy Source: `vfs_storage_hub.policies` (`role_id -> pool_name -> default_quota`).
- Routing Logic:
 - Priority to user settings table `yh_vfs_user_settings.pool_name`.
 - Otherwise route by role policy.
 - Then fallback to `default_pool`.
- User first access auto-completes settings (including `base_dir=/users/{user_id}`, default quota).

### 3.3 User File Isolation by `user_id`

- Implemented: Logical path uniformly mapped to physical path `"{user_id}/{relative_path}"`.
- Path Security Defense: Prohibits `..`, `//`, `./`, control characters.
- Path Restoration: Converts physical path back to frontend logical path (starting with `/`) via `translate_file_info`.

### 3.4 Temp File Mechanism

- Logical Prefix: `/.virtual/tmp`.
- Management Capabilities:
 - User-level temp file/directory creation.
 - Active file tracking, avoiding mistaken deletion.
 - RAII auto cleanup + scheduled expiration cleanup.
- WAL Compatible: Can skip temp path WAL via config `wal_skip_temp_path`.

### 3.5 Support for `yh-email-manager` Email Attachments

- No direct crate-level coupling implementation found (no `yh-email-manager` direct call adaptation code).
- Available Base Capabilities: VFS file read/write, streaming read/write, compression/decompression, sharing link can be reused for email attachment scenarios.
- Belongs to "integrable base exists, dedicated adaptation not implemented in this module".

### 3.6 Support for `yh-chat-manager` File Transfer
- Current Status: Supports file metadata transfer via chat protocol.
- Important Limitation: Since `yh-chat-manager` doesn't have message persistence capability, all file references transferred via chat are temporary. System doesn't record file send history. Users should use "File Share" feature for long-term stable access.

### 3.7 Compression and Decompression Mechanism (Built-in + External 7z)

- Current implementation is "hybrid mode", not "pure 7z mode":
 - External `7z`: Main path, supports `zip/7z/tar/gzip/tar.gz` process mode.
 - Rust built-in fallback: `zip/tar.gz/gz` still retained (`compress_*_native`/`decompress_*_native`).
- Queue and Concurrency: Uses `yh_external_process_manager`'s `run_with_permit` for concurrency limiting.
- Security Defense:
 - Pre-compression: `max_compress_items`, `max_compress_total_size_mb`.
 - During compression: Monitors output package size `max_compress_output_size_mb`, kills on exceed.
 - Pre-decompression: Archive size limit `max_decompress_archive_size_mb`.
 - Post-decompression: Traverses output directory accumulating size, limits `max_extract_size_gb` (Zip Bomb protection).
 - Timeout: `timeout_secs` expires kills child process.
- Result Consistency:
 - Triggers `sync_index` after decompression.
 - Supports `delete_source`/`delete_archive` for post-success source deletion.
- Online Capabilities:
 - `stream_compress_reader`: Single file streaming compression download (directory direct stream not supported).
 - `list_archive_contents`: Browse archive directory without extracting.
 - `extract_archive_file`: Single file streaming extraction from archive.

### 3.8 WAL Functionality

- WAL Operation Types: `WRITE/DELETE/MOVE/RENAME/CREATE_DIR/RESTORE_TRASH`.
- Recording Strategy: Pre-write `log_operation`, post-completion `complete_operation`.
- Skip Strategy: Small files (below `wal_min_size_bytes`), temp paths, thumbnail cache paths can skip WAL.

### 3.9 User-Level WAL Locking

- Implemented: `maintenance_mode` maintains user lock set.
- Recovery flow locks affected users (`enter_user_maintenance`), blocks writes (`check_maintenance`).
- RAII Guard auto-unlocks, supports manual unlock.

### 3.10 Admin Forced WAL Interruption Mechanism

- Implemented Capabilities:
 - `VfsWalManager::revoke_all`: Clears WAL table.
 - `clear_all_maintenance`: Force clears all maintenance locks.
- Note: Both combined can achieve "admin forced interrupt recovery chain" operations capability.

### 3.11 Quota Implementation

- Data Fields: `storage_quota`, `storage_used` (user-level).
- Checkpoints:
 - Incremental check during `write/write_at/copy/write_stream/decompress`.
 - Thumbnail cache paths (`/.thumbs`, `/.thumbs_cache`) default not counted in quota.
- Update Strategy: `UPDATE ... storage_used = storage_used + delta` atomic update.
- Exception: `storage_quota == 0` means unlimited.

### 3.12 File List Index

- Design: Database primary, physical secondary; write-through + lazy sync/active sync.
- Key Algorithm: `sync_directory_optimized` (chunked UPSERT + timestamp cleanup), avoiding massive `NOT IN`.
- Concurrency Control: `SyncGuardManager` locks `(user_id, path)`, preventing duplicate sync storms.
- Postgres Optimization: `yh_vfs_file_index` partitioned by `HASH(user_id)` (8 partitions) + prefix index.

### 3.13 Favorites, Sharing, Recycle Bin Mechanisms

- Favorites:
 - Field: `favorite_color`.
 - Logic: `0` is non-favorite, `>0` is favorite.
 - Current code doesn't enforce `0-9`; `vfs/types.rs` comment says `1-7`, actual DB can store any integer.
- Sharing:
 - `yh_vfs_file_shares`, supports password (Argon2 hash), expiration time, download count, direct link flag.
 - Supports paginated search, filtering, sorting, soft delete.
- Recycle Bin:
 - Physical Directory: `/.recycle_bin`.
 - Delete Behavior: Move to recycle bin (not immediate physical delete).
 - Conflict Avoidance: Currently uses `timestamp + original_name` for recycle bin path.
 - Restore Behavior: Restores based on `original_path`.
 - Auto Cleanup: Cleans expired recycle bin files by `trash_retention_days`.

### 3.14 Recycle Bin Auto Cleanup and Anti-Rename

- Auto Cleanup: Implemented (maintenance service periodic cleanup).
- Anti-Rename:
 - Recycle bin entry uses timestamp prefix, reducing rename conflict probability.
 - General path conflict handling capability `get_unique_path` exists and used for batch tasks.
 - `restore_from_trash_impl` currently directly restores by `original_path`, doesn't auto-call `get_unique_path` for restore conflict avoidance; restore target conflict relies on underlying behavior, room for improvement.

### 3.15 Thumbnail Mechanism and Garbage Control

- This module doesn't implement thumbnail "generator".
- Implemented Governance Strategy:
 - Identifies thumbnail cache paths (`/.thumbs`, `/.thumbs_cache`).
 - Thumbnail cache paths default skip quota, skip index, can skip WAL.
 - User settings table has multi-format disable switch fields (`thumbnail_disable_*`), but no specific generation logic consumption found in this crate.
- This is "governance/isolation strategy implemented, thumbnail production chain not landed in this module".

## 4. Core Algorithm Descriptions

### 4.1 Scope Isolation Algorithm

1. Entry validates logical path legality.
2. Physical mapping `logical -> {user_id}/{relative}`.
3. All operations execute within scoped engine, returned paths uniformly converted back to logical paths.

### 4.2 Write-Through Index Algorithm

1. Immediately `upsert_file` after physical write success.
2. Directory list prioritizes DB index read; executes `sync_index` for self-healing when necessary.
3. During sync, preserves business fields (like `favorite_color`) from being overwritten by physical refresh.

### 4.3 WAL Recovery Algorithm (Idempotent)

1. System reads `yh_vfs_wal` for sequential recovery.
2. Places maintenance lock on affected users, blocks writes.
3. Executes compensation based on physical state (like `.tmp` cleanup, suspicious file rename to `.corrupted_*`).
4. Deletes WAL record after success; releases lock after recovery ends.

### 4.4 Quota Atomic Update Algorithm

1. Pre-write calculates `delta` (old-new file difference).
2. When `delta > 0`, validates `storage_used + delta <= storage_quota` (except `quota=0`).
3. Atomically increments/decrements `storage_used` after operation success.
4. `write_stream` continuously checks during write loop, preventing streaming bypass.

### 4.5 Index Sync Algorithm (High-Performance)

1. Collects physical directory snapshot.
2. Chunked UPSERT (500/batch) and updates timestamp.
3. Deletes historical items where `row_updated_at < sync_start`.
4. Returns DB merged result (including business fields like favorites).

### 4.6 Compression/Decompression Security Algorithm

1. Pre-limit: Count, total input volume, archive volume.
2. Child process concurrency quota control (queue permit).
3. Execution monitoring: Timeout kill, output body exceed kill.
4. Post-decompression scan total output volume, intercept decompression bomb.
5. Triggers index sync after upload.

## 5. Configuration Items and Startup Constraints

- This module config is strong validation mode; key fields missing or invalid refuses startup.
- Key items related to compression/decompression:
 - `file_compress.enable`
 - `file_compress.exe_7zip_path`
 - `file_compress.process_manager_max_concurrency`
 - `file_compress.max_cpu_threads`
 - `file_compress.compression_max_level`
 - `file_compress.timeout_secs`
 - `file_compress.max_extract_size_gb`
 - `file_compress.max_compress_items`
 - `file_compress.max_compress_total_size_mb`
 - `file_compress.max_compress_output_size_mb`
 - `file_compress.max_decompress_archive_size_mb`
 - `file_compress.decompression_formats`
 - `file_compress.enable_archive_browser`

## 6. Interaction with Other Modules

- `yh-task-registry`: Submits batch async tasks (move/copy/delete/compress/decompress) via `VfsTaskHandler`.
- `yh-fast-kv-storage-hub`: S3 multipart state cleanup and sync lock assistance.
- `yh-journal-log` (via `VfsJournalRecorder`): Records VFS activity audit.
- Upper protocol modules (API/FTP/SFTP/WebDAV/S3): Reuse same VFS scoped engine, maintaining behavior consistency.

## 7. Currently Confirmable Areas for Improvement

- `migrate_storage` in `ScopedVfsStorageEngine` still returns `Not implemented`.
- `restore_from_trash` conflict avoidance strategy should integrate `get_unique_path`.
- Thumbnail generation chain needs to be completed in upper layer or independent module, and connected with this module's switch fields.
- If goal is "pure 7z architecture", need to remove native fallback branches in `compression.rs`.