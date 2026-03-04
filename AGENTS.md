# VFS Storage Hub Knowledge Base

**Role**: Unified Storage Abstraction Layer & Governance Core
**Backend**: OpenDAL (S3, FS, WebDAV, FTP support)

## 📂 STRUCTURE
```
src/
├── vfs/
│   ├── hub.rs           # Central entry point (VfsStorageHub)
│   ├── scoped/          # User-isolated engine (Modularized)
│   │   ├── mod.rs       # Interface dispatch
│   │   ├── read.rs      # Read-only operations
│   │   ├── write.rs     # Creation & mutation
│   │   ├── ops.rs       # Control & metadata ops
│   │   ├── batch.rs     # Async tasks & batches
│   │   └── internal.rs  # Security & path logic
│   ├── wal/             # Write-Ahead Log & Recovery
│   └── connector.rs     # Metadata indexing (Write-through)
├── business/
│   ├── entities/        # Database models (yh_vfs_*)
│   └── services/        # High-level business logic
```

## 📍 CRITICAL MECHANISMS
| Mechanism | Responsibility | Rule |
|-----------|----------------|------|
| **Scoped Engine** | Path isolation | **ALWAYS** use `create_scoped_storage_engine(user_id)` |
| **Write-Through** | Index consistency | Physical op success → Immediate DB update |
| **WAL** | Crash recovery | Log before write. Idempotent recovery. |
| **Recycle Bin** | Safety | Delete = Move to `/.recycle_bin/` |
| **Virtual Buckets** | S3 compat | `user-data` bucket maps to user root |

## 🚨 GOVERNANCE RULES (STRICT)
1.  **Single Truth**: All physical IO MUST go through VFS Hub.
2.  **No Path Concat**: NEVER concatenate physical paths in upper layers (API/FTP).
3.  **Atomic Quota**: Quota checks must happen *before* physical write.
4.  **Zero Panic**: Use `Result<?, AppError>`. No `.unwrap()`.
5.  **Consistency**: If DB update fails, log error but *do not* revert physical file (repair later).

## 🔄 DATA FLOW
1.  **Request** → `ScopedStorageEngine` (validates path/quota)
2.  **WAL** → Log intent (Pending)
3.  **OpenDAL** → Physical Operation
4.  **WAL** → Mark committed
5.  **Index** → Update `yh_vfs_file_index`
