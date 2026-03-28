// VFS storage hub configuration module
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::OwnedRwLockReadGuard;
pub use yh_config_infra::{
    BaseConfigManager, ConfigApp, ConfigDoc, impl_config_manager_boilerplate,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HardwareProfile {
    LowMemory,
    Balanced,
    Throughput,
}

fn require_allocator_profile() -> HardwareProfile {
    match yh_config_infra::utils::current_hardware_profile() {
        "low_memory" => HardwareProfile::LowMemory,
        "throughput" => HardwareProfile::Throughput,
        _ => HardwareProfile::Balanced,
    }
}

#[cfg(any(target_os = "android", target_os = "ios"))]
mod mobile_fs_guard {
    use std::path::{Component, Path, PathBuf};

    fn normalize_absolute_path(path: &Path) -> Result<PathBuf, String> {
        if !path.is_absolute() {
            return Err("path must be absolute".to_string());
        }
        let mut out = PathBuf::new();
        for comp in path.components() {
            match comp {
                Component::Prefix(prefix) => out.push(prefix.as_os_str()),
                Component::RootDir => out.push(comp.as_os_str()),
                Component::CurDir => {}
                Component::ParentDir => {
                    let _ = out.pop();
                }
                Component::Normal(part) => out.push(part),
            }
        }
        Ok(out)
    }

    pub fn validate_fs_root_under_runtime_dir(root: &str) -> Result<(), String> {
        let runtime_dir = yh_config_infra::utils::get_runtime_dir()
            .ok_or_else(|| "{RUNTIMEDIR} is not initialized".to_string())?;

        let base = normalize_absolute_path(Path::new(runtime_dir.as_ref()))?;
        let candidate = normalize_absolute_path(Path::new(root))?;

        if !candidate.starts_with(&base) {
            return Err(format!(
                "fs connector root must be inside app sandbox (under RUNTIMEDIR). Got: '{}' (RUNTIMEDIR='{}')",
                root, runtime_dir
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ConfigDoc)]
pub struct VfsConnectorConfig {
    #[config(
        desc_zh = "存储连接器唯一标识名称，用于存储池引用",
        desc_en = "Unique identifier name of storage connector for storage pool reference",
        example = "local-fs"
    )]
    pub name: Option<Arc<str>>,
    #[config(
        desc_zh = "存储后端驱动类型，可选项: fs(本地文件系统)|memory(内存)|android_saf(Android SAF 授权目录)|ios_scoped_fs(iOS 安全作用域目录)|s3(AWS S3兼容)|webdav(WebDAV)|dropbox(Dropbox)|onedrive(OneDrive Personal)|gdrive(Google Drive)，默认fs",
        desc_en = "Storage backend driver type, options: fs(local filesystem)|memory(in-memory)|android_saf(Android SAF granted directory)|ios_scoped_fs(iOS security-scoped directory)|s3(AWS S3 compatible)|webdav|dropbox|onedrive(OneDrive Personal)|gdrive(Google Drive), default fs",
        example = "fs"
    )]
    pub driver: Option<Arc<str>>,
    #[config(
        desc_zh = "存储根定位：fs=目录路径；s3/webdav/dropbox/onedrive/gdrive=工作根目录；android_saf=SAF tree uri(content://...)；ios_scoped_fs=bookmark_b64:<BASE64>",
        desc_en = "Storage root locator: fs=directory path; s3/webdav/dropbox/onedrive/gdrive=working root directory; android_saf=SAF tree uri (content://...); ios_scoped_fs=bookmark_b64:<BASE64>",
        example = "{RUNTIMEDIR}/vfs"
    )]
    pub root: Option<Arc<str>>,
    #[config(
        desc_zh = "是否启用此连接器，禁用后将无法用于文件操作",
        desc_en = "Whether to enable this connector, file operations will not be available when disabled",
        example = "true"
    )]
    pub enable: Option<bool>,
    #[config(
        desc_zh = "连接器特定配置选项，键值对形式传递给OpenDAL底层驱动",
        desc_en = "Connector-specific configuration options passed to OpenDAL underlying driver in key-value pairs",
        example = "{}"
    )]
    pub options: Option<HashMap<String, String>>,
}

impl VfsConnectorConfig {
    pub fn get_name(&self) -> &str {
        yh_config_infra::config_require_str!(self.name, "vfs_storage_hub", "connector name")
    }
    pub fn get_driver(&self) -> &str {
        yh_config_infra::config_require_str!(self.driver, "vfs_storage_hub", "connector driver")
    }
}

// Storage pool configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ConfigDoc)]
pub struct VfsPoolConfig {
    #[config(
        desc_zh = "存储池唯一标识名称，用于策略映射和引用",
        desc_en = "Unique identifier name of storage pool for policy mapping and reference",
        example = "default-pool"
    )]
    pub name: Option<Arc<str>>,
    #[config(
        desc_zh = "主存储连接器名称，用于正常读写操作",
        desc_en = "Primary storage connector name for normal read/write operations",
        example = "local-fs"
    )]
    pub primary_connector: Option<Arc<str>>,
    #[config(
        desc_zh = "备用存储连接器名称，仅用于只读兜底（read/stat/list 等）；写入仍只落主连接器，不做双写复制",
        desc_en = "Backup storage connector name used for read-only failover (read/stat/list). Writes still go to the primary connector only and are not mirrored",
        example = "local-backup"
    )]
    pub backup_connector: Option<Arc<str>>,
    #[config(
        desc_zh = "保留字段，当前不支持独立写入缓存；必须为 false，流式写入已经由 scoped engine 自带安全暂存",
        desc_en = "Reserved field. Independent write cache is currently unsupported and must remain false; scoped engine streaming writes already provide safe staging",
        example = "false"
    )]
    pub enable_write_cache: Option<bool>,
    #[config(
        desc_zh = "是否启用此存储池，禁用后将无法分配给用户",
        desc_en = "Whether to enable this storage pool, cannot be assigned to users when disabled",
        example = "true"
    )]
    pub enable: Option<bool>,
    #[config(
        desc_zh = "存储池特定配置选项，用于自定义存储行为",
        desc_en = "Storage pool specific configuration options for customizing storage behavior",
        example = "{}"
    )]
    pub options: Option<HashMap<String, String>>,
}

impl VfsPoolConfig {
    pub fn get_name(&self) -> &str {
        yh_config_infra::config_require_str!(self.name, "vfs_storage_hub", "pool name")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ConfigDoc)]
pub struct VfsPolicyConfig {
    #[config(
        desc_zh = "用户角色ID，用于区分不同用户群体的存储策略",
        desc_en = "User role ID for distinguishing storage policies of different user groups",
        example = "0"
    )]
    pub role_id: Option<Arc<str>>,
    #[config(
        desc_zh = "分配给此角色的存储池名称，决定文件存储位置",
        desc_en = "Storage pool name assigned to this role, determines file storage location",
        example = "default-pool"
    )]
    pub pool_name: Option<Arc<str>>,
    #[config(
        desc_zh = "此角色用户的默认存储配额（字节），超出将无法上传",
        desc_en = "Default storage quota (bytes) for users of this role, uploads will be rejected when exceeded",
        example = "10737418240"
    )]
    pub default_quota: Option<i64>,
    #[config(
        desc_zh = "此角色用户允许创建的私有远程挂载数量上限；0 表示不允许自助挂载",
        desc_en = "Maximum number of private remote mounts allowed for users of this role; 0 disables self-service mounts",
        example = "3"
    )]
    pub max_private_mounts: Option<usize>,
    #[config(
        desc_zh = "此角色用户允许设置的最小同步频率（分钟）；用户不能设置得更低",
        desc_en = "Minimum sync interval in minutes allowed for users of this role; users cannot choose a lower value",
        example = "5"
    )]
    pub min_mount_sync_interval_minutes: Option<u64>,
    #[config(
        desc_zh = "此角色用户单次远程挂载同步任务允许的最长执行时间（秒）",
        desc_en = "Maximum execution time in seconds for a single remote mount sync task for users of this role",
        example = "900"
    )]
    pub max_mount_sync_timeout_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ConfigDoc)]
pub struct VfsFileCompressConfig {
    #[config(
        desc_zh = "启用文件压缩和解压功能，支持在线创建压缩包和解压归档。低性能设备建议设为false以节省内存和CPU",
        desc_en = "Enable file compression and decompression feature, supports online creating archives and extracting. Low-performance devices: recommend false to save memory and CPU",
        example = "true"
    )]
    pub enable: Option<bool>,
    #[config(
        desc_zh = "7-Zip 可执行命令或绝对路径。默认建议使用命令名 \"7z\"（依赖系统 PATH）；Windows 常见路径：C:/Program Files/7-Zip/7z.exe，Linux 常见路径：/usr/bin/7z。若为空字符串则禁用 7z 相关格式和多线程增强，仅保留原生 ZIP/Tar 支持。",
        desc_en = "7-Zip executable command or absolute path. Default recommended value is command name \"7z\" (resolved via system PATH); common Windows path: C:/Program Files/7-Zip/7z.exe, common Linux path: /usr/bin/7z. If empty, 7z-related formats and multi-threading enhancements are disabled, keeping only native ZIP/Tar support.",
        example = "7z"
    )]
    pub exe_7zip_path: Option<Arc<str>>,
    #[config(
        desc_zh = "默认压缩格式，可选项: zip(推荐，兼容性好)|7z(压缩率高)|tar.gz(tar+gzip)|tar.bz2，默认zip",
        desc_en = "Default compression format, options: zip(recommended, good compatibility)|7z(high compression rate)|tar.gz(tar+gzip)|tar.bz2, default zip",
        example = "zip"
    )]
    pub default_compression_format: Option<Arc<str>>,
    #[config(
        desc_zh = "压缩/解压进程最大并发数。低性能设备建议设为1，32MB内存设备建议禁用压缩功能",
        desc_en = "Maximum concurrent compression/decompression processes. Low-performance devices: recommend 1, 32MB RAM: recommend disabling compression",
        example = "2"
    )]
    pub process_manager_max_concurrency: Option<usize>,
    #[config(
        desc_zh = "低内存档位（memory_allocator.profile=low_memory）时的压缩/解压进程最大并发数，必须显式配置",
        desc_en = "Max concurrent compression/decompression processes for low_memory hardware profile (memory_allocator.profile=low_memory), must be explicitly configured",
        example = "1"
    )]
    pub process_manager_max_concurrency_low_memory: Option<usize>,
    #[config(
        desc_zh = "高吞吐档位（memory_allocator.profile=throughput）时的压缩/解压进程最大并发数，必须显式配置",
        desc_en = "Max concurrent compression/decompression processes for throughput hardware profile (memory_allocator.profile=throughput), must be explicitly configured",
        example = "4"
    )]
    pub process_manager_max_concurrency_throughput: Option<usize>,
    #[config(
        desc_zh = "单个压缩/解压任务最大使用的CPU线程数。低性能设备建议设为1",
        desc_en = "Maximum CPU threads used by a single compression/decompression task. Low-performance devices: recommend 1",
        example = "2"
    )]
    pub max_cpu_threads: Option<usize>,
    #[config(
        desc_zh = "低内存档位（memory_allocator.profile=low_memory）时单任务最大CPU线程数，必须显式配置",
        desc_en = "Max CPU threads per compression/decompression task for low_memory hardware profile, must be explicitly configured",
        example = "1"
    )]
    pub max_cpu_threads_low_memory: Option<usize>,
    #[config(
        desc_zh = "高吞吐档位（memory_allocator.profile=throughput）时单任务最大CPU线程数，必须显式配置",
        desc_en = "Max CPU threads per compression/decompression task for throughput hardware profile, must be explicitly configured",
        example = "4"
    )]
    pub max_cpu_threads_throughput: Option<usize>,
    #[config(
        desc_zh = "压缩级别，范围: 0-9，默认5，0=无压缩最快，9=最高压缩率最慢，建议3-7平衡性能",
        desc_en = "Compression level, range: 0-9, default 5, 0=no compression fastest, 9=highest compression slowest, recommended 3-7 for balance",
        example = "5"
    )]
    pub compression_max_level: Option<u8>,
    #[config(
        desc_zh = "压缩/解压任务超时时间（秒），超时将终止任务",
        desc_en = "Compression/decompression task timeout (seconds), task will be terminated when timeout",
        example = "3600"
    )]
    pub timeout_secs: Option<u64>,
    #[config(
        desc_zh = "允许解压的最大归档大小（GB），超出将拒绝解压",
        desc_en = "Maximum allowed archive size (GB) for extraction, extraction will be rejected beyond this limit",
        example = "50"
    )]
    pub max_extract_size_gb: Option<u64>,
    #[config(
        desc_zh = "单次压缩操作允许的最大文件数量，超出将分批处理",
        desc_en = "Maximum number of files allowed in single compression operation, will process in batches beyond this limit",
        example = "1000"
    )]
    pub max_compress_items: Option<usize>,
    #[config(
        desc_zh = "单次压缩操作允许的总输入文件大小（MB）",
        desc_en = "Total input file size (MB) allowed in single compression operation",
        example = "4096"
    )]
    pub max_compress_total_size_mb: Option<u64>,
    #[config(
        desc_zh = "单次压缩操作允许的最大输出压缩包大小（MB）",
        desc_en = "Maximum output archive size (MB) allowed in single compression operation",
        example = "4096"
    )]
    pub max_compress_output_size_mb: Option<u64>,
    #[config(
        desc_zh = "允许解压的最大归档文件大小（MB），超出将拒绝解压",
        desc_en = "Maximum archive file size (MB) allowed for extraction, extraction will be rejected beyond this limit",
        example = "4096"
    )]
    pub max_decompress_archive_size_mb: Option<u64>,
    #[config(
        desc_zh = "支持的解压格式列表，如zip、tar.gz、7z等",
        desc_en = "Supported decompression formats list, such as zip, tar.gz, 7z, etc.",
        example = "[\"zip\",\"tar.gz\",\"7z\"]"
    )]
    pub decompression_formats: Option<Vec<Arc<str>>>,
    #[config(
        desc_zh = "启用归档浏览器，无需解压即可浏览归档内文件列表",
        desc_en = "Enable archive browser to view file list inside archive without extraction",
        example = "true"
    )]
    pub enable_archive_browser: Option<bool>,
}

impl VfsFileCompressConfig {
    pub fn is_enabled(&self) -> bool {
        yh_config_infra::config_require_clone!(
            self.enable,
            "vfs_storage_hub",
            "file_compress.enable"
        )
    }
    pub fn get_exe_7zip_path(&self) -> &str {
        yh_config_infra::config_require_str!(
            self.exe_7zip_path,
            "vfs_storage_hub",
            "file_compress.exe_7zip_path"
        )
    }
    pub fn get_default_compression_format(&self) -> &str {
        yh_config_infra::config_require_str!(
            self.default_compression_format,
            "vfs_storage_hub",
            "file_compress.default_compression_format"
        )
    }
    pub fn get_process_manager_max_concurrency(&self) -> usize {
        yh_config_infra::config_require_clone!(
            self.process_manager_max_concurrency,
            "vfs_storage_hub",
            "file_compress.process_manager_max_concurrency"
        )
    }
    pub fn get_effective_process_manager_max_concurrency(&self) -> usize {
        match require_allocator_profile() {
            HardwareProfile::LowMemory => {
                yh_config_infra::config_require_clone!(
                    self.process_manager_max_concurrency_low_memory,
                    "vfs_storage_hub",
                    "file_compress.process_manager_max_concurrency_low_memory"
                )
            }
            HardwareProfile::Throughput => {
                yh_config_infra::config_require_clone!(
                    self.process_manager_max_concurrency_throughput,
                    "vfs_storage_hub",
                    "file_compress.process_manager_max_concurrency_throughput"
                )
            }
            HardwareProfile::Balanced => self.get_process_manager_max_concurrency(),
        }
    }
    pub fn get_max_cpu_threads(&self) -> usize {
        yh_config_infra::config_require_clone!(
            self.max_cpu_threads,
            "vfs_storage_hub",
            "file_compress.max_cpu_threads"
        )
    }
    pub fn get_effective_max_cpu_threads(&self) -> usize {
        match require_allocator_profile() {
            HardwareProfile::LowMemory => {
                yh_config_infra::config_require_clone!(
                    self.max_cpu_threads_low_memory,
                    "vfs_storage_hub",
                    "file_compress.max_cpu_threads_low_memory"
                )
            }
            HardwareProfile::Throughput => {
                yh_config_infra::config_require_clone!(
                    self.max_cpu_threads_throughput,
                    "vfs_storage_hub",
                    "file_compress.max_cpu_threads_throughput"
                )
            }
            HardwareProfile::Balanced => self.get_max_cpu_threads(),
        }
    }
    pub fn get_compression_max_level(&self) -> u8 {
        yh_config_infra::config_require_clone!(
            self.compression_max_level,
            "vfs_storage_hub",
            "file_compress.compression_max_level"
        )
    }
    pub fn get_timeout_secs(&self) -> u64 {
        yh_config_infra::config_require_clone!(
            self.timeout_secs,
            "vfs_storage_hub",
            "file_compress.timeout_secs"
        )
    }
    pub fn get_max_extract_size_gb(&self) -> u64 {
        yh_config_infra::config_require_clone!(
            self.max_extract_size_gb,
            "vfs_storage_hub",
            "file_compress.max_extract_size_gb"
        )
    }
    pub fn get_max_compress_items(&self) -> usize {
        yh_config_infra::config_require_clone!(
            self.max_compress_items,
            "vfs_storage_hub",
            "file_compress.max_compress_items"
        )
    }
    pub fn get_max_compress_total_size_mb(&self) -> u64 {
        yh_config_infra::config_require_clone!(
            self.max_compress_total_size_mb,
            "vfs_storage_hub",
            "file_compress.max_compress_total_size_mb"
        )
    }
    pub fn get_max_compress_output_size_mb(&self) -> u64 {
        yh_config_infra::config_require_clone!(
            self.max_compress_output_size_mb,
            "vfs_storage_hub",
            "file_compress.max_compress_output_size_mb"
        )
    }
    pub fn get_max_decompress_archive_size_mb(&self) -> u64 {
        yh_config_infra::config_require_clone!(
            self.max_decompress_archive_size_mb,
            "vfs_storage_hub",
            "file_compress.max_decompress_archive_size_mb"
        )
    }
    pub fn get_decompression_formats(&self) -> &[Arc<str>] {
        yh_config_infra::config_require_slice!(
            self.decompression_formats,
            "vfs_storage_hub",
            "file_compress.decompression_formats"
        )
    }
    pub fn is_enable_archive_browser(&self) -> bool {
        yh_config_infra::config_require_clone!(
            self.enable_archive_browser,
            "vfs_storage_hub",
            "file_compress.enable_archive_browser"
        )
    }

    pub fn validate(&self, section: &str, errors: &mut Vec<String>) {
        let s = section;
        yh_config_infra::config_collect_bool!(self.enable, s, "enable", errors);
        // Allow empty string to explicitly disable 7z integration while keeping native ZIP/Tar/Gzip.
        yh_config_infra::config_collect_any!(self.exe_7zip_path, s, "exe_7zip_path", errors);
        yh_config_infra::config_collect_not_empty!(
            self.default_compression_format,
            s,
            "default_compression_format",
            errors
        );
        yh_config_infra::config_collect_gt_zero!(
            self.process_manager_max_concurrency,
            s,
            "process_manager_max_concurrency",
            errors
        );
        yh_config_infra::config_collect_gt_zero!(
            self.process_manager_max_concurrency_low_memory,
            s,
            "process_manager_max_concurrency_low_memory",
            errors
        );
        yh_config_infra::config_collect_gt_zero!(
            self.process_manager_max_concurrency_throughput,
            s,
            "process_manager_max_concurrency_throughput",
            errors
        );
        yh_config_infra::config_collect_gt_zero!(
            self.max_cpu_threads,
            s,
            "max_cpu_threads",
            errors
        );
        yh_config_infra::config_collect_gt_zero!(
            self.max_cpu_threads_low_memory,
            s,
            "max_cpu_threads_low_memory",
            errors
        );
        yh_config_infra::config_collect_gt_zero!(
            self.max_cpu_threads_throughput,
            s,
            "max_cpu_threads_throughput",
            errors
        );
        yh_config_infra::config_collect_range!(
            self.compression_max_level,
            s,
            "compression_max_level",
            0,
            9,
            errors
        );
        yh_config_infra::config_collect_gt_zero!(self.timeout_secs, s, "timeout_secs", errors);
        yh_config_infra::config_collect_gt_zero!(
            self.max_extract_size_gb,
            s,
            "max_extract_size_gb",
            errors
        );
        yh_config_infra::config_collect_gt_zero!(
            self.max_compress_items,
            s,
            "max_compress_items",
            errors
        );
        yh_config_infra::config_collect_gt_zero!(
            self.max_compress_total_size_mb,
            s,
            "max_compress_total_size_mb",
            errors
        );
        yh_config_infra::config_collect_gt_zero!(
            self.max_compress_output_size_mb,
            s,
            "max_compress_output_size_mb",
            errors
        );
        yh_config_infra::config_collect_gt_zero!(
            self.max_decompress_archive_size_mb,
            s,
            "max_decompress_archive_size_mb",
            errors
        );
        yh_config_infra::config_collect_not_empty_vec!(
            self.decompression_formats,
            s,
            "decompression_formats",
            errors
        );
        yh_config_infra::config_collect_bool!(
            self.enable_archive_browser,
            s,
            "enable_archive_browser",
            errors
        );

        // Cross-field consistency: if 7z format is enabled, exe_7zip_path must be non-empty.
        // Actual executability (PATH / file existence) is checked by config orchestrator preflight.
        let exe_7z = self.exe_7zip_path.as_deref().map(str::trim).unwrap_or("");
        let default_fmt = self
            .default_compression_format
            .as_deref()
            .map(str::trim)
            .unwrap_or("")
            .to_ascii_lowercase();
        let wants_7z_by_default = default_fmt == "7z";
        let allows_7z_extract = self
            .decompression_formats
            .as_ref()
            .map(|v| v.iter().any(|s| s.trim().eq_ignore_ascii_case("7z")))
            .unwrap_or(false);
        if (wants_7z_by_default || allows_7z_extract) && exe_7z.is_empty() {
            errors.push(format!(
                "[{}] file_compress.exe_7zip_path cannot be empty when 7z format is enabled",
                s
            ));
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ConfigDoc)]
pub struct VfsFileShareConfig {
    #[config(
        desc_zh = "启用文件分享功能，允许用户生成公开或私有分享链接",
        desc_en = "Enable file sharing feature, allows users to generate public or private share links",
        example = "true"
    )]
    pub enable: Option<bool>,
    #[config(
        desc_zh = "禁用搜索引擎索引，在分享页面添加robots.txt和meta标签",
        desc_en = "Disable search engine indexing, add robots.txt and meta tags on share pages",
        example = "true"
    )]
    pub isdisable_seacher_engine: Option<bool>,
    #[config(
        desc_zh = "启用用户直连分享，生成直接指向存储服务的下载链接",
        desc_en = "Enable user direct share, generate download links pointing directly to storage service",
        example = "true"
    )]
    pub enable_user_direct_share: Option<bool>,
    #[config(
        desc_zh = "回收站过期保留天数，过期后将被物理清理。0 表示永不自动清理。",
        desc_en = "Retention days for trash items. 0 means never auto-clean.",
        example = "30"
    )]
    pub trash_retention_days: Option<u32>,
}

impl VfsFileShareConfig {
    pub fn get_trash_retention_days(&self) -> u32 {
        yh_config_infra::config_require_clone!(
            self.trash_retention_days,
            "vfs_storage_hub",
            "file_share.trash_retention_days"
        )
    }
    pub fn is_isdisable_seacher_engine(&self) -> bool {
        yh_config_infra::config_require_clone!(
            self.isdisable_seacher_engine,
            "vfs_storage_hub",
            "file_share.isdisable_seacher_engine"
        )
    }
    pub fn is_enable_user_direct_share(&self) -> bool {
        yh_config_infra::config_require_clone!(
            self.enable_user_direct_share,
            "vfs_storage_hub",
            "file_share.enable_user_direct_share"
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ConfigDoc)]
pub struct VfsFileIndexConfig {
    #[config(
        desc_zh = "文件索引同步模式，可选值: 0=禁用，1=实时(推荐，有性能开销)，2=定时，3=手动，默认1",
        desc_en = "File index sync mode, options: 0=disabled, 1=realtime(recommended, performance overhead), 2=scheduled, 3=manual, default 1",
        example = "1"
    )]
    pub vfs_sync_index_mode: Option<i32>,
    #[config(
        desc_zh = "最大并发刷新目录数。低性能设备建议设为1-2，减少内存和IO压力",
        desc_en = "Maximum concurrent directory refresh count. Low-performance devices: recommend 1-2 to reduce memory and IO pressure",
        example = "5"
    )]
    pub max_concurrent_refresh: Option<u32>,
    #[config(
        desc_zh = "低内存档位（memory_allocator.profile=low_memory）时索引刷新并发数，必须显式配置",
        desc_en = "Concurrent index refresh count for low_memory hardware profile (memory_allocator.profile=low_memory), must be explicitly configured",
        example = "1"
    )]
    pub max_concurrent_refresh_low_memory: Option<u32>,
    #[config(
        desc_zh = "高吞吐档位（memory_allocator.profile=throughput）时索引刷新并发数，必须显式配置",
        desc_en = "Concurrent index refresh count for throughput hardware profile (memory_allocator.profile=throughput), must be explicitly configured",
        example = "8"
    )]
    pub max_concurrent_refresh_throughput: Option<u32>,
    #[config(
        desc_zh = "单次刷新操作扫描的最大文件数量，超出将分批处理",
        desc_en = "Maximum number of files scanned in single refresh operation, will process in batches beyond this limit",
        example = "10000"
    )]
    pub max_files_per_refresh: Option<u32>,
    #[config(
        desc_zh = "低内存档位（memory_allocator.profile=low_memory）时单次刷新分批大小，必须显式配置；值越小，内存占用越低但总耗时可能更高",
        desc_en = "Chunk size per index refresh in low_memory profile, must be explicitly configured; smaller values reduce memory usage but may increase total runtime.",
        example = "256"
    )]
    pub max_files_per_refresh_low_memory: Option<u32>,
    #[config(
        desc_zh = "高吞吐档位（memory_allocator.profile=throughput）时单次刷新分批大小，必须显式配置；值越大，更能利用高性能硬件吞吐",
        desc_en = "Chunk size per index refresh in throughput profile, must be explicitly configured; larger values make better use of high-performance hardware.",
        example = "2000"
    )]
    pub max_files_per_refresh_throughput: Option<u32>,
    #[config(
        desc_zh = "索引刷新操作超时时间（秒），范围: 60-3600，默认300，超时将终止任务，大目录建议增加",
        desc_en = "Index refresh operation timeout (seconds), range: 60-3600, default 300, task will be terminated when timeout, recommend increasing for large directories",
        example = "300"
    )]
    pub refresh_timeout: Option<u64>,
    #[config(
        desc_zh = "手动刷新触发文件名，创建或重命名为此名称的文件可触发目录刷新",
        desc_en = "Manual refresh trigger filename, creating or renaming file to this name triggers directory refresh",
        example = "._refresh_here_.txt"
    )]
    pub refresh_trigger_filename: Option<Arc<str>>,
    #[config(
        desc_zh = "启用分区索引，将文件索引按用户ID分区存储提升性能",
        desc_en = "Enable partitioned index, store file index partitioned by user ID to improve performance",
        example = "false"
    )]
    pub enable_partition: Option<bool>,
    #[config(
        desc_zh = "分区数量，索引表的分区数，应根据用户数量合理设置",
        desc_en = "Number of partitions, partition count of index tables, should be set reasonably based on user count",
        example = "10"
    )]
    pub partition_count: Option<u32>,
    #[config(
        desc_zh = "管理员一致性检查批量处理大小，每次检查的文件数量",
        desc_en = "Admin consistency check batch size, number of files checked per batch",
        example = "100"
    )]
    pub admin_consistency_check_batch_size: Option<u32>,
    #[config(
        desc_zh = "管理员一致性检查超时时间（秒），单次检查最长执行时间",
        desc_en = "Admin consistency check timeout (seconds), maximum execution time for single check",
        example = "300"
    )]
    pub admin_consistency_check_timeout: Option<u64>,
}

impl VfsFileIndexConfig {
    pub fn get_vfs_sync_index_mode(&self) -> i32 {
        yh_config_infra::config_require_clone!(
            self.vfs_sync_index_mode,
            "vfs_storage_hub",
            "file_index.vfs_sync_index_mode"
        )
    }
    pub fn get_max_concurrent_refresh(&self) -> u32 {
        yh_config_infra::config_require_clone!(
            self.max_concurrent_refresh,
            "vfs_storage_hub",
            "file_index.max_concurrent_refresh"
        )
    }
    pub fn get_effective_max_concurrent_refresh(&self) -> u32 {
        match require_allocator_profile() {
            HardwareProfile::LowMemory => {
                yh_config_infra::config_require_clone!(
                    self.max_concurrent_refresh_low_memory,
                    "vfs_storage_hub",
                    "file_index.max_concurrent_refresh_low_memory"
                )
            }
            HardwareProfile::Throughput => {
                yh_config_infra::config_require_clone!(
                    self.max_concurrent_refresh_throughput,
                    "vfs_storage_hub",
                    "file_index.max_concurrent_refresh_throughput"
                )
            }
            HardwareProfile::Balanced => self.get_max_concurrent_refresh(),
        }
    }
    pub fn get_refresh_timeout(&self) -> u64 {
        yh_config_infra::config_require_clone!(
            self.refresh_timeout,
            "vfs_storage_hub",
            "file_index.refresh_timeout"
        )
    }
    pub fn get_max_files_per_refresh(&self) -> u32 {
        yh_config_infra::config_require_clone!(
            self.max_files_per_refresh,
            "vfs_storage_hub",
            "file_index.max_files_per_refresh"
        )
    }
    pub fn get_effective_max_files_per_refresh(&self) -> u32 {
        match require_allocator_profile() {
            HardwareProfile::LowMemory => {
                yh_config_infra::config_require_clone!(
                    self.max_files_per_refresh_low_memory,
                    "vfs_storage_hub",
                    "file_index.max_files_per_refresh_low_memory"
                )
            }
            HardwareProfile::Throughput => {
                yh_config_infra::config_require_clone!(
                    self.max_files_per_refresh_throughput,
                    "vfs_storage_hub",
                    "file_index.max_files_per_refresh_throughput"
                )
            }
            HardwareProfile::Balanced => self.get_max_files_per_refresh(),
        }
    }
    pub fn get_refresh_trigger_filename(&self) -> &str {
        yh_config_infra::config_require_str!(
            self.refresh_trigger_filename,
            "vfs_storage_hub",
            "file_index.refresh_trigger_filename"
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ConfigDoc)]
pub struct VfsTempFileConfig {
    #[config(
        desc_zh = "临时文件存储目录，用于存储上传分片和临时处理文件",
        desc_en = "Temporary file storage directory for storing upload chunks and temporary processing files",
        example = "{RUNTIMEDIR}/tmp/vfs"
    )]
    pub dir: Option<Arc<str>>,
    #[config(
        desc_zh = "临时文件最大存活时间（秒），超时后将被自动清理",
        desc_en = "Maximum survival time (seconds) of temporary files, will be automatically cleaned after expiration",
        example = "3600"
    )]
    pub max_age: Option<u64>,
}

impl VfsTempFileConfig {
    pub fn get_dir(&self) -> &str {
        yh_config_infra::config_require_str!(self.dir, "vfs_storage_hub", "temp_file.dir")
    }
    pub fn get_max_age(&self) -> u64 {
        yh_config_infra::config_require_clone!(self.max_age, "vfs_storage_hub", "temp_file.max_age")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ConfigDoc)]
pub struct VfsReadCacheConfig {
    #[config(
        desc_zh = "是否启用本地读缓存。默认关闭；关闭时仍需显式填写所有字段以保持配置结构完整。",
        desc_en = "Enable local read cache. Disabled by default; all fields still need to be explicitly present to keep config structure complete.",
        example = "false"
    )]
    pub enable: Option<bool>,
    #[config(
        desc_zh = "读缓存后端类型，可选: memory|local_dir。memory 使用进程内存，local_dir 使用本地目录。默认示例保持关闭。",
        desc_en = "Read cache backend type. Options: memory|local_dir. memory uses process memory, local_dir uses a local directory. Example remains disabled by default.",
        example = "memory"
    )]
    pub backend: Option<Arc<str>>,
    #[config(
        desc_zh = "读缓存本地目录路径。backend=local_dir 时缓存数据会保存在这里；backend=memory 时该字段仍需显式配置以保持结构一致。",
        desc_en = "Local directory path for read cache. Used when backend=local_dir; still required explicitly when backend=memory to keep config structure consistent.",
        example = "{RUNTIMEDIR}/cache/vfs-read"
    )]
    pub local_dir: Option<Arc<str>>,
    #[config(
        desc_zh = "读缓存容量上限（字节）。达到上限后将逐出较旧条目，不会影响源存储数据。",
        desc_en = "Read cache capacity limit in bytes. Older entries are evicted when the limit is reached without affecting origin data.",
        example = "268435456"
    )]
    pub capacity_bytes: Option<u64>,
    #[config(
        desc_zh = "单个允许进入读缓存的最大文件大小（字节）。超过此值直接绕过缓存。",
        desc_en = "Maximum file size in bytes eligible for read cache. Files larger than this bypass cache directly.",
        example = "1048576"
    )]
    pub max_file_size_bytes: Option<u64>,
    #[config(
        desc_zh = "是否允许缩略图路径进入读缓存。默认 false，避免缩略图生成与内容缓存相互放大。",
        desc_en = "Allow thumbnail paths to enter read cache. Default false to avoid cache amplification between thumbnail generation and content caching.",
        example = "false"
    )]
    pub cache_thumbnail_paths: Option<bool>,
    #[config(
        desc_zh = "读缓存跳过的文件扩展名列表（不带点号，大小写不敏感）。例如 [\"md\", \"markdown\"]。",
        desc_en = "File extensions skipped by read cache (without leading dot, case-insensitive), for example [\"md\", \"markdown\"].",
        example = "[]"
    )]
    pub skip_extensions: Option<Vec<Arc<str>>>,
    #[config(
        desc_zh = "读缓存 TTL（秒）。超过后条目自动失效并回源读取。",
        desc_en = "Read cache TTL in seconds. Entries expire after this and reads fall back to origin.",
        example = "1800"
    )]
    pub ttl_secs: Option<u64>,
}

impl VfsReadCacheConfig {
    pub fn is_enabled(&self) -> bool {
        yh_config_infra::config_require_clone!(self.enable, "vfs_storage_hub", "read_cache.enable")
    }
    pub fn get_backend(&self) -> &str {
        yh_config_infra::config_require_str!(self.backend, "vfs_storage_hub", "read_cache.backend")
    }
    pub fn get_local_dir(&self) -> &str {
        yh_config_infra::config_require_str!(
            self.local_dir,
            "vfs_storage_hub",
            "read_cache.local_dir"
        )
    }
    pub fn get_capacity_bytes(&self) -> u64 {
        yh_config_infra::config_require_clone!(
            self.capacity_bytes,
            "vfs_storage_hub",
            "read_cache.capacity_bytes"
        )
    }
    pub fn get_max_file_size_bytes(&self) -> u64 {
        yh_config_infra::config_require_clone!(
            self.max_file_size_bytes,
            "vfs_storage_hub",
            "read_cache.max_file_size_bytes"
        )
    }
    pub fn is_cache_thumbnail_paths(&self) -> bool {
        yh_config_infra::config_require_clone!(
            self.cache_thumbnail_paths,
            "vfs_storage_hub",
            "read_cache.cache_thumbnail_paths"
        )
    }
    pub fn get_skip_extensions(&self) -> Vec<Arc<str>> {
        yh_config_infra::config_require_clone!(
            self.skip_extensions,
            "vfs_storage_hub",
            "read_cache.skip_extensions"
        )
    }
    pub fn get_ttl_secs(&self) -> u64 {
        yh_config_infra::config_require_clone!(
            self.ttl_secs,
            "vfs_storage_hub",
            "read_cache.ttl_secs"
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ConfigDoc)]
pub struct VfsWriteCacheConfig {
    #[config(
        desc_zh = "危险选项。是否启用小文件本地写缓存。启用后，小文件可能先写入本地缓存并立即返回成功，再后台异步写入真实存储；进程崩溃、系统重启、断电、本地缓存损坏或后台回放失败时，已确认成功的文件仍可能丢失。默认关闭。",
        desc_en = "Dangerous option. Enable local write cache for small files. When enabled, small files may be acknowledged after landing in local cache and flushed to origin asynchronously later; process crash, reboot, power loss, local cache corruption, or replay failure can still lose already acknowledged files. Disabled by default.",
        example = "false"
    )]
    pub enable: Option<bool>,
    #[config(
        desc_zh = "写缓存后端类型，可选: memory|local_dir。memory 延迟最低但风险最高；local_dir 使用本地目录，风险相对较低。",
        desc_en = "Write cache backend type. Options: memory|local_dir. memory has the lowest latency but highest risk; local_dir uses a local directory and is relatively safer.",
        example = "memory"
    )]
    pub backend: Option<Arc<str>>,
    #[config(
        desc_zh = "写缓存本地目录路径。backend=local_dir 时待刷盘小文件会先落在这里；backend=memory 时该字段仍需显式配置以保持结构一致。",
        desc_en = "Local directory path for write cache. Pending small files are first written here when backend=local_dir; still required explicitly when backend=memory to keep config structure consistent.",
        example = "{RUNTIMEDIR}/cache/vfs-write"
    )]
    pub local_dir: Option<Arc<str>>,
    #[config(
        desc_zh = "写缓存容量上限（字节）。这是准入上限而不是逐出上限；未刷盘数据不会因为容量达到上限而被自动丢弃。",
        desc_en = "Write cache capacity limit in bytes. This is an admission limit, not an eviction limit; unflushed data is never dropped automatically just because the limit is reached.",
        example = "268435456"
    )]
    pub capacity_bytes: Option<u64>,
    #[config(
        desc_zh = "单个允许进入写缓存的最大文件大小（字节）。超过此值将绕过写缓存并走普通同步写入。",
        desc_en = "Maximum file size in bytes eligible for write cache. Files larger than this bypass write cache and use the normal synchronous write path.",
        example = "262144"
    )]
    pub max_file_size_bytes: Option<u64>,
    #[config(
        desc_zh = "是否允许缩略图路径进入写缓存。默认 false，避免缩略图写入把危险写缓存容量耗尽。",
        desc_en = "Allow thumbnail paths to enter write cache. Default false to avoid thumbnail writes exhausting the risky write-back cache budget.",
        example = "false"
    )]
    pub cache_thumbnail_paths: Option<bool>,
    #[config(
        desc_zh = "写缓存跳过的文件扩展名列表（不带点号，大小写不敏感）。例如 [\"md\", \"markdown\"] 可让 Markdown 文档始终走同步写入。",
        desc_en = "File extensions skipped by write cache (without leading dot, case-insensitive). For example [\"md\", \"markdown\"] keeps Markdown documents on synchronous writes.",
        example = "[]"
    )]
    pub skip_extensions: Option<Vec<Arc<str>>>,
    #[config(
        desc_zh = "后台刷盘并发数。值越大，对远端存储施加的并发压力越高。",
        desc_en = "Background flush concurrency. Higher values place more concurrent pressure on the remote storage.",
        example = "2"
    )]
    pub flush_concurrency: Option<usize>,
    #[config(
        desc_zh = "后台刷盘轮询间隔（毫秒）。数值越小，待写文件越快尝试刷入真实存储。",
        desc_en = "Background flush polling interval in milliseconds. Smaller values attempt to flush pending files to origin faster.",
        example = "20"
    )]
    pub flush_interval_ms: Option<u64>,
    #[config(
        desc_zh = "异常时间上限（秒）。超过该时间仍未成功刷入真实存储的条目会被提升为异常条目；若 backend=memory，将先落盘到异常目录再从内存逐出，并在 yh-journal-log 留痕。",
        desc_en = "Abnormal time limit in seconds. Entries not flushed successfully before this deadline are promoted to abnormal items; when backend=memory, they are first spilled into the abnormal directory and then evicted from memory, with an audit record written to yh-journal-log.",
        example = "300"
    )]
    pub flush_deadline_secs: Option<u64>,
    #[config(
        desc_zh = "异常本地落盘目录。memory 后端超时后会把未刷盘数据落到这里；local_dir 后端也会把异常条目转移/隔离到这里。",
        desc_en = "Local spill directory for abnormal entries. Unflushed data from memory backend is spilled here after deadline; local_dir backend also moves or isolates abnormal entries here.",
        example = "{RUNTIMEDIR}/cache/vfs-write-abnormal"
    )]
    pub abnormal_spill_dir: Option<Arc<str>>,
}

impl VfsWriteCacheConfig {
    pub fn is_enabled(&self) -> bool {
        yh_config_infra::config_require_clone!(self.enable, "vfs_storage_hub", "write_cache.enable")
    }
    pub fn get_backend(&self) -> &str {
        yh_config_infra::config_require_str!(self.backend, "vfs_storage_hub", "write_cache.backend")
    }
    pub fn get_local_dir(&self) -> &str {
        yh_config_infra::config_require_str!(
            self.local_dir,
            "vfs_storage_hub",
            "write_cache.local_dir"
        )
    }
    pub fn get_capacity_bytes(&self) -> u64 {
        yh_config_infra::config_require_clone!(
            self.capacity_bytes,
            "vfs_storage_hub",
            "write_cache.capacity_bytes"
        )
    }
    pub fn get_max_file_size_bytes(&self) -> u64 {
        yh_config_infra::config_require_clone!(
            self.max_file_size_bytes,
            "vfs_storage_hub",
            "write_cache.max_file_size_bytes"
        )
    }
    pub fn is_cache_thumbnail_paths(&self) -> bool {
        yh_config_infra::config_require_clone!(
            self.cache_thumbnail_paths,
            "vfs_storage_hub",
            "write_cache.cache_thumbnail_paths"
        )
    }
    pub fn get_skip_extensions(&self) -> Vec<Arc<str>> {
        yh_config_infra::config_require_clone!(
            self.skip_extensions,
            "vfs_storage_hub",
            "write_cache.skip_extensions"
        )
    }
    pub fn get_flush_concurrency(&self) -> usize {
        yh_config_infra::config_require_clone!(
            self.flush_concurrency,
            "vfs_storage_hub",
            "write_cache.flush_concurrency"
        )
    }
    pub fn get_flush_interval_ms(&self) -> u64 {
        yh_config_infra::config_require_clone!(
            self.flush_interval_ms,
            "vfs_storage_hub",
            "write_cache.flush_interval_ms"
        )
    }
    pub fn get_flush_deadline_secs(&self) -> u64 {
        yh_config_infra::config_require_clone!(
            self.flush_deadline_secs,
            "vfs_storage_hub",
            "write_cache.flush_deadline_secs"
        )
    }
    pub fn get_abnormal_spill_dir(&self) -> &str {
        yh_config_infra::config_require_str!(
            self.abnormal_spill_dir,
            "vfs_storage_hub",
            "write_cache.abnormal_spill_dir"
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ConfigDoc)]
pub struct VfsBatchOperationConfig {
    #[config(
        desc_zh = "批量操作任务超时时间（秒），如复制/移动大量文件",
        desc_en = "Batch operation task timeout (seconds), such as copying/moving large number of files",
        example = "86400"
    )]
    pub timeout_secs: Option<u64>,
    #[config(
        desc_zh = "最大并行批处理任务数。低性能设备建议设为1，避免内存压力",
        desc_en = "Maximum concurrent batch operation tasks. Low-performance devices: recommend 1 to avoid memory pressure",
        example = "3"
    )]
    pub max_concurrent_tasks: Option<usize>,
    #[config(
        desc_zh = "低内存档位（memory_allocator.profile=low_memory）时批量任务并发数，必须显式配置",
        desc_en = "Concurrent batch operation tasks for low_memory hardware profile (memory_allocator.profile=low_memory), must be explicitly configured",
        example = "1"
    )]
    pub max_concurrent_tasks_low_memory: Option<usize>,
    #[config(
        desc_zh = "高吞吐档位（memory_allocator.profile=throughput）时批量任务并发数，必须显式配置",
        desc_en = "Concurrent batch operation tasks for throughput hardware profile (memory_allocator.profile=throughput), must be explicitly configured",
        example = "6"
    )]
    pub max_concurrent_tasks_throughput: Option<usize>,
    #[config(
        desc_zh = "WAL（写前日志）记录最小写入大小（字节），仅对普通文件写入生效；小于该值的写入可跳过 WAL，目录/删除/移动等控制操作不受此阈值影响",
        desc_en = "Minimum write size in bytes for WAL logging. Applies only to regular file writes; writes smaller than this threshold may skip WAL, while delete/move/create-dir control operations are still logged",
        example = "1048576"
    )]
    pub wal_min_size_bytes: Option<u64>,
    #[config(
        desc_zh = "临时文件路径是否跳过WAL记录，提升性能但可能降低数据一致性",
        desc_en = "Whether to skip WAL logging for temporary file paths, improves performance but may lose data",
        example = "true"
    )]
    pub wal_skip_temp_path: Option<bool>,
}

impl VfsBatchOperationConfig {
    pub fn get_timeout_secs(&self) -> u64 {
        yh_config_infra::config_require_clone!(
            self.timeout_secs,
            "vfs_storage_hub",
            "batch_operation.timeout_secs"
        )
    }
    pub fn get_max_concurrent_tasks(&self) -> usize {
        yh_config_infra::config_require_clone!(
            self.max_concurrent_tasks,
            "vfs_storage_hub",
            "batch_operation.max_concurrent_tasks"
        )
    }
    pub fn get_effective_max_concurrent_tasks(&self) -> usize {
        match require_allocator_profile() {
            HardwareProfile::LowMemory => {
                yh_config_infra::config_require_clone!(
                    self.max_concurrent_tasks_low_memory,
                    "vfs_storage_hub",
                    "batch_operation.max_concurrent_tasks_low_memory"
                )
            }
            HardwareProfile::Throughput => {
                yh_config_infra::config_require_clone!(
                    self.max_concurrent_tasks_throughput,
                    "vfs_storage_hub",
                    "batch_operation.max_concurrent_tasks_throughput"
                )
            }
            HardwareProfile::Balanced => self.get_max_concurrent_tasks(),
        }
    }
    pub fn is_wal_skip_temp_path(&self) -> bool {
        yh_config_infra::config_require_clone!(
            self.wal_skip_temp_path,
            "vfs_storage_hub",
            "batch_operation.wal_skip_temp_path"
        )
    }
    pub fn get_wal_min_size_bytes(&self) -> u64 {
        yh_config_infra::config_require_clone!(
            self.wal_min_size_bytes,
            "vfs_storage_hub",
            "batch_operation.wal_min_size_bytes"
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ConfigDoc)]
pub struct VfsMaintenanceConfig {
    #[config(
        desc_zh = "启用S3分片上传清理任务，清理未完成的分片上传",
        desc_en = "Enable S3 multipart upload cleanup task to clean up incomplete multipart uploads",
        example = "true"
    )]
    pub s3_multipart_cleanup_enabled: Option<bool>,
    #[config(
        desc_zh = "S3分片上传宽限期（秒），超过此时间未完成的分片将被清理",
        desc_en = "S3 multipart upload grace period (seconds), incomplete parts beyond this time will be cleaned",
        example = "3600"
    )]
    pub s3_multipart_grace_period_secs: Option<u64>,
}

impl VfsMaintenanceConfig {
    pub fn is_s3_multipart_cleanup_enabled(&self) -> bool {
        yh_config_infra::config_require_clone!(
            self.s3_multipart_cleanup_enabled,
            "vfs_storage_hub",
            "maintenance.s3_multipart_cleanup_enabled"
        )
    }
    pub fn get_s3_multipart_grace_period_secs(&self) -> u64 {
        yh_config_infra::config_require_clone!(
            self.s3_multipart_grace_period_secs,
            "vfs_storage_hub",
            "maintenance.s3_multipart_grace_period_secs"
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ConfigDoc)]
pub struct VfsThumbnailToolConfig {
    #[config(
        desc_zh = "libvips 可执行命令或绝对路径。默认建议使用命令名 \"vips\"（依赖系统 PATH）；Windows 常见路径：C:/Program Files/vips-dev/bin/vips.exe，Linux 常见路径：/usr/bin/vips",
        desc_en = "libvips executable command or absolute path. Default recommended value is command name \"vips\" (resolved via system PATH); common Windows path: C:/Program Files/vips-dev/bin/vips.exe, common Linux path: /usr/bin/vips",
        example = "vips"
    )]
    pub vips_path: Option<String>,
    #[config(
        desc_zh = "ImageMagick 可执行命令或绝对路径。默认建议使用命令名 \"convert\"（依赖系统 PATH）；Windows 常见路径：C:/Program Files/ImageMagick-7.*/magick.exe（新版本常用 magick 子命令），Linux 常见路径：/usr/bin/convert",
        desc_en = "ImageMagick executable command or absolute path. Default recommended value is command name \"convert\" (resolved via system PATH); common Windows path: C:/Program Files/ImageMagick-7.*/magick.exe (new versions commonly use magick subcommand), common Linux path: /usr/bin/convert",
        example = "convert"
    )]
    pub imagemagick_path: Option<String>,
    #[config(
        desc_zh = "FFmpeg 可执行命令或绝对路径。默认建议使用命令名 \"ffmpeg\"（依赖系统 PATH）；Windows 常见路径：C:/ffmpeg/bin/ffmpeg.exe，Linux 常见路径：/usr/bin/ffmpeg",
        desc_en = "FFmpeg executable command or absolute path. Default recommended value is command name \"ffmpeg\" (resolved via system PATH); common Windows path: C:/ffmpeg/bin/ffmpeg.exe, common Linux path: /usr/bin/ffmpeg",
        example = "ffmpeg"
    )]
    pub ffmpeg_path: Option<String>,
    #[config(
        desc_zh = "LibreOffice 可执行命令或绝对路径（用于 Office 文档缩略图）。默认建议使用命令名 \"soffice\"（依赖系统 PATH）；Windows 常见路径：C:/Program Files/LibreOffice/program/soffice.exe，Linux 常见路径：/usr/bin/soffice",
        desc_en = "LibreOffice executable command or absolute path (for Office thumbnails). Default recommended value is command name \"soffice\" (resolved via system PATH); common Windows path: C:/Program Files/LibreOffice/program/soffice.exe, common Linux path: /usr/bin/soffice",
        example = "soffice"
    )]
    pub libreoffice_path: Option<String>,
}

impl VfsThumbnailToolConfig {
    pub fn get_vips_path(&self) -> &str {
        yh_config_infra::config_require_str!(
            self.vips_path,
            "vfs_storage_hub",
            "thumbnail.tools.vips_path"
        )
    }
    pub fn get_ffmpeg_path(&self) -> &str {
        yh_config_infra::config_require_str!(
            self.ffmpeg_path,
            "vfs_storage_hub",
            "thumbnail.tools.ffmpeg_path"
        )
    }
    pub fn get_libreoffice_path(&self) -> &str {
        yh_config_infra::config_require_str!(
            self.libreoffice_path,
            "vfs_storage_hub",
            "thumbnail.tools.libreoffice_path"
        )
    }
    pub fn get_imagemagick_path(&self) -> &str {
        yh_config_infra::config_require_str!(
            self.imagemagick_path,
            "vfs_storage_hub",
            "thumbnail.tools.imagemagick_path"
        )
    }

    pub fn to_runtime_config(&self) -> crate::business::services::ThumbnailRuntimeToolConfig {
        crate::business::services::ThumbnailRuntimeToolConfig {
            vips_path: self.get_vips_path().to_string(),
            imagemagick_path: self.get_imagemagick_path().to_string(),
            ffmpeg_path: self.get_ffmpeg_path().to_string(),
            libreoffice_path: self.get_libreoffice_path().to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ConfigDoc)]
pub struct VfsThumbnailImageConfig {
    #[config(
        desc_zh = "是否启用图片缩略图",
        desc_en = "Enable image thumbnails",
        example = "true"
    )]
    pub enabled: Option<bool>,
    #[config(
        desc_zh = "小于此大小（MB）的图片直接返回原图，不再单独生成缩略图",
        desc_en = "Return the original image directly when it is smaller than this size (MB)",
        example = "1"
    )]
    pub small_skip_mb: Option<u64>,
    #[config(
        desc_zh = "大于此大小（MB）的图片跳过缩略图生成",
        desc_en = "Skip image thumbnail generation for files larger than this size (MB)",
        example = "100"
    )]
    pub max_size_mb: Option<u64>,
    #[config(
        desc_zh = "当 backend=external 时，ImageMagick 允许处理的最大文件大小（MB，0表示不限制）",
        desc_en = "Max file size allowed for ImageMagick when backend=external (MB, 0 means no limit)",
        example = "20"
    )]
    pub imagemagick_max_mb: Option<u64>,
    #[config(
        desc_zh = "生成超时时间（秒）",
        desc_en = "Generation timeout (seconds)",
        example = "10"
    )]
    pub timeout_secs: Option<u64>,
    #[config(
        desc_zh = "图片缩略图后端，可选 builtin|external；builtin 为纯 Rust 内置实现，external 为 libvips/ImageMagick",
        desc_en = "Image thumbnail backend, supported values: builtin|external. builtin uses the pure Rust pipeline, external uses libvips/ImageMagick",
        example = "builtin"
    )]
    pub backend: Option<String>,
}

impl VfsThumbnailImageConfig {
    pub fn is_enabled(&self) -> bool {
        yh_config_infra::config_require_clone!(
            self.enabled,
            "vfs_storage_hub",
            "thumbnail.image.enabled"
        )
    }
    pub fn get_max_size_mb(&self) -> u64 {
        yh_config_infra::config_require_clone!(
            self.max_size_mb,
            "vfs_storage_hub",
            "thumbnail.image.max_size_mb"
        )
    }
    pub fn get_imagemagick_max_mb(&self) -> u64 {
        yh_config_infra::config_require_clone!(
            self.imagemagick_max_mb,
            "vfs_storage_hub",
            "thumbnail.image.imagemagick_max_mb"
        )
    }
    pub fn get_timeout_secs(&self) -> u64 {
        yh_config_infra::config_require_clone!(
            self.timeout_secs,
            "vfs_storage_hub",
            "thumbnail.image.timeout_secs"
        )
    }
    pub fn get_small_skip_mb(&self) -> u64 {
        yh_config_infra::config_require_clone!(
            self.small_skip_mb,
            "vfs_storage_hub",
            "thumbnail.image.small_skip_mb"
        )
    }
    pub fn get_backend(&self) -> &str {
        yh_config_infra::config_require_str!(
            self.backend,
            "vfs_storage_hub",
            "thumbnail.image.backend"
        )
    }

    pub fn to_runtime_config(&self) -> crate::business::services::ThumbnailRuntimeImageConfig {
        crate::business::services::ThumbnailRuntimeImageConfig {
            enabled: self.is_enabled(),
            small_skip_mb: self.get_small_skip_mb(),
            max_size_mb: self.get_max_size_mb(),
            imagemagick_max_mb: self.get_imagemagick_max_mb(),
            timeout_secs: self.get_timeout_secs(),
            backend: self.get_backend().to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ConfigDoc)]
pub struct VfsThumbnailTypeConfig {
    #[config(
        desc_zh = "是否启用此类文件的缩略图",
        desc_en = "Enable thumbnails for this type",
        example = "true"
    )]
    pub enabled: Option<bool>,
    #[config(
        desc_zh = "小于此大小（MB）的文件跳过缩略图生成",
        desc_en = "Skip thumbnail generation for files smaller than this (MB)",
        example = "1"
    )]
    pub small_skip_mb: Option<u64>,
    #[config(
        desc_zh = "大于此大小（MB）的文件跳过缩略图生成",
        desc_en = "Skip thumbnail generation for files larger than this (MB)",
        example = "100"
    )]
    pub max_size_mb: Option<u64>,
    #[config(
        desc_zh = "ImageMagick最大允许处理的文件大小（MB，0表示不限制）",
        desc_en = "Max file size allowed for ImageMagick (MB, 0 means no limit)",
        example = "20"
    )]
    pub imagemagick_max_mb: Option<u64>,
    #[config(
        desc_zh = "生成超时时间（秒）",
        desc_en = "Generation timeout (seconds)",
        example = "10"
    )]
    pub timeout_secs: Option<u64>,
    #[config(
        desc_zh = "视频截取时间（秒）",
        desc_en = "Video seek position (seconds)",
        example = "3"
    )]
    pub seek_seconds: Option<u64>,
    #[config(
        desc_zh = "视频截取比例（0.0-1.0，优先于seek_seconds）",
        desc_en = "Video seek ratio (0.0-1.0, overrides seek_seconds)",
        example = "0.3"
    )]
    pub seek_ratio: Option<f64>,
    #[config(
        desc_zh = "文本预览最大提取字符数",
        desc_en = "Max characters to extract for text preview",
        example = "1000"
    )]
    pub max_chars: Option<u64>,
}

impl VfsThumbnailTypeConfig {
    pub fn is_enabled(&self) -> bool {
        yh_config_infra::config_require_clone!(
            self.enabled,
            "vfs_storage_hub",
            "thumbnail.type.enabled"
        )
    }
    pub fn get_max_size_mb(&self) -> u64 {
        yh_config_infra::config_require_clone!(
            self.max_size_mb,
            "vfs_storage_hub",
            "thumbnail.type.max_size_mb"
        )
    }
    pub fn get_imagemagick_max_mb(&self) -> u64 {
        yh_config_infra::config_require_clone!(
            self.imagemagick_max_mb,
            "vfs_storage_hub",
            "thumbnail.type.imagemagick_max_mb"
        )
    }
    pub fn get_timeout_secs(&self) -> u64 {
        yh_config_infra::config_require_clone!(
            self.timeout_secs,
            "vfs_storage_hub",
            "thumbnail.type.timeout_secs"
        )
    }
    pub fn get_small_skip_mb(&self) -> u64 {
        yh_config_infra::config_require_clone!(
            self.small_skip_mb,
            "vfs_storage_hub",
            "thumbnail.type.small_skip_mb"
        )
    }
    pub fn get_seek_ratio(&self) -> f32 {
        yh_config_infra::config_require_clone!(
            self.seek_ratio,
            "vfs_storage_hub",
            "thumbnail.type.seek_ratio"
        ) as f32
    }
    pub fn get_seek_seconds(&self) -> u64 {
        yh_config_infra::config_require_clone!(
            self.seek_seconds,
            "vfs_storage_hub",
            "thumbnail.type.seek_seconds"
        )
    }
    pub fn get_max_chars(&self) -> u64 {
        yh_config_infra::config_require_clone!(
            self.max_chars,
            "vfs_storage_hub",
            "thumbnail.type.max_chars"
        )
    }

    pub fn to_runtime_config(&self) -> crate::business::services::ThumbnailRuntimeTypeConfig {
        crate::business::services::ThumbnailRuntimeTypeConfig {
            enabled: self.is_enabled(),
            small_skip_mb: self.get_small_skip_mb(),
            max_size_mb: self.get_max_size_mb(),
            imagemagick_max_mb: self.get_imagemagick_max_mb(),
            timeout_secs: self.get_timeout_secs(),
            seek_seconds: self.seek_seconds,
            seek_ratio: self.seek_ratio,
            max_chars: self.max_chars,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ConfigDoc)]
pub struct VfsThumbnailConfig {
    #[config(
        desc_zh = "是否启用缩略图生成服务",
        desc_en = "Enable thumbnail generation service",
        example = "true"
    )]
    pub enabled: Option<bool>,
    #[config(
        desc_zh = "缓存模式: dir(目录内 .thumbs)|global(集中缓存目录)|none(不缓存)",
        desc_en = "Cache mode",
        example = "dir"
    )]
    pub cache_mode: Option<String>,
    #[config(
        desc_zh = "缩略图缓存目录路径",
        desc_en = "Thumbnail cache directory path",
        example = "{RUNTIMEDIR}/cache/thumbnails"
    )]
    pub cache_dir: Option<String>,
    #[config(
        desc_zh = "缩略图尺寸（像素）",
        desc_en = "Thumbnail size (pixels)",
        example = "256"
    )]
    pub thumb_size_px: Option<u32>,
    #[config(
        desc_zh = "输出格式: jpg|png|webp",
        desc_en = "Output format",
        example = "jpg"
    )]
    pub thumb_format: Option<String>,
    #[config(
        desc_zh = "输出质量（1-100）",
        desc_en = "Output quality (1-100)",
        example = "85"
    )]
    pub thumb_quality: Option<u8>,
    #[config(
        desc_zh = "外部工具路径配置",
        desc_en = "External tools path configuration"
    )]
    pub tools: Option<VfsThumbnailToolConfig>,
    #[config(desc_zh = "图片缩略图配置", desc_en = "Image thumbnail configuration")]
    pub image: Option<VfsThumbnailImageConfig>,
    #[config(desc_zh = "视频缩略图配置", desc_en = "Video thumbnail configuration")]
    pub video: Option<VfsThumbnailTypeConfig>,
    #[config(desc_zh = "PDF缩略图配置", desc_en = "PDF thumbnail configuration")]
    pub pdf: Option<VfsThumbnailTypeConfig>,
    #[config(
        desc_zh = "Office文档缩略图配置",
        desc_en = "Office thumbnail configuration"
    )]
    pub office: Option<VfsThumbnailTypeConfig>,
    #[config(
        desc_zh = "文本文件缩略图配置",
        desc_en = "Text thumbnail configuration"
    )]
    pub text: Option<VfsThumbnailTypeConfig>,
}

impl VfsThumbnailConfig {
    pub fn is_enabled(&self) -> bool {
        yh_config_infra::config_require_clone!(self.enabled, "vfs_storage_hub", "thumbnail.enabled")
    }
    pub fn get_tools(&self) -> &VfsThumbnailToolConfig {
        yh_config_infra::config_require_ref!(self.tools, "vfs_storage_hub", "thumbnail.tools")
    }
    pub fn get_image(&self) -> &VfsThumbnailImageConfig {
        yh_config_infra::config_require_ref!(self.image, "vfs_storage_hub", "thumbnail.image")
    }
    pub fn get_video(&self) -> &VfsThumbnailTypeConfig {
        yh_config_infra::config_require_ref!(self.video, "vfs_storage_hub", "thumbnail.video")
    }
    pub fn get_pdf(&self) -> &VfsThumbnailTypeConfig {
        yh_config_infra::config_require_ref!(self.pdf, "vfs_storage_hub", "thumbnail.pdf")
    }
    pub fn get_office(&self) -> &VfsThumbnailTypeConfig {
        yh_config_infra::config_require_ref!(self.office, "vfs_storage_hub", "thumbnail.office")
    }
    pub fn get_text(&self) -> &VfsThumbnailTypeConfig {
        yh_config_infra::config_require_ref!(self.text, "vfs_storage_hub", "thumbnail.text")
    }
    pub fn get_thumb_size_px(&self) -> u32 {
        yh_config_infra::config_require_clone!(
            self.thumb_size_px,
            "vfs_storage_hub",
            "thumbnail.thumb_size_px"
        )
    }
    pub fn get_thumb_quality(&self) -> u8 {
        yh_config_infra::config_require_clone!(
            self.thumb_quality,
            "vfs_storage_hub",
            "thumbnail.thumb_quality"
        )
    }
    pub fn get_cache_dir(&self) -> &str {
        yh_config_infra::config_require_str!(
            self.cache_dir,
            "vfs_storage_hub",
            "thumbnail.cache_dir"
        )
    }
    pub fn get_cache_mode(&self) -> &str {
        yh_config_infra::config_require_str!(
            self.cache_mode,
            "vfs_storage_hub",
            "thumbnail.cache_mode"
        )
    }
    pub fn get_thumb_format(&self) -> &str {
        yh_config_infra::config_require_str!(
            self.thumb_format,
            "vfs_storage_hub",
            "thumbnail.thumb_format"
        )
    }

    pub fn to_runtime_config(&self) -> crate::business::services::ThumbnailRuntimeConfig {
        crate::business::services::ThumbnailRuntimeConfig {
            enabled: self.is_enabled(),
            cache_mode: self.get_cache_mode().to_string(),
            cache_dir: self.get_cache_dir().to_string(),
            thumb_size_px: self.get_thumb_size_px(),
            thumb_format: self.get_thumb_format().to_string(),
            thumb_quality: self.get_thumb_quality(),
            tools: self.get_tools().to_runtime_config(),
            image: self.get_image().to_runtime_config(),
            video: self.get_video().to_runtime_config(),
            pdf: self.get_pdf().to_runtime_config(),
            office: self.get_office().to_runtime_config(),
            text: self.get_text().to_runtime_config(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ConfigDoc)]
pub struct VfsStorageHubConfig {
    #[config(
        desc_zh = "启用WebDAV协议服务。低性能设备建议设为false，仅保留HTTP API",
        desc_en = "Enable WebDAV protocol service. Low-performance devices: recommend false, keep only HTTP API",
        example = "true"
    )]
    pub enable_webdav: Option<bool>,
    #[config(
        desc_zh = "启用SFTP协议服务。低性能设备建议设为false，仅保留HTTP API",
        desc_en = "Enable SFTP protocol service. Low-performance devices: recommend false, keep only HTTP API",
        example = "true"
    )]
    pub enable_sftp: Option<bool>,
    #[config(
        desc_zh = "启用FTP协议服务。低性能设备建议设为false，仅保留HTTP API",
        desc_en = "Enable FTP protocol service. Low-performance devices: recommend false, keep only HTTP API",
        example = "true"
    )]
    pub enable_ftp: Option<bool>,
    #[config(
        desc_zh = "启用S3兼容服务。低性能设备建议设为false，仅保留HTTP API",
        desc_en = "Enable S3 compatible service. Low-performance devices: recommend false, keep only HTTP API",
        example = "true"
    )]
    pub enable_s3: Option<bool>,
    #[config(
        desc_zh = "启用Web API服务，提供RESTful文件管理接口",
        desc_en = "Enable Web API service to provide RESTful file management interface",
        example = "true"
    )]
    pub enable_api: Option<bool>,
    #[config(
        desc_zh = "存储连接器配置列表，定义所有可用的存储后端",
        desc_en = "Storage connector configuration list, defines all available storage backends",
        example = "[{ name = \"local-fs\", driver = \"fs\", root = \"{RUNTIMEDIR}/vfs\", enable = true, options = {} }]"
    )]
    pub connectors: Option<Vec<VfsConnectorConfig>>,
    #[config(
        desc_zh = "存储池配置列表，定义可用的存储池",
        desc_en = "Storage pool configuration list, defines available storage pools",
        example = "[{ name = \"default-pool\", primary_connector = \"local-fs\", backup_connector = \"local-fs\", enable_write_cache = false, enable = true, options = {} }]"
    )]
    pub pools: Option<Vec<VfsPoolConfig>>,
    #[config(
        desc_zh = "存储策略配置列表，定义不同角色的存储分配策略",
        desc_en = "Storage policy configuration list, defines storage allocation policies for different roles",
        example = "[]"
    )]
    pub policies: Option<Vec<VfsPolicyConfig>>,
    #[config(
        desc_zh = "默认存储池名称，未指定策略时使用的存储池",
        desc_en = "Default storage pool name, used when no policy is specified",
        example = "default-pool"
    )]
    pub default_pool: Option<Arc<str>>,
    #[config(
        desc_zh = "临时文件管理配置",
        desc_en = "Temporary file management configuration"
    )]
    pub temp_file: Option<VfsTempFileConfig>,
    #[config(desc_zh = "本地读缓存配置", desc_en = "Local read cache configuration")]
    pub read_cache: Option<VfsReadCacheConfig>,
    #[config(
        desc_zh = "本地写缓存配置",
        desc_en = "Local write cache configuration"
    )]
    pub write_cache: Option<VfsWriteCacheConfig>,
    #[config(desc_zh = "批量操作配置", desc_en = "Batch operation configuration")]
    pub batch_operation: Option<VfsBatchOperationConfig>,
    #[config(
        desc_zh = "文件压缩解压配置",
        desc_en = "File compression and decompression configuration"
    )]
    pub file_compress: Option<VfsFileCompressConfig>,
    #[config(desc_zh = "文件分享配置", desc_en = "File sharing configuration")]
    pub file_share: Option<VfsFileShareConfig>,
    #[config(desc_zh = "文件索引配置", desc_en = "File index configuration")]
    pub file_index: Option<VfsFileIndexConfig>,
    #[config(desc_zh = "缩略图配置", desc_en = "Thumbnail configuration")]
    pub thumbnail: Option<VfsThumbnailConfig>,
    #[config(desc_zh = "维护任务配置", desc_en = "Maintenance task configuration")]
    pub maintenance: Option<VfsMaintenanceConfig>,
}

impl VfsStorageHubConfig {
    pub fn is_enable_webdav(&self) -> bool {
        yh_config_infra::config_require_clone!(
            self.enable_webdav,
            "vfs_storage_hub",
            "enable_webdav"
        )
    }
    pub fn is_enable_sftp(&self) -> bool {
        yh_config_infra::config_require_clone!(self.enable_sftp, "vfs_storage_hub", "enable_sftp")
    }
    pub fn is_enable_ftp(&self) -> bool {
        yh_config_infra::config_require_clone!(self.enable_ftp, "vfs_storage_hub", "enable_ftp")
    }
    pub fn is_enable_s3(&self) -> bool {
        yh_config_infra::config_require_clone!(self.enable_s3, "vfs_storage_hub", "enable_s3")
    }
    pub fn is_enable_api(&self) -> bool {
        yh_config_infra::config_require_clone!(self.enable_api, "vfs_storage_hub", "enable_api")
    }
    pub fn validate(&self, section: &str, errors: &mut Vec<String>) {
        let s = section;
        yh_config_infra::config_collect_bool!(self.enable_webdav, s, "enable_webdav", errors);
        yh_config_infra::config_collect_bool!(self.enable_sftp, s, "enable_sftp", errors);
        yh_config_infra::config_collect_bool!(self.enable_ftp, s, "enable_ftp", errors);
        yh_config_infra::config_collect_bool!(self.enable_s3, s, "enable_s3", errors);
        yh_config_infra::config_collect_bool!(self.enable_api, s, "enable_api", errors);
        yh_config_infra::config_collect_not_empty!(self.default_pool, s, "default_pool", errors);
        if let Some(tf) = &self.temp_file {
            yh_config_infra::config_collect_not_empty!(tf.dir, s, "temp_file.dir", errors);
            yh_config_infra::config_collect_gt_zero!(tf.max_age, s, "temp_file.max_age", errors);
        } else {
            errors.push(format!("[{}] temp_file is required (section)", s));
        }
        if let Some(rc) = &self.read_cache {
            yh_config_infra::config_collect_bool!(rc.enable, s, "read_cache.enable", errors);
            yh_config_infra::config_collect_not_empty!(rc.backend, s, "read_cache.backend", errors);
            yh_config_infra::config_collect_not_empty!(
                rc.local_dir,
                s,
                "read_cache.local_dir",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                rc.capacity_bytes,
                s,
                "read_cache.capacity_bytes",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                rc.max_file_size_bytes,
                s,
                "read_cache.max_file_size_bytes",
                errors
            );
            yh_config_infra::config_collect_bool!(
                rc.cache_thumbnail_paths,
                s,
                "read_cache.cache_thumbnail_paths",
                errors
            );
            yh_config_infra::config_collect_any!(
                rc.skip_extensions,
                s,
                "read_cache.skip_extensions",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(rc.ttl_secs, s, "read_cache.ttl_secs", errors);
            if !matches!(rc.backend.as_deref(), Some("memory") | Some("local_dir")) {
                errors.push(format!(
                    "[{}] read_cache.backend must be one of: memory | local_dir",
                    s
                ));
            }
            if let Some(exts) = &rc.skip_extensions {
                for (idx, ext) in exts.iter().enumerate() {
                    if ext.trim().trim_start_matches('.').is_empty() {
                        errors.push(format!(
                            "[{}] read_cache.skip_extensions[{}] cannot be empty",
                            s, idx
                        ));
                    }
                }
            }
        } else {
            errors.push(format!("[{}] read_cache is required (section)", s));
        }
        if let Some(wc) = &self.write_cache {
            yh_config_infra::config_collect_bool!(wc.enable, s, "write_cache.enable", errors);
            yh_config_infra::config_collect_not_empty!(
                wc.backend,
                s,
                "write_cache.backend",
                errors
            );
            yh_config_infra::config_collect_not_empty!(
                wc.local_dir,
                s,
                "write_cache.local_dir",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                wc.capacity_bytes,
                s,
                "write_cache.capacity_bytes",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                wc.max_file_size_bytes,
                s,
                "write_cache.max_file_size_bytes",
                errors
            );
            yh_config_infra::config_collect_bool!(
                wc.cache_thumbnail_paths,
                s,
                "write_cache.cache_thumbnail_paths",
                errors
            );
            yh_config_infra::config_collect_any!(
                wc.skip_extensions,
                s,
                "write_cache.skip_extensions",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                wc.flush_concurrency,
                s,
                "write_cache.flush_concurrency",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                wc.flush_interval_ms,
                s,
                "write_cache.flush_interval_ms",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                wc.flush_deadline_secs,
                s,
                "write_cache.flush_deadline_secs",
                errors
            );
            yh_config_infra::config_collect_not_empty!(
                wc.abnormal_spill_dir,
                s,
                "write_cache.abnormal_spill_dir",
                errors
            );
            if !matches!(wc.backend.as_deref(), Some("memory") | Some("local_dir")) {
                errors.push(format!(
                    "[{}] write_cache.backend must be one of: memory | local_dir",
                    s
                ));
            }
            if let Some(exts) = &wc.skip_extensions {
                for (idx, ext) in exts.iter().enumerate() {
                    if ext.trim().trim_start_matches('.').is_empty() {
                        errors.push(format!(
                            "[{}] write_cache.skip_extensions[{}] cannot be empty",
                            s, idx
                        ));
                    }
                }
            }
        } else {
            errors.push(format!("[{}] write_cache is required (section)", s));
        }
        if let Some(bo) = &self.batch_operation {
            yh_config_infra::config_collect_gt_zero!(
                bo.timeout_secs,
                s,
                "batch_operation.timeout_secs",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                bo.max_concurrent_tasks,
                s,
                "batch_operation.max_concurrent_tasks",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                bo.max_concurrent_tasks_low_memory,
                s,
                "batch_operation.max_concurrent_tasks_low_memory",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                bo.max_concurrent_tasks_throughput,
                s,
                "batch_operation.max_concurrent_tasks_throughput",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                bo.wal_min_size_bytes,
                s,
                "batch_operation.wal_min_size_bytes",
                errors
            );
            yh_config_infra::config_collect_bool!(
                bo.wal_skip_temp_path,
                s,
                "batch_operation.wal_skip_temp_path",
                errors
            );
        } else {
            errors.push(format!("[{}] batch_operation is required (section)", s));
        }
        if let Some(fc) = &self.file_compress {
            yh_config_infra::config_collect_bool!(fc.enable, s, "file_compress.enable", errors);
            yh_config_infra::config_collect_any!(
                fc.exe_7zip_path,
                s,
                "file_compress.exe_7zip_path",
                errors
            );
            yh_config_infra::config_collect_not_empty!(
                fc.default_compression_format,
                s,
                "file_compress.default_compression_format",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                fc.process_manager_max_concurrency,
                s,
                "file_compress.process_manager_max_concurrency",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                fc.process_manager_max_concurrency_low_memory,
                s,
                "file_compress.process_manager_max_concurrency_low_memory",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                fc.process_manager_max_concurrency_throughput,
                s,
                "file_compress.process_manager_max_concurrency_throughput",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                fc.max_cpu_threads,
                s,
                "file_compress.max_cpu_threads",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                fc.max_cpu_threads_low_memory,
                s,
                "file_compress.max_cpu_threads_low_memory",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                fc.max_cpu_threads_throughput,
                s,
                "file_compress.max_cpu_threads_throughput",
                errors
            );
            yh_config_infra::config_collect_range!(
                fc.compression_max_level,
                s,
                "file_compress.compression_max_level",
                0,
                9,
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                fc.timeout_secs,
                s,
                "file_compress.timeout_secs",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                fc.max_extract_size_gb,
                s,
                "file_compress.max_extract_size_gb",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                fc.max_compress_items,
                s,
                "file_compress.max_compress_items",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                fc.max_compress_total_size_mb,
                s,
                "file_compress.max_compress_total_size_mb",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                fc.max_compress_output_size_mb,
                s,
                "file_compress.max_compress_output_size_mb",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                fc.max_decompress_archive_size_mb,
                s,
                "file_compress.max_decompress_archive_size_mb",
                errors
            );
            yh_config_infra::config_collect_bool!(
                fc.enable_archive_browser,
                s,
                "file_compress.enable_archive_browser",
                errors
            );
            if fc.decompression_formats.is_none() {
                errors.push(format!(
                    "[{}] file_compress.decompression_formats is required",
                    s
                ));
            }
        } else {
            errors.push(format!("[{}] file_compress is required (section)", s));
        }
        if let Some(fi) = &self.file_index {
            yh_config_infra::config_collect_range!(
                fi.vfs_sync_index_mode,
                s,
                "file_index.vfs_sync_index_mode",
                0,
                3,
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                fi.max_concurrent_refresh,
                s,
                "file_index.max_concurrent_refresh",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                fi.max_concurrent_refresh_low_memory,
                s,
                "file_index.max_concurrent_refresh_low_memory",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                fi.max_concurrent_refresh_throughput,
                s,
                "file_index.max_concurrent_refresh_throughput",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                fi.max_files_per_refresh,
                s,
                "file_index.max_files_per_refresh",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                fi.max_files_per_refresh_low_memory,
                s,
                "file_index.max_files_per_refresh_low_memory",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                fi.max_files_per_refresh_throughput,
                s,
                "file_index.max_files_per_refresh_throughput",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                fi.refresh_timeout,
                s,
                "file_index.refresh_timeout",
                errors
            );
            yh_config_infra::config_collect_not_empty!(
                fi.refresh_trigger_filename,
                s,
                "file_index.refresh_trigger_filename",
                errors
            );
            yh_config_infra::config_collect_bool!(
                fi.enable_partition,
                s,
                "file_index.enable_partition",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                fi.partition_count,
                s,
                "file_index.partition_count",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                fi.admin_consistency_check_batch_size,
                s,
                "file_index.admin_consistency_check_batch_size",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                fi.admin_consistency_check_timeout,
                s,
                "file_index.admin_consistency_check_timeout",
                errors
            );
        } else {
            errors.push(format!("[{}] file_index is required (section)", s));
        }
        if let Some(thumb) = &self.thumbnail {
            yh_config_infra::config_collect_bool!(thumb.enabled, s, "thumbnail.enabled", errors);
            yh_config_infra::config_collect_not_empty!(
                thumb.cache_mode,
                s,
                "thumbnail.cache_mode",
                errors
            );
            if let Some(mode) = thumb.cache_mode.as_deref() {
                let mode = mode.trim().to_ascii_lowercase();
                if !matches!(mode.as_str(), "dir" | "global" | "none" | "db") {
                    errors.push(format!(
                        "[{}] thumbnail.cache_mode must be one of dir|global|none (legacy alias db is accepted)",
                        s
                    ));
                }
            }
            yh_config_infra::config_collect_not_empty!(
                thumb.cache_dir,
                s,
                "thumbnail.cache_dir",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                thumb.thumb_size_px,
                s,
                "thumbnail.thumb_size_px",
                errors
            );
            yh_config_infra::config_collect_not_empty!(
                thumb.thumb_format,
                s,
                "thumbnail.thumb_format",
                errors
            );
            yh_config_infra::config_collect_range!(
                thumb.thumb_quality,
                s,
                "thumbnail.thumb_quality",
                1,
                100,
                errors
            );
            if let Some(tools) = &thumb.tools {
                yh_config_infra::config_collect_any!(
                    tools.vips_path,
                    s,
                    "thumbnail.tools.vips_path",
                    errors
                );
                yh_config_infra::config_collect_any!(
                    tools.imagemagick_path,
                    s,
                    "thumbnail.tools.imagemagick_path",
                    errors
                );
                yh_config_infra::config_collect_any!(
                    tools.ffmpeg_path,
                    s,
                    "thumbnail.tools.ffmpeg_path",
                    errors
                );
                yh_config_infra::config_collect_any!(
                    tools.libreoffice_path,
                    s,
                    "thumbnail.tools.libreoffice_path",
                    errors
                );
            } else {
                errors.push(format!("[{}] thumbnail.tools is required (section)", s));
            }

            if let Some(image) = &thumb.image {
                yh_config_infra::config_collect_bool!(
                    image.enabled,
                    s,
                    "thumbnail.image.enabled",
                    errors
                );
                yh_config_infra::config_collect_gt_zero!(
                    image.max_size_mb,
                    s,
                    "thumbnail.image.max_size_mb",
                    errors
                );
                yh_config_infra::config_collect_gt_zero!(
                    image.timeout_secs,
                    s,
                    "thumbnail.image.timeout_secs",
                    errors
                );
                yh_config_infra::config_collect_any!(
                    image.backend,
                    s,
                    "thumbnail.image.backend",
                    errors
                );
                if let Some(backend) = image.backend.as_deref() {
                    let backend = backend.trim().to_ascii_lowercase();
                    if !matches!(backend.as_str(), "builtin" | "external") {
                        errors.push(format!(
                            "[{}] thumbnail.image.backend must be one of builtin|external",
                            s
                        ));
                    }
                }
            } else {
                errors.push(format!("[{}] thumbnail.image is required (section)", s));
            }

            let types = [
                (&thumb.video, "video"),
                (&thumb.pdf, "pdf"),
                (&thumb.office, "office"),
                (&thumb.text, "text"),
            ];
            for (t, name) in types {
                if let Some(tc) = t {
                    yh_config_infra::config_collect_bool!(
                        tc.enabled,
                        s,
                        format!("thumbnail.{}.enabled", name),
                        errors
                    );
                    yh_config_infra::config_collect_gt_zero!(
                        tc.max_size_mb,
                        s,
                        format!("thumbnail.{}.max_size_mb", name),
                        errors
                    );
                    yh_config_infra::config_collect_gt_zero!(
                        tc.timeout_secs,
                        s,
                        format!("thumbnail.{}.timeout_secs", name),
                        errors
                    );

                    if name == "video" && tc.enabled == Some(true) {
                        let seek_ratio = tc.seek_ratio;
                        let seek_seconds = tc.seek_seconds;
                        let ratio_ok = seek_ratio
                            .map(|r| r.is_finite() && r > 0.0 && r <= 1.0)
                            .unwrap_or(false);
                        let secs_ok = seek_seconds.map(|v| v > 0).unwrap_or(false);
                        if !ratio_ok && !secs_ok {
                            errors.push(format!(
                                "[{}] thumbnail.video.seek_ratio or thumbnail.video.seek_seconds is required",
                                s
                            ));
                        }
                        if let Some(r) = seek_ratio
                            && (!r.is_finite() || r <= 0.0 || r > 1.0)
                        {
                            errors.push(format!(
                                "[{}] thumbnail.video.seek_ratio must be within (0.0, 1.0]",
                                s
                            ));
                        }
                        if let Some(v) = seek_seconds
                            && v == 0
                        {
                            errors
                                .push(format!("[{}] thumbnail.video.seek_seconds must be > 0", s));
                        }
                    }

                    if name == "text" && tc.enabled == Some(true) {
                        yh_config_infra::config_collect_gt_zero!(
                            tc.max_chars,
                            s,
                            "thumbnail.text.max_chars",
                            errors
                        );
                    }
                } else {
                    errors.push(format!("[{}] thumbnail.{} is required (section)", s, name));
                }
            }

            if let (Some(tools), Some(image), Some(video), Some(pdf), Some(office), Some(text)) = (
                thumb.tools.as_ref(),
                thumb.image.as_ref(),
                thumb.video.as_ref(),
                thumb.pdf.as_ref(),
                thumb.office.as_ref(),
                thumb.text.as_ref(),
            ) {
                let vips = tools.vips_path.as_deref().unwrap_or("").trim();
                let magick = tools.imagemagick_path.as_deref().unwrap_or("").trim();
                let ffmpeg = tools.ffmpeg_path.as_deref().unwrap_or("").trim();
                let libreoffice = tools.libreoffice_path.as_deref().unwrap_or("").trim();
                let image_backend_is_builtin = image
                    .backend
                    .as_deref()
                    .map(|value| value.trim().eq_ignore_ascii_case("builtin"))
                    .unwrap_or(false);
                let video_backend_requires_ffmpeg =
                    !cfg!(any(target_os = "android", target_os = "ios"));

                let has_raster_tool = !vips.is_empty() || !magick.is_empty();
                if image.enabled == Some(true) && !image_backend_is_builtin && !has_raster_tool {
                    errors.push(format!(
                        "[{}] thumbnail.tools.vips_path or thumbnail.tools.imagemagick_path must be set for image thumbnails",
                        s
                    ));
                }
                if pdf.enabled == Some(true) && !has_raster_tool {
                    errors.push(format!(
                        "[{}] thumbnail.tools.vips_path or thumbnail.tools.imagemagick_path must be set for pdf thumbnails",
                        s
                    ));
                }
                if office.enabled == Some(true) {
                    if libreoffice.is_empty() {
                        errors.push(format!(
                            "[{}] thumbnail.tools.libreoffice_path must be set for office thumbnails",
                            s
                        ));
                    }
                    if !has_raster_tool {
                        errors.push(format!(
                            "[{}] thumbnail.tools.vips_path or thumbnail.tools.imagemagick_path must be set for office thumbnails",
                            s
                        ));
                    }
                }
                if video.enabled == Some(true) && video_backend_requires_ffmpeg && ffmpeg.is_empty()
                {
                    errors.push(format!(
                        "[{}] thumbnail.tools.ffmpeg_path must be set for video thumbnails",
                        s
                    ));
                }
                if text.enabled == Some(true) && magick.is_empty() {
                    errors.push(format!(
                        "[{}] thumbnail.tools.imagemagick_path must be set for text thumbnails",
                        s
                    ));
                }
            }
        } else {
            errors.push(format!("[{}] thumbnail is required (section)", s));
        }
        if let Some(fs) = &self.file_share {
            yh_config_infra::config_collect_bool!(fs.enable, s, "file_share.enable", errors);
            yh_config_infra::config_collect_bool!(
                fs.isdisable_seacher_engine,
                s,
                "file_share.isdisable_seacher_engine",
                errors
            );
            yh_config_infra::config_collect_bool!(
                fs.enable_user_direct_share,
                s,
                "file_share.enable_user_direct_share",
                errors
            );
            yh_config_infra::config_collect_any!(
                fs.trash_retention_days,
                s,
                "file_share.trash_retention_days",
                errors
            );
        } else {
            errors.push(format!("[{}] file_share is required (section)", s));
        }
        if let Some(mc) = &self.maintenance {
            yh_config_infra::config_collect_bool!(
                mc.s3_multipart_cleanup_enabled,
                s,
                "maintenance.s3_multipart_cleanup_enabled",
                errors
            );
            yh_config_infra::config_collect_gt_zero!(
                mc.s3_multipart_grace_period_secs,
                s,
                "maintenance.s3_multipart_grace_period_secs",
                errors
            );
        } else {
            errors.push(format!("[{}] maintenance is required (section)", s));
        }
        // Connectors / Pools / Policies are core for VFS and must be present.
        // Connectors and pools must be non-empty, otherwise VFS cannot route any request.
        yh_config_infra::config_collect_not_empty_vec!(self.connectors, s, "connectors", errors);
        yh_config_infra::config_collect_not_empty_vec!(self.pools, s, "pools", errors);
        if self.policies.is_none() {
            errors.push(format!("[{}] policies is required (even if empty [])", s));
        }

        let mut connector_name_set: HashSet<String> = HashSet::new();
        if let Some(connectors) = &self.connectors {
            for (i, conn) in connectors.iter().enumerate() {
                yh_config_infra::config_collect_not_empty!(
                    conn.name,
                    s,
                    format!("connectors[{}].name", i),
                    errors
                );
                yh_config_infra::config_collect_not_empty!(
                    conn.driver,
                    s,
                    format!("connectors[{}].driver", i),
                    errors
                );
                yh_config_infra::config_collect_not_empty!(
                    conn.root,
                    s,
                    format!("connectors[{}].root", i),
                    errors
                );

                if let Some(name) = conn
                    .name
                    .as_deref()
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
                    && !connector_name_set.insert(name.to_string())
                {
                    errors.push(format!(
                        "[{}] connectors[{}].name '{}' is duplicated",
                        s, i, name
                    ));
                }

                if matches!(conn.enable, Some(true))
                    && conn.driver.as_deref() == Some("android_saf")
                    && let Some(root) = conn.root.as_deref()
                    && !root.trim().starts_with("content://")
                {
                    errors.push(format!(
                        "[{}] connectors[{}].root must be a SAF tree uri (content://...) for android_saf",
                        s, i
                    ));
                }

                if matches!(conn.enable, Some(true))
                    && conn.driver.as_deref() == Some("ios_scoped_fs")
                    && let Some(root) = conn.root.as_deref()
                    && !root.trim().starts_with("bookmark_b64:")
                {
                    errors.push(format!(
                        "[{}] connectors[{}].root must start with 'bookmark_b64:' for ios_scoped_fs",
                        s, i
                    ));
                }

                #[cfg(any(target_os = "android", target_os = "ios"))]
                {
                    if matches!(conn.enable, Some(true)) && conn.driver.as_deref() == Some("fs") {
                        if let Some(root) = conn.root.as_deref() {
                            if let Err(e) =
                                mobile_fs_guard::validate_fs_root_under_runtime_dir(root)
                            {
                                errors.push(format!("[{}] connectors[{}].root: {}", s, i, e));
                            }
                        }
                    }
                }

                yh_config_infra::config_collect_bool!(
                    conn.enable,
                    s,
                    format!("connectors[{}].enable", i),
                    errors
                );
            }
        }

        let mut pool_name_set: HashSet<String> = HashSet::new();
        if let Some(pools) = &self.pools {
            for (i, pool) in pools.iter().enumerate() {
                yh_config_infra::config_collect_not_empty!(
                    pool.name,
                    s,
                    format!("pools[{}].name", i),
                    errors
                );
                yh_config_infra::config_collect_not_empty!(
                    pool.primary_connector,
                    s,
                    format!("pools[{}].primary_connector", i),
                    errors
                );

                // backup_connector is optional. When present, it must be non-empty.
                if let Some(backup) = pool.backup_connector.as_deref()
                    && backup.trim().is_empty()
                {
                    errors.push(format!(
                        "[{}] pools[{}].backup_connector cannot be empty (omit field or set a connector name)",
                        s, i
                    ));
                }

                yh_config_infra::config_collect_bool!(
                    pool.enable_write_cache,
                    s,
                    format!("pools[{}].enable_write_cache", i),
                    errors
                );
                if pool.enable_write_cache == Some(true) {
                    errors.push(format!(
                        "[{}] pools[{}].enable_write_cache=true is not supported; scoped engine already performs safe staging and this field must remain false",
                        s, i
                    ));
                }
                yh_config_infra::config_collect_bool!(
                    pool.enable,
                    s,
                    format!("pools[{}].enable", i),
                    errors
                );

                if let Some(name) = pool
                    .name
                    .as_deref()
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
                    && !pool_name_set.insert(name.to_string())
                {
                    errors.push(format!(
                        "[{}] pools[{}].name '{}' is duplicated",
                        s, i, name
                    ));
                }

                // Validate connector references for this pool.
                if !connector_name_set.is_empty()
                    && let Some(primary) = pool
                        .primary_connector
                        .as_deref()
                        .map(str::trim)
                        .filter(|v| !v.is_empty())
                    && !connector_name_set.contains(primary)
                {
                    errors.push(format!(
                        "[{}] pools[{}].primary_connector references unknown connector '{}'",
                        s, i, primary
                    ));
                }

                if !connector_name_set.is_empty()
                    && let Some(backup) = pool
                        .backup_connector
                        .as_deref()
                        .map(str::trim)
                        .filter(|v| !v.is_empty())
                    && !connector_name_set.contains(backup)
                {
                    errors.push(format!(
                        "[{}] pools[{}].backup_connector references unknown connector '{}'",
                        s, i, backup
                    ));
                }
            }
        }

        // default_pool must match an existing pool.
        if let Some(default_pool) = self
            .default_pool
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            && !pool_name_set.is_empty()
            && !pool_name_set.contains(default_pool)
        {
            errors.push(format!(
                "[{}] default_pool '{}' must match an existing pool name in pools",
                s, default_pool
            ));
        }

        if let Some(policies) = &self.policies {
            for (i, poly) in policies.iter().enumerate() {
                yh_config_infra::config_collect_not_empty!(
                    poly.role_id,
                    s,
                    format!("policies[{}].role_id", i),
                    errors
                );
                yh_config_infra::config_collect_not_empty!(
                    poly.pool_name,
                    s,
                    format!("policies[{}].pool_name", i),
                    errors
                );
                match poly.default_quota {
                    Some(v) if v >= 0 => {}
                    Some(_) => errors.push(format!(
                        "[{}] policies[{}].default_quota cannot be negative",
                        s, i
                    )),
                    None => errors.push(format!(
                        "[{}] policies[{}].default_quota is required (number >= 0)",
                        s, i
                    )),
                }
                if poly.max_private_mounts.is_none() {
                    errors.push(format!(
                        "[{}] policies[{}].max_private_mounts is required (number >= 0)",
                        s, i
                    ));
                }
                match poly.min_mount_sync_interval_minutes {
                    Some(v) if v > 0 => {}
                    Some(_) => errors.push(format!(
                        "[{}] policies[{}].min_mount_sync_interval_minutes must be > 0",
                        s, i
                    )),
                    None => errors.push(format!(
                        "[{}] policies[{}].min_mount_sync_interval_minutes is required (number > 0)",
                        s, i
                    )),
                }
                match poly.max_mount_sync_timeout_secs {
                    Some(v) if v > 0 => {}
                    Some(_) => errors.push(format!(
                        "[{}] policies[{}].max_mount_sync_timeout_secs must be > 0",
                        s, i
                    )),
                    None => errors.push(format!(
                        "[{}] policies[{}].max_mount_sync_timeout_secs is required (number > 0)",
                        s, i
                    )),
                }

                if let Some(pool_name) = poly
                    .pool_name
                    .as_deref()
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
                    && !pool_name_set.is_empty()
                    && !pool_name_set.contains(pool_name)
                {
                    errors.push(format!(
                        "[{}] policies[{}].pool_name references unknown pool '{}'",
                        s, i, pool_name
                    ));
                }
            }
        }
    }
    pub fn get_connectors(&self) -> &Vec<VfsConnectorConfig> {
        yh_config_infra::config_require!(self.connectors, "vfs_storage_hub", "connectors")
    }

    pub fn get_pools(&self) -> &Vec<VfsPoolConfig> {
        yh_config_infra::config_require!(self.pools, "vfs_storage_hub", "pools")
    }
    pub fn get_policies(&self) -> &Vec<VfsPolicyConfig> {
        yh_config_infra::config_require!(self.policies, "vfs_storage_hub", "policies")
    }
    pub fn get_temp_file(&self) -> &VfsTempFileConfig {
        yh_config_infra::config_require!(self.temp_file, "vfs_storage_hub", "temp_file")
    }
    pub fn get_read_cache(&self) -> &VfsReadCacheConfig {
        yh_config_infra::config_require!(self.read_cache, "vfs_storage_hub", "read_cache")
    }
    pub fn get_write_cache(&self) -> &VfsWriteCacheConfig {
        yh_config_infra::config_require!(self.write_cache, "vfs_storage_hub", "write_cache")
    }
    pub fn get_batch_operation(&self) -> &VfsBatchOperationConfig {
        yh_config_infra::config_require!(self.batch_operation, "vfs_storage_hub", "batch_operation")
    }
    pub fn get_file_compress(&self) -> &VfsFileCompressConfig {
        yh_config_infra::config_require!(self.file_compress, "vfs_storage_hub", "file_compress")
    }
    pub fn get_file_index(&self) -> &VfsFileIndexConfig {
        yh_config_infra::config_require!(self.file_index, "vfs_storage_hub", "file_index")
    }
    pub fn get_thumbnail(&self) -> &VfsThumbnailConfig {
        yh_config_infra::config_require!(self.thumbnail, "vfs_storage_hub", "thumbnail")
    }
    pub fn get_maintenance(&self) -> &VfsMaintenanceConfig {
        yh_config_infra::config_require!(self.maintenance, "vfs_storage_hub", "maintenance")
    }
    pub fn get_file_share(&self) -> &VfsFileShareConfig {
        yh_config_infra::config_require!(self.file_share, "vfs_storage_hub", "file_share")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ConfigDoc)]
pub struct VfsStorageHubAppConfig {
    #[config(
        desc_zh = "VFS 存储枢纽核心配置 (连接器、池、策略)",
        desc_en = "VFS Storage Hub core configuration (Connectors, Pools, Policies)"
    )]
    pub vfs_storage_hub: VfsStorageHubConfig,
}

impl ConfigApp for VfsStorageHubAppConfig {
    fn get_section_name() -> &'static str {
        "vfs_storage_hub"
    }
}

impl VfsStorageHubAppConfig {
    pub fn validate(&self, errors: &mut Vec<String>) {
        self.vfs_storage_hub
            .validate(Self::get_section_name(), errors);
    }
}

pub struct VfsHubConfigManager {
    pub inner: BaseConfigManager<VfsStorageHubAppConfig>,
}

pub struct VfsHubConfigGuard {
    guard: OwnedRwLockReadGuard<Arc<VfsStorageHubAppConfig>>,
}

impl Deref for VfsHubConfigGuard {
    type Target = VfsStorageHubConfig;

    fn deref(&self) -> &Self::Target {
        &self.guard.as_ref().vfs_storage_hub
    }
}

impl VfsHubConfigManager {
    pub async fn new(config_path: &str) -> anyhow::Result<Self> {
        let inner = BaseConfigManager::new(config_path)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;
        Ok(Self { inner })
    }
    pub fn get_config_arc(&self) -> Arc<tokio::sync::RwLock<Arc<VfsStorageHubAppConfig>>> {
        self.inner.get_config_arc()
    }
    pub async fn get_config(&self) -> VfsHubConfigGuard {
        let guard = self.inner.get_config_arc().read_owned().await;
        VfsHubConfigGuard { guard }
    }
    pub async fn validate(&self) -> Result<(), String> {
        self.inner
            .with_config(|config: &VfsStorageHubAppConfig| {
                let mut errors = Vec::new();
                config.validate(&mut errors);
                if errors.is_empty() {
                    Ok(())
                } else {
                    Err(errors.join(", "))
                }
            })
            .await
    }
}

impl_config_manager_boilerplate!(
    VfsHubConfigManager,
    VfsStorageHubAppConfig,
    VFS_HUB_CONFIG_MANAGER,
    init_vfs_hub_config_manager,
    get_vfs_hub_config_manager
);

pub async fn get_vfs_hub_config() -> VfsHubConfigGuard {
    let manager =
        yh_config_infra::config_require_manager!(get_vfs_hub_config_manager(), "vfs_storage_hub");
    manager.get_config().await
}
