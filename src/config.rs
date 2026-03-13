// VFS storage hub configuration module
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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

    pub fn validate_fs_root_under_app_data_dir(root: &str) -> Result<(), String> {
        let app_data_dir = yh_config_infra::utils::get_app_data_dir()
            .ok_or_else(|| "{APPDATADIR} is not initialized".to_string())?;

        let base = normalize_absolute_path(Path::new(app_data_dir.as_ref()))?;
        let candidate = normalize_absolute_path(Path::new(root))?;

        if !candidate.starts_with(&base) {
            return Err(format!(
                "fs connector root must be inside app sandbox (under APPDATADIR). Got: '{}' (APPDATADIR='{}')",
                root,
                app_data_dir
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
        desc_zh = "存储后端驱动类型，可选项: fs(本地文件系统)|android_saf(Android SAF 授权目录)|s3(AWS S3兼容)|webdav(WebDAV)|ftp|sftp，默认fs，生产环境推荐s3",
        desc_en = "Storage backend driver type, options: fs(local filesystem)|android_saf(Android SAF granted directory)|s3(AWS S3 compatible)|webdav|ftp|sftp, default fs, recommend s3 for production",
        example = "fs"
    )]
    pub driver: Option<Arc<str>>,
    #[config(
        desc_zh = "存储根路径，对于本地文件系统为目录路径，对于S3等为bucket名称",
        desc_en = "Storage root path, directory path for local filesystem, bucket name for S3, etc.",
        example = "{APPDATADIR}/vfs"
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
        desc_zh = "备用存储连接器名称，主连接器不可用时自动切换",
        desc_en = "Backup storage connector name, automatically switches when primary is unavailable",
        example = "local-backup"
    )]
    pub backup_connector: Option<Arc<str>>,
    #[config(
        desc_zh = "是否启用写入缓存，提升写入性能但可能降低数据一致性",
        desc_en = "Whether to enable write cache to improve write performance but may reduce data consistency",
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
        yh_config_infra::config_collect_not_empty!(self.exe_7zip_path, s, "exe_7zip_path", errors);
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
        example = "{APPDATADIR}/tmp/vfs"
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
        desc_zh = "WAL（写前日志）最小大小（字节），达到此大小后触发刷盘",
        desc_en = "WAL (Write-Ahead Log) minimum size (bytes), triggers flush when reached",
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
        example = "[]"
    )]
    pub connectors: Option<Vec<VfsConnectorConfig>>,
    #[config(
        desc_zh = "存储池配置列表，定义可用的存储池",
        desc_en = "Storage pool configuration list, defines available storage pools",
        example = "[]"
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

                #[cfg(any(target_os = "android", target_os = "ios"))]
                {
                    if matches!(conn.enable, Some(true)) && conn.driver.as_deref() == Some("fs") {
                        if let Some(root) = conn.root.as_deref() {
                            if let Err(e) = mobile_fs_guard::validate_fs_root_under_app_data_dir(root)
                            {
                                errors.push(format!(
                                    "[{}] connectors[{}].root: {}",
                                    s, i, e
                                ));
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
        } else {
            errors.push(format!("[{}] connectors is required (even if empty [])", s));
        }
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
                yh_config_infra::config_collect_not_empty!(
                    pool.backup_connector,
                    s,
                    format!("pools[{}].backup_connector", i),
                    errors
                );
                yh_config_infra::config_collect_bool!(
                    pool.enable_write_cache,
                    s,
                    format!("pools[{}].enable_write_cache", i),
                    errors
                );
                yh_config_infra::config_collect_bool!(
                    pool.enable,
                    s,
                    format!("pools[{}].enable", i),
                    errors
                );
            }
        } else {
            errors.push(format!("[{}] pools is required", s));
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
                yh_config_infra::config_collect_gt_zero!(
                    poly.default_quota,
                    s,
                    format!("policies[{}].default_quota", i),
                    errors
                );
            }
        } else {
            errors.push(format!("[{}] policies is required", s));
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
    pub fn get_batch_operation(&self) -> &VfsBatchOperationConfig {
        yh_config_infra::config_require!(self.batch_operation, "vfs_storage_hub", "batch_operation")
    }
    pub fn get_file_compress(&self) -> &VfsFileCompressConfig {
        yh_config_infra::config_require!(self.file_compress, "vfs_storage_hub", "file_compress")
    }
    pub fn get_file_index(&self) -> &VfsFileIndexConfig {
        yh_config_infra::config_require!(self.file_index, "vfs_storage_hub", "file_index")
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
        let c = &self.vfs_storage_hub;

        let s = Self::get_section_name();
        yh_config_infra::config_collect_bool!(c.enable_webdav, s, "enable_webdav", errors);
        yh_config_infra::config_collect_bool!(c.enable_sftp, s, "enable_sftp", errors);
        yh_config_infra::config_collect_bool!(c.enable_ftp, s, "enable_ftp", errors);
        yh_config_infra::config_collect_bool!(c.enable_s3, s, "enable_s3", errors);
        yh_config_infra::config_collect_bool!(c.enable_api, s, "enable_api", errors);
        yh_config_infra::config_collect_not_empty!(c.default_pool, s, "default_pool", errors);
        if let Some(tf) = &c.temp_file {
            yh_config_infra::config_collect_not_empty!(tf.dir, s, "temp_file.dir", errors);
            yh_config_infra::config_collect_gt_zero!(tf.max_age, s, "temp_file.max_age", errors);
        } else {
            errors.push(format!("[{}] temp_file is required (section)", s));
        }
        if let Some(bo) = &c.batch_operation {
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
        if let Some(fc) = &c.file_compress {
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
        if let Some(fi) = &c.file_index {
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
        if let Some(fs) = &c.file_share {
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
        if let Some(mc) = &c.maintenance {
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
        if let Some(connectors) = &c.connectors {
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
                yh_config_infra::config_collect_bool!(
                    conn.enable,
                    s,
                    format!("connectors[{}].enable", i),
                    errors
                );
            }
        } else {
            errors.push(format!("[{}] connectors is required (even if empty [])", s));
        }
        if let Some(pools) = &c.pools {
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
                yh_config_infra::config_collect_not_empty!(
                    pool.backup_connector,
                    s,
                    format!("pools[{}].backup_connector", i),
                    errors
                );
                yh_config_infra::config_collect_bool!(
                    pool.enable_write_cache,
                    s,
                    format!("pools[{}].enable_write_cache", i),
                    errors
                );
                yh_config_infra::config_collect_bool!(
                    pool.enable,
                    s,
                    format!("pools[{}].enable", i),
                    errors
                );
            }
        } else {
            errors.push(format!("[{}] pools is required", s));
        }
        if let Some(policies) = &c.policies {
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
                yh_config_infra::config_collect_gt_zero!(
                    poly.default_quota,
                    s,
                    format!("policies[{}].default_quota", i),
                    errors
                );
            }
        } else {
            errors.push(format!("[{}] policies is required", s));
        }
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
