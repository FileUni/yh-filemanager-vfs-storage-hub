// VFS Storage Hub compression and decompression (7z process version)
//
//7z
//  Implements all compression/decompression via external 7z process, supporting concurrency control and streaming.
use crate::vfs::error::{VfsError, VfsResult};
use crate::vfs::{VfsFileInfo, VfsStorage};
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tar::{Archive, Builder};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use yh_console_log::yhlog;
use yh_external_process_manager::get_global_manager;
use zip::ZipArchive;
use zip::write::SimpleFileOptions;
fn is_extension_allowed(path: &str, allowed_formats: &[Arc<str>]) -> bool {
    let path_lower = path.to_lowercase();
    for format in allowed_formats {
        let ext = format.to_lowercase();
        if path_lower.ends_with(&format!(".{}", ext)) {
            return true;
        }
    }
    false
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CompressionFormat {
    Zip,
    SevenZip,
    Tar,
    Gzip,
    TarGz,
}
impl CompressionFormat {
    pub fn from_path(path: &str) -> Option<Self> {
        let path_lower = path.to_lowercase();
        if path_lower.ends_with(".zip") {
            Some(CompressionFormat::Zip)
        } else if path_lower.ends_with(".7z") {
            Some(CompressionFormat::SevenZip)
        } else if path_lower.ends_with(".tar.gz") {
            Some(CompressionFormat::TarGz)
        } else if path_lower.ends_with(".tar") {
            Some(CompressionFormat::Tar)
        } else if path_lower.ends_with(".gz") {
            Some(CompressionFormat::Gzip)
        } else {
            None
        }
    }
    pub fn extension(&self) -> &'static str {
        match self {
            CompressionFormat::Zip => "zip",
            CompressionFormat::SevenZip => "7z",
            CompressionFormat::Tar => "tar",
            CompressionFormat::Gzip => "gz",
            CompressionFormat::TarGz => "tar.gz",
        }
    }
    pub fn type_flag(&self) -> &'static str {
        match self {
            CompressionFormat::Zip => "zip",
            CompressionFormat::SevenZip => "7z",
            CompressionFormat::Tar => "tar",
            CompressionFormat::Gzip => "gzip",
            CompressionFormat::TarGz => "tar.gz",
        }
    }
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CompressionOptions {
    pub format: CompressionFormat,
    pub password: Option<String>,
    pub encrypt_filenames: bool,
    pub compression_level: u8,
}
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct DecompressionOptions {
    pub overwrite: bool,
    pub password: Option<String>,
}
/// Copy VFS path to local temp dir recursively
async fn copy_to_local_temp(engine: &dyn VfsStorage, src: &str, dst: &Path) -> VfsResult<()> {
    let info = engine.stat(src).await?;
    if info.is_dir {
        fs::create_dir_all(dst).await.map_err(VfsError::Io)?;
        let entries = engine.list(src).await?;
        for entry in entries {
            let child_src = format!("{}/{}", src.trim_end_matches('/'), entry.name);
            let child_dst = dst.join(&*entry.name);
            Box::pin(copy_to_local_temp(engine, &child_src, &child_dst)).await?;
        }
        Ok(())
    } else {
        let (mut stream, _info) = engine.read_stream(src).await?;
        let mut file = fs::File::create(dst).await.map_err(VfsError::Io)?;
        use futures::StreamExt;
        while let Some(chunk) = stream.next().await {
            file.write_all(&chunk?).await.map_err(VfsError::Io)?;
        }
        file.sync_all().await.map_err(VfsError::Io)?;
        Ok(())
    }
}
/// Upload local dir to VFS recursively
async fn copy_from_local_temp(src: &Path, dst: &str, engine: &dyn VfsStorage) -> VfsResult<()> {
    if src.is_dir() {
        engine.create_dir_all(dst).await?;
        let mut entries = fs::read_dir(src).await.map_err(VfsError::Io)?;
        while let Some(entry) = entries.next_entry().await.map_err(VfsError::Io)? {
            let s = entry.path();
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            let d = format!("{}/{}", dst.trim_end_matches('/'), name_str);
            Box::pin(copy_from_local_temp(&s, &d, engine)).await?;
        }
        Ok(())
    } else {
        let content = fs::read(src).await.map_err(VfsError::Io)?;
        engine.write(dst, bytes::Bytes::from(content)).await?;
        Ok(())
    }
}
/// Native ZIP compression implementation
fn compress_zip_native(src_dir: &Path, dst_file: &Path, opts: &CompressionOptions) -> anyhow::Result<()> {
    let file = File::create(dst_file)?;
    let mut zip = zip::ZipWriter::new(file);
    let mut options = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated).unix_permissions(0o755);
    if let Some(pwd) = &opts.password {
        options = options.with_aes_encryption(zip::AesMode::Aes256, pwd);
    }
    let mut walk_dir = vec![src_dir.to_path_buf()];
    while let Some(current) = walk_dir.pop() {
        for entry in std::fs::read_dir(current)? {
            let entry = entry?;
            let path = entry.path();
            let name = path.strip_prefix(src_dir)?.to_string_lossy().replace('\\', "/");
            if path.is_dir() {
                zip.add_directory(name, options)?;
                walk_dir.push(path);
            } else {
                zip.start_file(name, options)?;
                let mut f = File::open(path)?;
                std::io::copy(&mut f, &mut zip)?;
            }
        }
    }
    zip.finish()?;
    Ok(())
}
/// Native ZIP decompression implementation
fn decompress_zip_native(src_file: &Path, dst_dir: &Path, password: Option<&str>) -> anyhow::Result<()> {
    let file = File::open(src_file)?;
    let mut archive = ZipArchive::new(file)?;
    for i in 0..archive.len() {
        let mut file = if let Some(pwd) = password { archive.by_index_decrypt(i, pwd.as_bytes())? } else { archive.by_index(i)? };
        let outpath = match file.enclosed_name() {
            Some(path) => dst_dir.join(path),
            None => continue,
        };
        if file.is_dir() {
            std::fs::create_dir_all(&outpath)?;
        } else {
            if let Some(p) = outpath.parent()
                && !p.exists()
            {
                std::fs::create_dir_all(p)?;
            }
            let mut outfile = File::create(&outpath)?;
            std::io::copy(&mut file, &mut outfile)?;
        }
    }
    Ok(())
}
/// Native TarGz compression implementation
fn compress_tar_gz_native(src_dir: &Path, dst_file: &Path, level: u8) -> anyhow::Result<()> {
    let file = File::create(dst_file)?;
    let enc = GzEncoder::new(file, Compression::new(level as u32));
    let mut tar = Builder::new(enc);
    tar.append_dir_all(".", src_dir)?;
    tar.finish()?;
    Ok(())
}
/// Native TarGz decompression implementation
fn decompress_tar_gz_native(src_file: &Path, dst_dir: &Path) -> anyhow::Result<()> {
    let file = File::open(src_file)?;
    let tar = GzDecoder::new(file);
    let mut archive = Archive::new(tar);
    archive.unpack(dst_dir)?;
    Ok(())
}
/// Native Gzip compression implementation (single file only)
fn compress_gz_native(src_file: &Path, dst_file: &Path, level: u8) -> anyhow::Result<()> {
    let mut input = File::open(src_file)?;
    let output = File::create(dst_file)?;
    let mut encoder = GzEncoder::new(output, Compression::new(level as u32));
    std::io::copy(&mut input, &mut encoder)?;
    encoder.finish()?;
    Ok(())
}
/// Native Gzip decompression implementation
fn decompress_gz_native(src_file: &Path, dst_file: &Path) -> anyhow::Result<()> {
    let input = File::open(src_file)?;
    let mut decoder = GzDecoder::new(input);
    let mut output = File::create(dst_file)?;
    std::io::copy(&mut decoder, &mut output)?;
    Ok(())
}
/// Core compression logic: hybrid mode
pub async fn compress_task(engine: &dyn VfsStorage, src_path: &str, dst_path: &str, user_id: &str, opts: &CompressionOptions, delete_source: bool) -> Result<VfsFileInfo, anyhow::Error> {
    let cfg = crate::config::get_vfs_hub_config().await;
    let fc = cfg.get_file_compress();
    if !fc.is_enabled() {
        return Err(anyhow::anyhow!("Compression is disabled"));
    }
    // Basic quota check
    engine.check_quota(0).await.map_err(|e| anyhow::anyhow!("Quota check failed: {}", e))?;
    // Preliminary check
    let info = engine.stat(src_path).await?;
    let (total_items, total_size) = if info.is_dir {
        let entries = engine.list_recursive(src_path).await?;
        (entries.len(), entries.iter().map(|e| e.size).sum::<u64>())
    } else {
        (1, info.size)
    };
    let max_compress_items = yh_config_infra::config_require_clone!(fc.max_compress_items, "vfs_storage_hub", "file_compress.max_compress_items");
    if total_items > max_compress_items {
        return Err(anyhow::anyhow!("Compression aborted: too many items ({}), limit is {}", total_items, max_compress_items));
    }
    let max_compress_total_size_mb = yh_config_infra::config_require_clone!(fc.max_compress_total_size_mb, "vfs_storage_hub", "file_compress.max_compress_total_size_mb");
    let max_in_size = max_compress_total_size_mb * 1024 * 1024;
    if total_size > max_in_size {
        return Err(anyhow::anyhow!("Compression aborted: source total size ({} MB) exceeds limit ({} MB)", total_size / 1024 / 1024, max_compress_total_size_mb));
    }
    let manager = yh_config_infra::config_require_manager!(get_global_manager(), "external_process_manager");
    manager
        .run_with_permit(yh_external_process_manager::TaskPriority::Normal, || async move {
            let temp_manager = yh_config_infra::config_require_manager!(crate::utils::get_global_temp_manager().await, "vfs_storage_hub");
            let (t_dir, _guard) = temp_manager.create_user_temp_dir(user_id, "compress").await?;
            let local_input = t_dir.join("input");
            let local_output = t_dir.join(format!("output.{}", opts.format.extension()));
            // Prepare data
            copy_to_local_temp(engine, src_path, &local_input).await?;
            // Choose compression method based on config
            let exe_path = fc.get_exe_7zip_path();
            let use_7z = !exe_path.is_empty() || opts.format == CompressionFormat::SevenZip;
            let output_path_for_upload = if use_7z {
                if exe_path.is_empty() && opts.format == CompressionFormat::SevenZip {
                    return Err(anyhow::anyhow!("7z format requires 7z executable to be configured."));
                }
                let mut cmd = Command::new(if exe_path.is_empty() { "7z" } else { exe_path });
                cmd.arg("a")
                    .arg(&local_output)
                    .arg(format!("-t{}", opts.format.type_flag()))
                    .arg(format!("-mx={}", opts.compression_level))
                    .arg(format!("-mmt={}", fc.get_effective_max_cpu_threads()))
                    .arg("-y");
                // Add password argument
                if let Some(pwd) = &opts.password {
                    cmd.arg(format!("-p{}", pwd));
                    if opts.format == CompressionFormat::SevenZip && opts.encrypt_filenames {
                        cmd.arg("-mhe=on");
                    }
                }
                let mut child = match cmd
                    .args(if local_input.is_dir() { vec![format!("{}/.", local_input.to_string_lossy())] } else { vec![local_input.to_string_lossy().into_owned()] })
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                {
                    Ok(c) => c,
                    Err(e) => {
                        let err_msg = format!("Failed to start 7z process: {}. Path: {}", e, exe_path);
                        yhlog("error", &err_msg);
                        return Err(anyhow::anyhow!(err_msg));
                    }
                };
                let max_out_size = yh_config_infra::config_require_clone!(fc.max_compress_output_size_mb, "vfs_storage_hub", "file_compress.max_compress_output_size_mb") * 1024 * 1024;
                let mut monitor_interval = tokio::time::interval(Duration::from_secs(1));
                let timeout_duration = Duration::from_secs(yh_config_infra::config_require_clone!(fc.timeout_secs, "vfs_storage_hub", "file_compress.timeout_secs"));
                loop {
                    tokio::select! {
                        res = tokio::time::timeout(timeout_duration, child.wait()) => {
                            match res {
                                Ok(status) => {
                                    let exit_status = status?;
                                    if !exit_status.success() {
                                        let output = child.wait_with_output().await?;
                                        let stderr = String::from_utf8_lossy(&output.stderr);
                                        return Err(anyhow::anyhow!("7z compression failed: {}. {}", exit_status, stderr));
                                    }
                                    break;
                                }
                                Err(_) => {
                                    let _ = child.kill().await;
                                    return Err(anyhow::anyhow!("Compression timed out"));
                                }
                            }
                        }
                        _ = monitor_interval.tick() => {
                            if let Ok(meta) = fs::metadata(&local_output).await
                                && meta.len() > max_out_size
                            {
                                let _ = child.kill().await;
                                return Err(anyhow::anyhow!("Compression output limit exceeded"));
                            }
                        }
                    }
                }
                local_output
            } else {
                // Native Rust processing
                let format = opts.format;
                let level = opts.compression_level;
                let input_path = local_input;
                let output_path = local_output;
                let zip_password = opts.password.as_deref().map(str::to_owned);
                let zip_encrypt_filenames = opts.encrypt_filenames;
                tokio::task::spawn_blocking(move || {
                    match format {
                        CompressionFormat::Zip => compress_zip_native(
                            &input_path,
                            &output_path,
                            &CompressionOptions {
                                format,
                                password: zip_password,
                                encrypt_filenames: zip_encrypt_filenames,
                                compression_level: level,
                            },
                        ),
                        CompressionFormat::TarGz => compress_tar_gz_native(&input_path, &output_path, level),
                        CompressionFormat::Gzip => {
                            if input_path.is_dir() {
                                return Err(anyhow::anyhow!("Gzip format only supports single file compression."));
                            }
                            compress_gz_native(&input_path, &output_path, level)
                        }
                        _ => Err(anyhow::anyhow!("Native compression for {:?} is not supported without 7z.", format)),
                    }?;
                    Ok::<std::path::PathBuf, anyhow::Error>(output_path)
                })
                .await??
            };
            // Upload result
            let out_data = fs::read(&output_path_for_upload).await?;
            let info = engine.write(dst_path, bytes::Bytes::from(out_data)).await?;
            if delete_source {
                let _ = engine.delete(src_path).await;
                if let Some(parent) = Path::new(src_path).parent() {
                    let _ = engine.sync_index(&parent.to_string_lossy()).await;
                }
            }
            Ok(info)
        })
        .await
}
/// Core decompression logic
pub async fn decompress_task(engine: &dyn VfsStorage, src_path: &str, dst_dir: &str, user_id: &str, opts: &DecompressionOptions, delete_archive: bool) -> Result<VfsFileInfo, anyhow::Error> {
    let cfg = crate::config::get_vfs_hub_config().await;
    let fc = cfg.get_file_compress();
    let allowed = yh_config_infra::config_require!(fc.decompression_formats, "vfs_storage_hub", "file_compress.decompression_formats");
    if !is_extension_allowed(src_path, allowed) {
        return Err(anyhow::anyhow!("Decompression format not supported for: {}", src_path));
    }
    engine.check_quota(0).await.map_err(|e| anyhow::anyhow!("Quota check failed: {}", e))?;
    let info = engine.stat(src_path).await?;
    let max_archive_size_mb = yh_config_infra::config_require_clone!(fc.max_decompress_archive_size_mb, "vfs_storage_hub", "file_compress.max_decompress_archive_size_mb");
    let max_archive_size = max_archive_size_mb * 1024 * 1024;
    if info.size > max_archive_size {
        return Err(anyhow::anyhow!("Decompression aborted: archive size exceeds limit"));
    }
    let manager = yh_config_infra::config_require_manager!(get_global_manager(), "external_process_manager");
    manager
        .run_with_permit(yh_external_process_manager::TaskPriority::Normal, || async move {
            let temp_manager = yh_config_infra::config_require_manager!(crate::utils::get_global_temp_manager().await, "vfs_storage_hub");
            let (t_dir, _guard) = temp_manager.create_user_temp_dir(user_id, "decompress").await?;
            let local_archive = t_dir.join("input.archive");
            let local_output_dir = t_dir.join("output");
            fs::create_dir_all(&local_output_dir).await?;
            let (data, _) = engine.read(src_path).await?;
            fs::write(&local_archive, data).await?;
            let format = CompressionFormat::from_path(src_path).ok_or_else(|| anyhow::anyhow!("Unsupported format"))?;
            let exe_path = fc.get_exe_7zip_path();
            let use_7z = !exe_path.is_empty() || format == CompressionFormat::SevenZip;
            if use_7z {
                if exe_path.is_empty() && format == CompressionFormat::SevenZip {
                    return Err(anyhow::anyhow!("7z format requires 7z executable to be configured."));
                }
                let mut child = Command::new(if exe_path.is_empty() { "7z" } else { exe_path })
                    .arg("x")
                    .arg(&local_archive)
                    .arg(format!("-o{}", local_output_dir.to_string_lossy()))
                    .arg(format!("-mmt={}", fc.get_effective_max_cpu_threads()))
                    .arg("-y")
                    .args(if let Some(pwd) = &opts.password { vec![format!("-p{}", pwd)] } else { vec![] })
                    .args(if opts.overwrite { vec!["-aoa"] } else { vec!["-aos"] })
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()?;
                let timeout_duration = Duration::from_secs(yh_config_infra::config_require_clone!(fc.timeout_secs, "vfs_storage_hub", "file_compress.timeout_secs"));
                match tokio::time::timeout(timeout_duration, child.wait()).await {
                    Ok(Ok(status)) => {
                        if !status.success() {
                            return Err(anyhow::anyhow!("7z extraction failed with status {}", status));
                        }
                    }
                    Ok(Err(e)) => return Err(anyhow::anyhow!("Wait for 7z failed: {}", e)),
                    Err(_) => {
                        let _ = child.kill().await;
                        return Err(anyhow::anyhow!("Decompression timed out"));
                    }
                }
            } else {
                let archive_path = local_archive;
                let output_dir = local_output_dir.to_path_buf();
                let gzip_output_name = if format == CompressionFormat::Gzip {
                    Some(Path::new(src_path).file_stem().ok_or_else(|| anyhow::anyhow!("Invalid gzip source path without file stem: {}", src_path))?.to_os_string())
                } else {
                    None
                };
                let password = opts.password.as_deref().map(str::to_owned);
                tokio::task::spawn_blocking(move || match format {
                    CompressionFormat::Zip => decompress_zip_native(&archive_path, &output_dir, password.as_deref()),
                    CompressionFormat::TarGz => decompress_tar_gz_native(&archive_path, &output_dir),
                    CompressionFormat::Gzip => {
                        let file_name = gzip_output_name.ok_or_else(|| anyhow::anyhow!("Missing derived output file name for gzip decompression"))?;
                        let out_file = output_dir.join(file_name);
                        decompress_gz_native(&archive_path, &out_file)
                    }
                    _ => Err(anyhow::anyhow!("Native decompression for {:?} is not supported without 7z.", format)),
                })
                .await??;
            }
            let mut total_size = 0u64;
            let mut dir_queue = vec![local_output_dir.to_path_buf()];
            let max_extract_size_gb = yh_config_infra::config_require_clone!(fc.max_extract_size_gb, "vfs_storage_hub", "file_compress.max_extract_size_gb");
            let max_total_size = max_extract_size_gb * 1024 * 1024 * 1024;
            while let Some(current_dir) = dir_queue.pop() {
                let mut entries = fs::read_dir(current_dir).await?;
                while let Some(entry) = entries.next_entry().await? {
                    let meta = entry.metadata().await?;
                    if meta.is_dir() {
                        dir_queue.push(entry.path());
                    } else {
                        total_size += meta.len();
                        if total_size > max_total_size {
                            return Err(anyhow::anyhow!("Extraction failed: output size exceeds limit"));
                        }
                    }
                }
            }
            engine.check_quota(total_size as i64).await.map_err(|e| anyhow::anyhow!("Quota check failed after extraction: {}", e))?;
            copy_from_local_temp(&local_output_dir, dst_dir, engine).await?;
            let _ = engine.sync_index(dst_dir).await;
            if delete_archive {
                let _ = engine.delete(src_path).await;
            }
            engine.stat(dst_dir).await.map_err(|e| anyhow::anyhow!(e))
        })
        .await
}
/// Streaming compression download
pub async fn stream_compress_reader(engine: Arc<dyn VfsStorage>, src_path: String, format: CompressionFormat) -> Result<impl tokio::io::AsyncRead, anyhow::Error> {
    let cfg = crate::config::get_vfs_hub_config().await;
    let fc = cfg.get_file_compress();
    let info = engine.stat(&src_path).await?;
    if info.is_dir {
        return Err(anyhow::anyhow!("Streaming directory compression is not supported via stdin."));
    }
    let (mut vfs_stream, _) = engine.read_stream(&src_path).await?;
    let exe_path = fc.get_exe_7zip_path();
    let final_exe = if exe_path.is_empty() { "7z" } else { exe_path };
    let mut child = Command::new(final_exe)
        .arg("a")
        .arg("dummy")
        .arg(format!("-t{}", format.type_flag()))
        .arg("-si")
        .arg("-so")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    let mut stdin = child.stdin.take().ok_or_else(|| anyhow::anyhow!("Failed to open stdin"))?;
    let stdout = child.stdout.take().ok_or_else(|| anyhow::anyhow!("Failed to open stdout"))?;
    tokio::spawn(async move {
        use futures::StreamExt;
        while let Some(chunk) = vfs_stream.next().await {
            if let Ok(bytes) = chunk
                && stdin.write_all(&bytes).await.is_err()
            {
                break;
            }
        }
        let _ = stdin.shutdown().await;
        let _ = child.wait().await;
    });
    Ok(stdout)
}
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct ArchiveEntry {
    pub path: String,
    pub is_dir: bool,
    pub size: u64,
    pub modified: Option<String>,
}
/// List archive contents (Cross-backend via Pipe)
pub async fn list_archive_contents(engine: &dyn VfsStorage, archive_path: &str, password: Option<&str>) -> Result<Vec<ArchiveEntry>, anyhow::Error> {
    let cfg = crate::config::get_vfs_hub_config().await;
    let fc = cfg.get_file_compress();
    if !yh_config_infra::config_require_clone!(fc.enable_archive_browser, "vfs_storage_hub", "file_compress.enable_archive_browser") {
        return Err(anyhow::anyhow!("Archive browsing is disabled"));
    }
    let format = CompressionFormat::from_path(archive_path).ok_or_else(|| anyhow::anyhow!("Unsupported format"))?;
    let allowed = yh_config_infra::config_require!(fc.decompression_formats, "vfs_storage_hub", "file_compress.decompression_formats");
    if !is_extension_allowed(archive_path, allowed) {
        return Err(anyhow::anyhow!("Archive browsing not supported for format: {}", archive_path));
    }
    let exe_path = fc.get_exe_7zip_path();
    let use_7z = !exe_path.is_empty() || format == CompressionFormat::SevenZip;
    if use_7z {
        if exe_path.is_empty() && format == CompressionFormat::SevenZip {
            return Err(anyhow::anyhow!("7z format requires 7z executable to be configured."));
        }
        let (mut stream, _) = engine.read_stream(archive_path).await?;
        let mut child = Command::new(if exe_path.is_empty() { "7z" } else { exe_path })
            .arg("l")
            .arg("-slt")
            .arg("-si")
            .args(if let Some(pwd) = password { vec![format!("-p{}", pwd)] } else { vec![] })
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        let mut stdin = child.stdin.take().ok_or_else(|| anyhow::anyhow!("Failed to open stdin"))?;
        tokio::spawn(async move {
            use futures::StreamExt;
            while let Some(chunk) = stream.next().await {
                if let Ok(b) = chunk
                    && stdin.write_all(&b).await.is_err()
                {
                    break;
                }
            }
            let _ = stdin.shutdown().await;
        });
        let output = child.wait_with_output().await?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut entries = Vec::new();
        let mut current_entry = ArchiveEntry::default();
        let mut has_data = false;
        for line in stdout.lines() {
            if line.is_empty() {
                if has_data && !current_entry.path.is_empty() {
                    entries.push(std::mem::take(&mut current_entry));
                }
                has_data = false;
                continue;
            }
            if let Some((key, value)) = line.split_once(" = ") {
                has_data = true;
                match key.trim() {
                    "Path" => current_entry.path = value.trim().replace('\\', "/"),
                    "Size" => {
                        current_entry.size = match value.trim().parse() {
                            Ok(parsed) => parsed,
                            Err(err) => {
                                yh_console_log::yhlog("warn", &format!("Failed to parse archive entry size '{}': {}", value.trim(), err));
                                0
                            }
                        }
                    }
                    "Attributes" => current_entry.is_dir = value.contains('D'),
                    "Modified" => current_entry.modified = Some(value.trim().to_string()),
                    _ => {}
                }
            }
        }
        Ok(entries)
    } else {
        if format != CompressionFormat::Zip {
            return Err(anyhow::anyhow!("Native browsing only supports ZIP format without 7z."));
        }
        let (data, _) = engine.read(archive_path).await?;
        let cursor = std::io::Cursor::new(data);
        let mut archive = ZipArchive::new(cursor)?;
        let mut entries = Vec::new();
        for i in 0..archive.len() {
            let file = if let Some(pwd) = password { archive.by_index_decrypt(i, pwd.as_bytes())? } else { archive.by_index(i)? };
            entries.push(ArchiveEntry {
                path: file.name().to_string(),
                is_dir: file.is_dir(),
                size: file.size(),
                modified: None,
            });
        }
        Ok(entries)
    }
}
/// Browse archive content without extraction
pub async fn list_archive_content(engine: Arc<dyn VfsStorage>, src_path: String, _user_id: String) -> Result<Vec<ArchiveEntry>, anyhow::Error> {
    list_archive_contents(&*engine, &src_path, None).await
}
/// Extract single file from archive as stream
pub async fn extract_archive_file(engine: &dyn VfsStorage, archive_path: &str, file_in_archive: &str, password: Option<&str>) -> Result<Pin<Box<dyn tokio::io::AsyncRead + Send + Sync>>, anyhow::Error> {
    let cfg = crate::config::get_vfs_hub_config().await;
    let fc = cfg.get_file_compress();
    let format = CompressionFormat::from_path(archive_path).ok_or_else(|| anyhow::anyhow!("Unsupported format"))?;
    let exe_path = fc.get_exe_7zip_path();
    let use_7z = !exe_path.is_empty() || format == CompressionFormat::SevenZip;
    if use_7z {
        if exe_path.is_empty() && format == CompressionFormat::SevenZip {
            return Err(anyhow::anyhow!("7z format requires 7z executable to be configured."));
        }
        let (mut stream, _) = engine.read_stream(archive_path).await?;
        let mut child = Command::new(if exe_path.is_empty() { "7z" } else { exe_path })
            .arg("e")
            .arg("-si")
            .arg("-so")
            .arg("-y")
            .args(if let Some(pwd) = password { vec![format!("-p{}", pwd)] } else { vec![] })
            .arg(file_in_archive)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        let mut stdin = child.stdin.take().ok_or_else(|| anyhow::anyhow!("Failed to open stdin"))?;
        let stdout = child.stdout.take().ok_or_else(|| anyhow::anyhow!("Failed to open stdout"))?;
        tokio::spawn(async move {
            use futures::StreamExt;
            while let Some(chunk) = stream.next().await {
                if let Ok(b) = chunk
                    && stdin.write_all(&b).await.is_err()
                {
                    break;
                }
            }
            let _ = stdin.shutdown().await;
            let _ = child.wait().await;
        });
        Ok(Box::pin(stdout))
    } else {
        if format != CompressionFormat::Zip {
            return Err(anyhow::anyhow!("Native extraction only supports ZIP format without 7z."));
        }
        let (data, _) = engine.read(archive_path).await?;
        let cursor = std::io::Cursor::new(data);
        let mut archive = ZipArchive::new(cursor)?;
        let mut file = if let Some(pwd) = password { archive.by_name_decrypt(file_in_archive, pwd.as_bytes())? } else { archive.by_name(file_in_archive)? };
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        Ok(Box::pin(std::io::Cursor::new(buffer)))
    }
}
// --- Trait Compatible Wrappers ---
pub async fn compress_compat(engine: &dyn VfsStorage, source_path: &str, target_path: &str, user_id: &str, password: Option<&str>, encrypt_filenames: bool) -> VfsResult<VfsFileInfo> {
    let format = if let Some(parsed) = CompressionFormat::from_path(target_path) {
        parsed
    } else {
        yh_console_log::yhlog("warn", &format!("Unknown target archive extension for '{}', fallback to zip format", target_path));
        CompressionFormat::Zip
    };
    let cfg = crate::config::get_vfs_hub_config().await;
    let fc = cfg.get_file_compress();
    let opts = CompressionOptions {
        format,
        password: password.map(|s| s.to_string()),
        encrypt_filenames,
        compression_level: yh_config_infra::config_require_clone!(fc.compression_max_level, "vfs_storage_hub", "file_compress.compression_max_level"),
    };
    compress_task(engine, source_path, target_path, user_id, &opts, false).await.map_err(|e| VfsError::Internal(e.to_string()))
}
pub async fn decompress_compat(engine: &dyn VfsStorage, archive_path: &str, target_dir: &str, user_id: &str, overwrite: bool, password: Option<&str>) -> VfsResult<VfsFileInfo> {
    let opts = DecompressionOptions { overwrite, password: password.map(|s| s.to_string()) };
    decompress_task(engine, archive_path, target_dir, user_id, &opts, false).await.map_err(|e| VfsError::Internal(e.to_string()))
}
#[cfg(test)]
mod tests {
    use super::*;

    // =============== CompressionFormat::from_path tests ===============
    #[test]
    fn test_compression_format_from_path() {
        assert_eq!(CompressionFormat::from_path("test.zip"), Some(CompressionFormat::Zip));
        assert_eq!(CompressionFormat::from_path("test.7z"), Some(CompressionFormat::SevenZip));
        assert_eq!(CompressionFormat::from_path("test.tar.gz"), Some(CompressionFormat::TarGz));
        assert_eq!(CompressionFormat::from_path("test.TAR.GZ"), Some(CompressionFormat::TarGz));
        assert_eq!(CompressionFormat::from_path("test.txt"), None);
    }

    #[test]
    fn test_compression_format_from_path_case_insensitive() {
        assert_eq!(CompressionFormat::from_path("FILE.ZIP"), Some(CompressionFormat::Zip));
        assert_eq!(CompressionFormat::from_path("FILE.7Z"), Some(CompressionFormat::SevenZip));
        assert_eq!(CompressionFormat::from_path("FILE.TAR"), Some(CompressionFormat::Tar));
        assert_eq!(CompressionFormat::from_path("FILE.GZ"), Some(CompressionFormat::Gzip));
        assert_eq!(CompressionFormat::from_path("FILE.TAR.GZ"), Some(CompressionFormat::TarGz));
    }

    #[test]
    fn test_compression_format_from_path_with_directory() {
        assert_eq!(CompressionFormat::from_path("/path/to/archive.zip"), Some(CompressionFormat::Zip));
        assert_eq!(CompressionFormat::from_path("C:\\Users\\test\\file.7z"), Some(CompressionFormat::SevenZip));
        assert_eq!(CompressionFormat::from_path("/data/backup.tar.gz"), Some(CompressionFormat::TarGz));
    }

    #[test]
    fn test_compression_format_from_path_edge_cases() {
        // Empty string
        assert_eq!(CompressionFormat::from_path(""), None);
        // No extension
        assert_eq!(CompressionFormat::from_path("filename"), None);
        // Dot at end
        assert_eq!(CompressionFormat::from_path("filename."), None);
        // Multiple dots but not tar.gz
        assert_eq!(CompressionFormat::from_path("file.name.txt"), None);
    }

    // =============== CompressionFormat::extension tests ===============
    #[test]
    fn test_compression_format_extension() {
        assert_eq!(CompressionFormat::Zip.extension(), "zip");
        assert_eq!(CompressionFormat::SevenZip.extension(), "7z");
        assert_eq!(CompressionFormat::Tar.extension(), "tar");
        assert_eq!(CompressionFormat::Gzip.extension(), "gz");
        assert_eq!(CompressionFormat::TarGz.extension(), "tar.gz");
    }

    // =============== CompressionFormat::type_flag tests ===============
    #[test]
    fn test_compression_format_type_flag() {
        assert_eq!(CompressionFormat::Zip.type_flag(), "zip");
        assert_eq!(CompressionFormat::SevenZip.type_flag(), "7z");
        assert_eq!(CompressionFormat::Tar.type_flag(), "tar");
        assert_eq!(CompressionFormat::Gzip.type_flag(), "gzip");
        assert_eq!(CompressionFormat::TarGz.type_flag(), "tar.gz");
    }

    // =============== is_extension_allowed tests ===============
    #[test]
    fn test_is_extension_allowed_basic() {
        let allowed: Vec<Arc<str>> = vec!["zip".into(), "7z".into(), "tar.gz".into()];
        assert!(is_extension_allowed("archive.zip", &allowed));
        assert!(is_extension_allowed("archive.7z", &allowed));
        assert!(is_extension_allowed("archive.tar.gz", &allowed));
        assert!(!is_extension_allowed("archive.rar", &allowed));
        assert!(!is_extension_allowed("archive.txt", &allowed));
    }

    #[test]
    fn test_is_extension_allowed_case_insensitive() {
        let allowed: Vec<Arc<str>> = vec!["zip".into()];
        assert!(is_extension_allowed("FILE.ZIP", &allowed));
        assert!(is_extension_allowed("FILE.Zip", &allowed));
        assert!(is_extension_allowed("file.zip", &allowed));
    }

    #[test]
    fn test_is_extension_allowed_with_path() {
        let allowed: Vec<Arc<str>> = vec!["zip".into()];
        assert!(is_extension_allowed("/path/to/archive.zip", &allowed));
        assert!(is_extension_allowed("C:\\Users\\test\\file.zip", &allowed));
    }

    #[test]
    fn test_is_extension_allowed_empty_list() {
        let allowed: Vec<Arc<str>> = vec![];
        assert!(!is_extension_allowed("file.zip", &allowed));
        assert!(!is_extension_allowed("file.7z", &allowed));
    }

    #[test]
    fn test_is_extension_allowed_no_extension() {
        let allowed: Vec<Arc<str>> = vec!["zip".into()];
        assert!(!is_extension_allowed("filename", &allowed));
        assert!(!is_extension_allowed("/path/to/file", &allowed));
    }

    // =============== CompressionOptions tests ===============
    #[test]
    fn test_compression_options_default_values() {
        let opts = CompressionOptions {
            format: CompressionFormat::Zip,
            password: None,
            encrypt_filenames: false,
            compression_level: 5,
        };
        assert_eq!(opts.format, CompressionFormat::Zip);
        assert!(opts.password.is_none());
        assert!(!opts.encrypt_filenames);
        assert_eq!(opts.compression_level, 5);
    }

    // =============== DecompressionOptions tests ===============
    #[test]
    fn test_decompression_options_default() {
        let opts = DecompressionOptions::default();
        assert!(!opts.overwrite);
        assert!(opts.password.is_none());
    }

    #[test]
    fn test_decompression_options_with_password() {
        let opts = DecompressionOptions { overwrite: true, password: Some("secret".to_string()) };
        assert!(opts.overwrite);
        assert_eq!(opts.password, Some("secret".to_string()));
    }

    // =============== ArchiveEntry tests ===============
    #[test]
    fn test_archive_entry_default() {
        let entry = ArchiveEntry::default();
        assert!(entry.path.is_empty());
        assert!(!entry.is_dir);
        assert_eq!(entry.size, 0);
        assert!(entry.modified.is_none());
    }

    // =============== Native compression/decompression tests ===============
    #[test]
    fn test_zip_integrity_native() -> anyhow::Result<()> {
        let tmp_dir = std::env::temp_dir().join(format!("vfs_test_{}", uuid::Uuid::new_v4()));
        let src_dir = tmp_dir.join("src");
        let dst_zip = tmp_dir.join("test.zip");
        let out_dir = tmp_dir.join("out");
        std::fs::create_dir_all(&src_dir)?;
        // Create dummy files
        let file1_content = "Hello VFS Compression";
        let file2_content = "Nested content";
        std::fs::write(src_dir.join("file1.txt"), file1_content)?;
        let sub_dir = src_dir.join("subdir");
        std::fs::create_dir_all(&sub_dir)?;
        std::fs::write(sub_dir.join("file2.txt"), file2_content)?;
        // Compress
        let opts = CompressionOptions {
            format: CompressionFormat::Zip,
            password: None,
            encrypt_filenames: false,
            compression_level: 5,
        };
        compress_zip_native(&src_dir, &dst_zip, &opts)?;
        assert!(dst_zip.exists());
        // Decompress
        std::fs::create_dir_all(&out_dir)?;
        decompress_zip_native(&dst_zip, &out_dir, None)?;
        // Verify contents
        assert_eq!(std::fs::read_to_string(out_dir.join("file1.txt"))?, file1_content);
        assert_eq!(std::fs::read_to_string(out_dir.join("subdir/file2.txt"))?, file2_content);
        // Cleanup
        let _ = std::fs::remove_dir_all(&tmp_dir);
        Ok(())
    }

    #[test]
    fn test_tar_gz_integrity_native() -> anyhow::Result<()> {
        let tmp_dir = std::env::temp_dir().join(format!("vfs_test_tar_{}", uuid::Uuid::new_v4()));
        let src_dir = tmp_dir.join("src");
        let dst_tar = tmp_dir.join("test.tar.gz");
        let out_dir = tmp_dir.join("out");
        std::fs::create_dir_all(&src_dir)?;
        // Create test file
        let content = "TarGz test content";
        std::fs::write(src_dir.join("test.txt"), content)?;
        // Compress
        compress_tar_gz_native(&src_dir, &dst_tar, 5)?;
        assert!(dst_tar.exists());
        // Decompress
        std::fs::create_dir_all(&out_dir)?;
        decompress_tar_gz_native(&dst_tar, &out_dir)?;
        // Verify
        assert_eq!(std::fs::read_to_string(out_dir.join("test.txt"))?, content);
        // Cleanup
        let _ = std::fs::remove_dir_all(&tmp_dir);
        Ok(())
    }

    #[test]
    fn test_gzip_single_file_native() -> anyhow::Result<()> {
        let tmp_dir = std::env::temp_dir().join(format!("vfs_test_gz_{}", uuid::Uuid::new_v4()));
        let src_file = tmp_dir.join("test.txt");
        let dst_gz = tmp_dir.join("test.txt.gz");
        let out_file = tmp_dir.join("output.txt");
        std::fs::create_dir_all(&tmp_dir)?;
        // Create test file
        let content = "Gzip single file test";
        std::fs::write(&src_file, content)?;
        // Compress
        compress_gz_native(&src_file, &dst_gz, 5)?;
        assert!(dst_gz.exists());
        // Decompress
        decompress_gz_native(&dst_gz, &out_file)?;
        // Verify
        assert_eq!(std::fs::read_to_string(&out_file)?, content);
        // Cleanup
        let _ = std::fs::remove_dir_all(&tmp_dir);
        Ok(())
    }
}
