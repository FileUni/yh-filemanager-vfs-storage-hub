use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Default)]
pub struct VfsMetricsSnapshot {
    pub read_cache_hits: u64,
    pub read_cache_misses: u64,
    pub read_cache_puts: u64,
    pub read_cache_put_bytes: u64,
    pub write_cache_enqueues: u64,
    pub write_cache_bypasses: u64,
    pub write_cache_pending_reads: u64,
    pub write_cache_flush_success: u64,
    pub write_cache_flush_failures: u64,
    pub write_cache_abnormal_spills: u64,
    pub index_sync_spawned: u64,
    pub index_sync_skipped_debounce: u64,
    pub index_sync_skipped_inflight: u64,
    pub index_sync_completed: u64,
    pub index_sync_failed: u64,
    pub index_sync_rows: u64,
    pub index_sync_chunks: u64,
    pub quota_sync_scheduled: u64,
    pub quota_sync_success: u64,
    pub quota_sync_failed: u64,
}

#[derive(Debug, Default)]
pub struct VfsMetrics {
    read_cache_hits: AtomicU64,
    read_cache_misses: AtomicU64,
    read_cache_puts: AtomicU64,
    read_cache_put_bytes: AtomicU64,
    write_cache_enqueues: AtomicU64,
    write_cache_bypasses: AtomicU64,
    write_cache_pending_reads: AtomicU64,
    write_cache_flush_success: AtomicU64,
    write_cache_flush_failures: AtomicU64,
    write_cache_abnormal_spills: AtomicU64,
    index_sync_spawned: AtomicU64,
    index_sync_skipped_debounce: AtomicU64,
    index_sync_skipped_inflight: AtomicU64,
    index_sync_completed: AtomicU64,
    index_sync_failed: AtomicU64,
    index_sync_rows: AtomicU64,
    index_sync_chunks: AtomicU64,
    quota_sync_scheduled: AtomicU64,
    quota_sync_success: AtomicU64,
    quota_sync_failed: AtomicU64,
}

static GLOBAL_VFS_METRICS: Lazy<VfsMetrics> = Lazy::new(VfsMetrics::default);

#[inline]
fn inc(counter: &AtomicU64, value: u64) {
    counter.fetch_add(value, Ordering::Relaxed);
}

pub fn global_vfs_metrics() -> &'static VfsMetrics {
    &GLOBAL_VFS_METRICS
}

pub fn snapshot_global_vfs_metrics() -> VfsMetricsSnapshot {
    GLOBAL_VFS_METRICS.snapshot()
}

impl VfsMetrics {
    pub fn record_read_cache_hit(&self) {
        inc(&self.read_cache_hits, 1);
    }
    pub fn record_read_cache_miss(&self) {
        inc(&self.read_cache_misses, 1);
    }
    pub fn record_read_cache_put(&self, bytes: u64) {
        inc(&self.read_cache_puts, 1);
        inc(&self.read_cache_put_bytes, bytes);
    }
    pub fn record_write_cache_enqueue(&self) {
        inc(&self.write_cache_enqueues, 1);
    }
    pub fn record_write_cache_bypass(&self) {
        inc(&self.write_cache_bypasses, 1);
    }
    pub fn record_write_cache_pending_read(&self) {
        inc(&self.write_cache_pending_reads, 1);
    }
    pub fn record_write_cache_flush_success(&self) {
        inc(&self.write_cache_flush_success, 1);
    }
    pub fn record_write_cache_flush_failure(&self) {
        inc(&self.write_cache_flush_failures, 1);
    }
    pub fn record_write_cache_abnormal_spill(&self) {
        inc(&self.write_cache_abnormal_spills, 1);
    }
    pub fn record_index_sync_spawned(&self) {
        inc(&self.index_sync_spawned, 1);
    }
    pub fn record_index_sync_skipped_debounce(&self) {
        inc(&self.index_sync_skipped_debounce, 1);
    }
    pub fn record_index_sync_skipped_inflight(&self) {
        inc(&self.index_sync_skipped_inflight, 1);
    }
    pub fn record_index_sync_completed(&self, rows: u64, chunks: u64) {
        inc(&self.index_sync_completed, 1);
        inc(&self.index_sync_rows, rows);
        inc(&self.index_sync_chunks, chunks);
    }
    pub fn record_index_sync_failed(&self) {
        inc(&self.index_sync_failed, 1);
    }
    pub fn record_quota_sync_scheduled(&self) {
        inc(&self.quota_sync_scheduled, 1);
    }
    pub fn record_quota_sync_success(&self) {
        inc(&self.quota_sync_success, 1);
    }
    pub fn record_quota_sync_failed(&self) {
        inc(&self.quota_sync_failed, 1);
    }
    pub fn snapshot(&self) -> VfsMetricsSnapshot {
        VfsMetricsSnapshot {
            read_cache_hits: self.read_cache_hits.load(Ordering::Relaxed),
            read_cache_misses: self.read_cache_misses.load(Ordering::Relaxed),
            read_cache_puts: self.read_cache_puts.load(Ordering::Relaxed),
            read_cache_put_bytes: self.read_cache_put_bytes.load(Ordering::Relaxed),
            write_cache_enqueues: self.write_cache_enqueues.load(Ordering::Relaxed),
            write_cache_bypasses: self.write_cache_bypasses.load(Ordering::Relaxed),
            write_cache_pending_reads: self.write_cache_pending_reads.load(Ordering::Relaxed),
            write_cache_flush_success: self.write_cache_flush_success.load(Ordering::Relaxed),
            write_cache_flush_failures: self.write_cache_flush_failures.load(Ordering::Relaxed),
            write_cache_abnormal_spills: self.write_cache_abnormal_spills.load(Ordering::Relaxed),
            index_sync_spawned: self.index_sync_spawned.load(Ordering::Relaxed),
            index_sync_skipped_debounce: self.index_sync_skipped_debounce.load(Ordering::Relaxed),
            index_sync_skipped_inflight: self.index_sync_skipped_inflight.load(Ordering::Relaxed),
            index_sync_completed: self.index_sync_completed.load(Ordering::Relaxed),
            index_sync_failed: self.index_sync_failed.load(Ordering::Relaxed),
            index_sync_rows: self.index_sync_rows.load(Ordering::Relaxed),
            index_sync_chunks: self.index_sync_chunks.load(Ordering::Relaxed),
            quota_sync_scheduled: self.quota_sync_scheduled.load(Ordering::Relaxed),
            quota_sync_success: self.quota_sync_success.load(Ordering::Relaxed),
            quota_sync_failed: self.quota_sync_failed.load(Ordering::Relaxed),
        }
    }
}
