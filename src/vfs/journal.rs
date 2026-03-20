use once_cell::sync::OnceCell;
use std::sync::Arc;

/// VFS Journal Record Event
pub struct VfsJournalEvent<'a> {
    pub user_id: &'a str,
    pub action: &'a str,
    pub src: &'a str,
    pub dst: Option<&'a str>,
    pub success: bool,
    pub error: Option<String>,
}
/// VFS Journal Recorder Observer Interface
#[async_trait::async_trait]
pub trait VfsJournalRecorder: Send + Sync {
    async fn log_event(&self, event: VfsJournalEvent<'_>);
}

static GLOBAL_CACHE_JOURNAL_RECORDER: OnceCell<Arc<dyn VfsJournalRecorder>> = OnceCell::new();

pub fn set_global_cache_journal_recorder(recorder: Arc<dyn VfsJournalRecorder>) {
    let _ = GLOBAL_CACHE_JOURNAL_RECORDER.set(recorder);
}

pub fn get_global_cache_journal_recorder() -> Option<&'static Arc<dyn VfsJournalRecorder>> {
    GLOBAL_CACHE_JOURNAL_RECORDER.get()
}
