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
