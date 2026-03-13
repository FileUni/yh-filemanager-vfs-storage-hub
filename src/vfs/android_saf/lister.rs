use opendal::raw::oio;
use opendal::Result;

#[derive(Debug)]
pub struct AndroidSafLister {
    entries: Vec<oio::Entry>,
    idx: usize,
}

impl AndroidSafLister {
    pub fn new(entries: Vec<oio::Entry>) -> Self {
        Self { entries, idx: 0 }
    }
}

impl oio::List for AndroidSafLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        if self.idx >= self.entries.len() {
            return Ok(None);
        }
        let entry = self.entries[self.idx].clone();
        self.idx += 1;
        Ok(Some(entry))
    }
}
