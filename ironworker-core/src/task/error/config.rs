#[derive(Debug, Default, Clone, Copy)]
pub struct ErrorRetryConfiguration {
    pub(crate) wait: Option<usize>,
    pub(crate) attempts: Option<usize>,
    pub(crate) queue: Option<&'static str>,
}

impl ErrorRetryConfiguration {
    pub fn with_attempts(mut self, attempts: usize) -> Self {
        self.attempts = Some(attempts);
        self
    }

    pub fn with_queue(mut self, queue: &'static str) -> Self {
        self.queue = Some(queue);
        self
    }

    pub fn with_wait(mut self, wait: usize) -> Self {
        self.wait = Some(wait);
        self
    }
}
