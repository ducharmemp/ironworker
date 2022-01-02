#[derive(Debug, Default, Clone)]
pub struct ErrorRetryConfiguration {
    pub(crate) _wait: Option<usize>,
    pub(crate) attempts: Option<usize>,
    pub(crate) queue: Option<&'static str>,
}
