#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub(crate) queue: &'static str,
    pub(crate) retries: usize,
    pub(crate) max_run_time: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            queue: "default",
            retries: 0,
            max_run_time: 30,
        }
    }
}
