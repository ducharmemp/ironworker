#[derive(Debug)]
pub struct Config {
    pub(crate) queue: &'static str,
    pub(crate) retries: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            queue: "default",
            retries: 0,
        }
    }
}
