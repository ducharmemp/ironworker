use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Copy, Default)]
pub struct Config {
    pub(crate) queue: Option<&'static str>,
    pub(crate) retries: Option<usize>,
    pub(crate) timeout: Option<u64>,
    pub(crate) at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy)]
pub struct UnwrappedConfig {
    pub(crate) queue: &'static str,
    pub(crate) retries: usize,
    pub(crate) timeout: u64,
    pub(crate) at: Option<DateTime<Utc>>,
}

impl Config {
    pub(crate) fn merge(&self, other: Config) -> Config {
        Config {
            queue: self.queue.or(other.queue),
            retries: self.retries.or(other.retries),
            timeout: self.timeout.or(other.timeout),
            at: self.at.or(other.at),
        }
    }

    pub(crate) fn unwrap(self) -> UnwrappedConfig {
        UnwrappedConfig {
            queue: self.queue.unwrap_or("default"),
            retries: self.retries.unwrap_or(0),
            timeout: self.timeout.unwrap_or(30),
            at: self.at,
        }
    }
}
