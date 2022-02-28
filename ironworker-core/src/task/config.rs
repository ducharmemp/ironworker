use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Copy, Default)]
pub struct Config {
    pub queue: Option<&'static str>,
    pub retries: Option<u64>,
    pub timeout: Option<u64>,
    pub at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy)]
pub struct UnwrappedConfig {
    pub queue: &'static str,
    pub retries: u64,
    pub timeout: u64,
    pub at: Option<DateTime<Utc>>,
}

impl Config {
    pub fn merge(&self, other: Config) -> Config {
        Config {
            queue: self.queue.or(other.queue),
            retries: self.retries.or(other.retries),
            timeout: self.timeout.or(other.timeout),
            at: self.at.or(other.at),
        }
    }

    pub fn unwrap(self) -> UnwrappedConfig {
        UnwrappedConfig {
            queue: self.queue.unwrap_or("default"),
            retries: self.retries.unwrap_or(0),
            timeout: self.timeout.unwrap_or(30),
            at: self.at,
        }
    }
}
