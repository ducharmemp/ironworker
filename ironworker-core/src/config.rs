use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize)]
pub(crate) struct QueueConfig {
    pub concurrency: usize,
}

#[derive(Deserialize)]
#[serde(default)]
pub(crate) struct IronworkerConfig {
    pub concurrency: usize,
    pub queues: HashMap<String, QueueConfig>,
}

impl Default for IronworkerConfig {
    fn default() -> Self {
        Self {
            concurrency: 1,
            queues: Default::default(),
        }
    }
}

impl IronworkerConfig {
    pub fn new() -> Result<Self, ConfigError> {
        let mut s = Config::default();

        s.merge(File::with_name("Ironworker").required(false))?;
        s.merge(Environment::with_prefix("ironworker"))?;

        s.try_into()
    }
}
