use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct QueueConfig {
    pub(crate) concurrency: usize,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(default)]
pub(crate) struct IronworkerConfig {
    pub(crate) concurrency: usize,
    pub(crate) queues: HashMap<String, QueueConfig>,
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
    pub(crate) fn new() -> Result<Self, ConfigError> {
        let mut s = Config::default();

        s.merge(File::with_name("Ironworker").required(false))?;
        s.merge(Environment::with_prefix("ironworker"))?;

        s.try_into()
    }
}
