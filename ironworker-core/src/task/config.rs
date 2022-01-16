use std::any::TypeId;
use std::collections::{HashMap, HashSet};

use super::error::ErrorRetryConfiguration;

#[derive(Debug)]
pub struct Config {
    pub(crate) queue: &'static str,
    pub(crate) retries: usize,
    pub(crate) max_run_time: u64,
    pub(crate) retry_on: HashMap<TypeId, ErrorRetryConfiguration>,
    pub(crate) discard_on: HashSet<TypeId>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            queue: "default",
            retries: 0,
            max_run_time: 30,
            retry_on: HashMap::new(),
            discard_on: HashSet::new(),
        }
    }
}
