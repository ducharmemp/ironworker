use std::collections::HashMap;

use ironworker_core::task::PerformableTask;
use ironworker_core::{broker::Broker, middleware::IronworkerMiddleware, task::Config};

pub(crate) struct SharedData<B: Broker> {
    pub(crate) broker: B,
    pub(crate) tasks: HashMap<&'static str, (Box<dyn PerformableTask>, Config)>,
    pub(crate) middleware: Vec<Box<dyn IronworkerMiddleware>>,
}

impl<B: Broker> std::fmt::Debug for SharedData<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedData").finish()
    }
}
