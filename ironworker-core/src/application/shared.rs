use std::collections::HashMap;

use tokio::sync::Mutex;

use crate::{task::PerformableTask, Broker, IronworkerMiddleware};

pub(crate) struct SharedData<B: Broker> {
    pub(crate) broker: B,
    pub(crate) tasks: Mutex<HashMap<&'static str, Box<dyn PerformableTask>>>,
    pub(crate) middleware: Vec<Box<dyn IronworkerMiddleware>>,
}
