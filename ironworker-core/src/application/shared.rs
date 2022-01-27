use std::collections::HashMap;

use tokio::sync::Mutex;

use crate::{task::{PerformableTask, Config}, Broker, IronworkerMiddleware};

pub(crate) struct SharedData<B: Broker> {
    pub(crate) broker: B,
    pub(crate) tasks: Mutex<HashMap<&'static str, (Box<dyn PerformableTask>, Config)>>,
    pub(crate) middleware: Vec<Box<dyn IronworkerMiddleware>>,
}
