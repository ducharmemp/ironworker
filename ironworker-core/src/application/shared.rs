use std::collections::HashMap;

use state::Container;

use crate::{Broker, IronworkerMiddleware, Task};

pub(crate) struct SharedData<B: Broker> {
    pub(crate) broker: B,
    pub(crate) tasks: HashMap<&'static str, Box<dyn Task>>,
    pub(crate) state: Container![Send + Sync],
    pub(crate) middleware: Vec<Box<dyn IronworkerMiddleware>>,
}
