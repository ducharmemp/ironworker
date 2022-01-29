use std::collections::HashMap;

use crate::{
    task::{Config, PerformableTask},
    Broker, IronworkerMiddleware,
};

pub(crate) struct SharedData<B: Broker> {
    pub(crate) broker: B,
    pub(crate) tasks: HashMap<&'static str, (Box<dyn PerformableTask>, Config)>,
    pub(crate) middleware: Vec<Box<dyn IronworkerMiddleware>>,
}
