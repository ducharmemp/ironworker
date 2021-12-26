use std::{collections::HashMap, sync::Arc};

use uuid::Uuid;

use crate::{IronworkerApplication, Broker, Task};

pub struct IronworkerApplicationBuilder<B: Broker + Send + Sync + 'static> {
    id: String,
    broker: Option<B>,
    tasks: HashMap<&'static str, Box<dyn Task>>,
}

impl<B: Broker + Send + Sync + 'static> IronworkerApplicationBuilder<B> {
    pub fn register_task<T: Task + Send>(mut self, task: T) -> IronworkerApplicationBuilder<B> {
        self.tasks
            .entry(task.name())
            .or_insert_with(|| Box::new(task));
        self
    }

    pub fn broker(mut self, broker: B) -> IronworkerApplicationBuilder<B> {
        self.broker = Some(broker);
        self
    }

    pub fn build(self) -> IronworkerApplication<B> {
        IronworkerApplication {
            id: self.id,
            broker: Arc::new(self.broker.expect("Expected a broker to be registered")),
            tasks: Arc::new(self.tasks)
        }
    }
}

impl<B: Broker + Send + Sync + 'static> Default for IronworkerApplicationBuilder<B> {
    fn default() -> Self {
        Self {
            id: format!("worker:{}", Uuid::new_v4()),
            broker: None,
            tasks: HashMap::new(),
        }
    }
}