use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use uuid::Uuid;

use crate::config::IronworkerConfig;
use crate::{Broker, IronworkerApplication, Task};

pub struct IronworkerApplicationBuilder<B: Broker + Send + Sync + 'static> {
    id: String,
    broker: Option<B>,
    tasks: HashMap<&'static str, Box<dyn Task>>,
    queues: HashSet<&'static str>,
}

impl<B: Broker + Send + Sync + 'static> IronworkerApplicationBuilder<B> {
    #[must_use = "An application must be built in order to use"]
    pub fn register_task<T: Task + Send>(mut self, task: T) -> IronworkerApplicationBuilder<B> {
        let task_config = task.config();
        self.queues.insert(task_config.queue);

        self.tasks
            .entry(task.name())
            .or_insert_with(|| Box::new(task));
        self
    }

    #[must_use = "An application must be built in order to use"]
    pub fn broker(mut self, broker: B) -> IronworkerApplicationBuilder<B> {
        self.broker = Some(broker);
        self
    }

    pub fn build(self) -> IronworkerApplication<B> {
        IronworkerApplication {
            id: self.id,
            config: IronworkerConfig::new().expect("Could not construct Ironworker configuration"),
            broker: Arc::new(self.broker.expect("Expected a broker to be registered")),
            tasks: Arc::new(self.tasks),
            queues: Arc::new(self.queues),
        }
    }
}

impl<B: Broker + Send + Sync + 'static> Default for IronworkerApplicationBuilder<B> {
    fn default() -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            broker: None,
            tasks: HashMap::new(),
            queues: HashSet::new(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::error::Error;

    use crate::{broker::InProcessBroker, IntoTask, Message};

    use super::*;

    #[tokio::test]
    async fn register_task_builds_queue() {
        fn test_task(_m: Message<u32>) -> Result<(), Box<dyn Error + Send>> {
            Ok(())
        }

        let builder = IronworkerApplicationBuilder::default()
            .broker(InProcessBroker::default())
            .register_task(test_task.task());
        assert_eq!(builder.queues, HashSet::from_iter(["default"]));
    }

    #[tokio::test]
    async fn register_task_boxes_task() {
        fn test_task(_m: Message<u32>) -> Result<(), Box<dyn Error + Send>> {
            Ok(())
        }

        let builder = IronworkerApplicationBuilder::default()
            .broker(InProcessBroker::default())
            .register_task(test_task.task());
        assert_eq!(builder.tasks.values().len(), 1);
    }
}
