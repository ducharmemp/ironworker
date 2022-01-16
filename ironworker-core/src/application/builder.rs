use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use state::Container;
use tokio::sync::Notify;
use uuid::Uuid;

use crate::config::IronworkerConfig;
use crate::IronworkerMiddleware;
use crate::{Broker, IronworkerApplication, Task};

use super::shared::SharedData;

pub struct IronworkerApplicationBuilder<B: Broker + 'static> {
    id: String,
    broker: Option<B>,
    tasks: HashMap<&'static str, Box<dyn Task>>,
    middleware: Vec<Box<dyn IronworkerMiddleware>>,
    queues: HashSet<&'static str>,
    state: Container![Send + Sync],
}

impl<B: Broker + 'static> IronworkerApplicationBuilder<B> {
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

    pub fn manage<T>(self, state: T) -> IronworkerApplicationBuilder<B>
    where
        T: Send + Sync + 'static,
    {
        // let type_name = std::any::type_name::<T>();
        if !self.state.set(state) {
            // error!("state for type '{}' is already being managed", type_name);
            panic!("aborting due to duplicately managed state");
        }

        self
    }

    pub fn register_middleware<T: IronworkerMiddleware>(
        mut self,
        middleware: T,
    ) -> IronworkerApplicationBuilder<B> {
        self.middleware.push(Box::new(middleware));
        self
    }

    pub fn build(self) -> IronworkerApplication<B> {
        IronworkerApplication {
            id: self.id,
            config: IronworkerConfig::new().expect("Could not construct Ironworker configuration"),
            notify_shutdown: Notify::new(),
            queues: self.queues,
            shared_data: Arc::new(SharedData {
                broker: self.broker.expect("Expected a broker to be registered"),
                tasks: self.tasks,
                state: self.state,
                middleware: self.middleware,
            }),
        }
    }
}

impl<B: Broker + 'static> Default for IronworkerApplicationBuilder<B> {
    fn default() -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            broker: None,
            middleware: Default::default(),
            tasks: HashMap::new(),
            queues: HashSet::new(),
            state: Default::default(),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{broker::InProcessBroker, test::TestEnum, IntoTask, Message};

    use super::*;

    #[tokio::test]
    async fn register_task_builds_queue() {
        fn test_task(_m: Message<u32>) -> Result<(), TestEnum> {
            Ok(())
        }

        let builder = IronworkerApplicationBuilder::default()
            .broker(InProcessBroker::default())
            .register_task(test_task.task());
        assert_eq!(builder.queues, HashSet::from_iter(["default"]));
    }

    #[tokio::test]
    async fn register_task_boxes_task() {
        fn test_task(_m: Message<u32>) -> Result<(), TestEnum> {
            Ok(())
        }

        let builder = IronworkerApplicationBuilder::default()
            .broker(InProcessBroker::default())
            .register_task(test_task.task());
        assert_eq!(builder.tasks.values().len(), 1);
    }

    #[tokio::test]
    async fn manage_registers_state() {
        let builder = IronworkerApplicationBuilder::default()
            .broker(InProcessBroker::default())
            .manage(1_u32);
        assert_eq!(builder.state.try_get::<u32>().unwrap(), &1);
    }
}
