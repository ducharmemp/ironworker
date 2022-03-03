use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use ironworker_core::broker::Broker;
use ironworker_core::message::Message;
use ironworker_core::middleware::IronworkerMiddleware;
use ironworker_core::task::{Config, PerformableTask, Task};
use serde::Serialize;
use tokio::sync::Notify;
use uuid::Uuid;

use crate::application::config::IronworkerConfig;

use super::shared::SharedData;
use super::IronworkerApplication;

#[allow(missing_debug_implementations)]
pub struct IronworkerApplicationBuilder<B: Broker + 'static> {
    id: String,
    name: String,
    broker: Option<B>,
    tasks: HashMap<&'static str, (Box<dyn PerformableTask>, Config)>,
    middleware: Vec<Box<dyn IronworkerMiddleware>>,
    queues: HashSet<&'static str>,
}

impl<B: Broker + 'static> IronworkerApplicationBuilder<B> {
    /// Registers a task for later use when processing messages.
    #[must_use = "An application must be built in order to use"]
    pub fn register_task<T: Serialize + Send + Into<Message<T>> + 'static, Tsk: Task<T> + Send>(
        mut self,
        task: Tsk,
    ) -> IronworkerApplicationBuilder<B> {
        let task_config = task.config();
        let task_config = task_config.unwrap();
        self.queues.insert(task_config.queue);

        self.tasks.entry(task.name()).or_insert_with(|| {
            let config = task.config();
            let performable = Box::new(task.into_performable_task()) as Box<_>;
            (performable, config)
        });
        self
    }

    #[must_use = "An application must be built in order to use"]
    pub fn broker(mut self, broker: B) -> IronworkerApplicationBuilder<B> {
        self.broker = Some(broker);
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
        #[allow(clippy::expect_used)]
        IronworkerApplication {
            id: self.id,
            name: self.name,
            config: IronworkerConfig::new().expect("Could not construct Ironworker configuration"),
            notify_shutdown: Notify::new(),
            queues: self.queues,
            shared_data: Arc::new(SharedData {
                broker: self.broker.expect("Expected a broker to be registered"),
                tasks: self.tasks,
                middleware: self.middleware,
            }),
        }
    }
}

impl<B: Broker + 'static> Default for IronworkerApplicationBuilder<B> {
    fn default() -> Self {
        let mut rng = rand::thread_rng();
        let petname = petname::Petnames::default().generate(&mut rng, 3, "-");
        Self {
            id: Uuid::new_v4().to_string(),
            name: petname,
            broker: None,
            middleware: Default::default(),
            tasks: HashMap::new(),
            queues: HashSet::new(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::iter::FromIterator;

    use ironworker_core::message::Message;
    use ironworker_core::task::IntoTask;

    use crate::{process::InProcessBroker, test::TestEnum};

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
}
