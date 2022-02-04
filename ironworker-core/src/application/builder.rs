use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde::Serialize;
use tokio::sync::Notify;
use uuid::Uuid;

use crate::config::IronworkerConfig;
use crate::task::{Config, PerformableTask};
use crate::{Broker, IronworkerApplication};
use crate::{IronworkerMiddleware, Message, Task};

use super::shared::SharedData;

#[allow(missing_debug_implementations)]
pub struct IronworkerApplicationBuilder<B: Broker + 'static> {
    id: String,
    broker: Option<B>,
    tasks: HashMap<&'static str, (Box<dyn PerformableTask>, Config)>,
    middleware: Vec<Box<dyn IronworkerMiddleware>>,
    queues: HashSet<&'static str>,
}

impl<B: Broker + 'static> IronworkerApplicationBuilder<B> {
    #[must_use = "An application must be built in order to use"]
    pub fn register_task<T: Serialize + Send + Into<Message<T>> + 'static, Tsk: Task<T> + Send>(
        mut self,
        task: Tsk,
    ) -> IronworkerApplicationBuilder<B> {
        let task_config = Task::config(&task);
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
        Self {
            id: Uuid::new_v4().to_string(),
            broker: None,
            middleware: Default::default(),
            tasks: HashMap::new(),
            queues: HashSet::new(),
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
}
