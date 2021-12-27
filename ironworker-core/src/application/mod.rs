mod builder;
mod worker;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::future::join_all;
use serde::Serialize;

use crate::config::IronworkerConfig;
use crate::{Broker, Message, QueueState, SerializableMessage, Task, WorkerState};
pub use builder::IronworkerApplicationBuilder;
use worker::IronWorker;

pub struct IronworkerApplication<B: Broker> {
    pub(crate) id: String,
    pub(crate) broker: Arc<B>,
    pub(crate) tasks: Arc<HashMap<&'static str, Box<dyn Task>>>,
    pub(crate) queues: Arc<HashSet<&'static str>>,
    pub(crate) config: IronworkerConfig,
}

impl<B: Broker + Sync + Send + 'static> IronworkerApplication<B> {
    pub async fn list_workers(&self) -> Vec<WorkerState> {
        self.broker.list_workers().await
    }

    pub async fn list_queues(&self) -> Vec<QueueState> {
        self.broker.list_queues().await
    }

    pub async fn enqueue<P: Serialize + Send + Into<Message<P>>>(&self, task: &str, payload: P) {
        let message: Message<P> = payload.into();
        let serializable = SerializableMessage::from_message(task, message);
        let task = self.tasks.get(task).unwrap();
        let task_config = task.config();
        self.broker.enqueue(task_config.queue, serializable).await;
    }

    fn spawn_consumer_worker(
        &self,
        index: usize,
        queue: &'static str,
    ) -> tokio::task::JoinHandle<()> {
        let broker = self.broker.clone();
        let id = format!("{}-{}-{}", self.id.clone(), queue, index);
        let tasks = self.tasks.clone();

        tokio::task::spawn(async move { IronWorker::new(id, broker, tasks, queue).work().await })
    }

    pub async fn run(&self) {
        let handles: Vec<_> = self
            .queues
            .iter()
            .flat_map(|queue| {
                let default_count = self.config.concurrency;
                let queue_count = self
                    .config
                    .queues
                    .get(&queue.to_string())
                    .map(|queue| queue.concurrency)
                    .unwrap_or(default_count);

                (0..queue_count).map(|count| self.spawn_consumer_worker(count, queue))
            })
            .collect();

        join_all(handles).await;
    }
}

#[cfg(test)]
mod test {
    #[tokio::test]
    async fn enqueuing_message_goes_to_broker() {}
}
