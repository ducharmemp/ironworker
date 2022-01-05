mod builder;
mod worker;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::future::join_all;
use serde::Serialize;
use state::Container;
use tokio::select;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::broadcast::channel;
use tracing::debug;

use crate::config::IronworkerConfig;
use crate::{Broker, Message, QueueState, SerializableMessage, Task, WorkerState};
pub use builder::IronworkerApplicationBuilder;
use worker::IronWorkerPool;

pub struct IronworkerApplication<B: Broker> {
    pub(crate) id: String,
    pub(crate) broker: Arc<B>,
    pub(crate) tasks: Arc<HashMap<&'static str, Box<dyn Task>>>,
    pub(crate) queues: Arc<HashSet<&'static str>>,
    pub(crate) config: IronworkerConfig,
    pub(crate) state: Arc<Container![Send + Sync]>,
}

impl<B: Broker + Sync + Send + 'static> IronworkerApplication<B> {
    pub fn manage<T>(&self, state: T)
    where
        T: Send + Sync + 'static,
    {
        // let type_name = std::any::type_name::<T>();
        if !self.state.set(state) {
            // error!("state for type '{}' is already being managed", type_name);
            panic!("aborting due to duplicately managed state");
        }
    }

    pub async fn list_workers(&self) -> Vec<WorkerState> {
        self.broker.list_workers().await
    }

    pub async fn list_queues(&self) -> Vec<QueueState> {
        self.broker.list_queues().await
    }

    pub async fn enqueue<P: Serialize + Send + Into<Message<P>>>(&self, task: &str, payload: P) {
        let message: Message<P> = payload.into();
        let serializable = SerializableMessage::from_message(task, message);
        debug!(id=?self.id, "Enqueueing job {}", serializable.job_id);
        let task = self.tasks.get(task).unwrap();
        let task_config = task.config();
        self.broker.enqueue(task_config.queue, serializable).await;
    }

    pub async fn run(&self) {
        let (shutdown_tx, _) = channel(1);
        let mut ctrl_c_signal = signal(SignalKind::interrupt()).unwrap();

        let mut handles: Vec<_> = self
            .queues
            .iter()
            .map(|queue| {
                let default_count = self.config.concurrency;
                let worker_count = self
                    .config
                    .queues
                    .get(&queue.to_string())
                    .map(|queue| queue.concurrency)
                    .unwrap_or(default_count);

                let broker = self.broker.clone();
                let tasks = self.tasks.clone();
                let state = self.state.clone();
                let id = self.id.clone();
                let rx = shutdown_tx.subscribe();
                let queue = <&str>::clone(queue);

                tokio::task::spawn(async move {
                    IronWorkerPool::new(id, broker, tasks, queue, state, worker_count, rx)
                        .work()
                        .await
                })
            })
            .collect();

        select!(
            _ = join_all(&mut handles) => {},
            _ = ctrl_c_signal.recv() => {
                shutdown_tx.send(()).expect("All workers have already been dropped");
                join_all(&mut handles).await;
            }
        );
    }
}

#[cfg(test)]
mod test {
    #[tokio::test]
    async fn enqueuing_message_goes_to_broker() {}
}
