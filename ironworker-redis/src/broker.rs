use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use ironworker_core::Broker;
use ironworker_core::{Message, SerializableMessage, Task, Worker};
use redis::{Client, Commands, Value};
use serde::Serialize;
use serde_json::{from_str, to_string};

use crate::error::Result;
use crate::worker::RedisWorker;

pub struct RedisBroker<'application> {
    workers: HashMap<&'application str, Arc<Box<dyn Worker + Sync + Send>>>,
    client: Client,
}

impl<'application> RedisBroker<'application> {
    pub async fn new(uri: &'application str) -> RedisBroker<'application> {
        Self {
            workers: HashMap::new(),
            client: Client::open(uri).unwrap(),
        }
    }
}

#[async_trait]
impl<'application> Broker for RedisBroker<'application> {
    async fn register_task<T: Task + Send>(&mut self, task: T) {
        self.workers.insert(
            task.name(),
            Arc::new(Box::new(RedisWorker::new(&self, task.name(), task).await)),
        );
    }

    async fn register_worker(&self, worker_name: &str) {
        let mut conn = self.client.get_connection().unwrap();
        conn.set_ex::<_, _, ()>(worker_name, 1_u32, 5 * 60).unwrap();
    }

    async fn enqueue<T: Into<SerializableMessage> + Send + Serialize>(
        &self,
        queue: &str,
        payload: T,
    ) {
        let mut conn = self.client.get_connection().unwrap();
        let message: SerializableMessage = payload.into();
        let _: () = conn
            .rpush(queue, vec![to_string(&message).unwrap()])
            .unwrap();
    }

    async fn work(self) {
        let mut conn = self.client.get_connection().unwrap();
        loop {
            for (queue, worker) in &self.workers {
                let enqueued_items = conn
                    .lpop::<&'application str, Vec<String>>(queue, std::num::NonZeroUsize::new(1))
                    .unwrap();
                let item = enqueued_items.first().unwrap();
                let item = from_str::<SerializableMessage>(item).unwrap();

                worker.work(item).await;
            }
        }
    }

    async fn workers(&self) -> Vec<String> {
        let mut conn = self.client.get_connection().unwrap();
        let workers = conn.keys::<&str, Vec<String>>("worker:*").unwrap();
        workers
    }

    async fn queues(&self) -> Vec<String> {
        self.workers.keys().map(|val| val.to_string()).collect()
    }
}

#[cfg(test)]
mod test {}
