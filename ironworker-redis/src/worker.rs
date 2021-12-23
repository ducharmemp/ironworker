use uuid::Uuid;
use async_trait::async_trait;
use ironworker_core::{Worker, Task, SerializableMessage, Broker};

use crate::{error::Result, RedisBroker};

#[derive(Clone)]
pub struct RedisWorker<'worker, H: Task> {
    name: String,
    queue: &'worker str,
    task: H
}

impl<'worker, H: Task> RedisWorker<'worker, H> {
    pub async fn new(broker: &RedisBroker<'_>, queue: &'worker str, task: H) -> RedisWorker<'worker, H> {
        let name = format!("worker:{}", Uuid::new_v4());
        broker.register_worker(&name).await;
        RedisWorker {
            name,
            queue,
            task
        }
    }
}

#[async_trait]
impl<'worker, H: Task> Worker for RedisWorker<'worker, H> {
    fn id(&self) -> &str {
        &self.name
    }

    async fn work(&self, item: SerializableMessage) {
        self.task.perform(item).await;
    }
    async fn register(&self) {}
    async fn heartbeat(&self) {}
    async fn deregister(&self) {}
}
