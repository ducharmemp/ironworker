use async_trait::async_trait;
use serde::Serialize;

use crate::{message::SerializableMessage, task::Task};

#[async_trait]
pub trait Broker {
    async fn register_task<T: Task + Send>(&mut self, task: T);
    async fn register_worker(&self, worker_name: &str);
    async fn enqueue<T: Into<SerializableMessage> + Send + Serialize>(
        &self,
        queue: &str,
        payload: T,
    );
    async fn work(self);
    async fn workers(&self) -> Vec<String>;
    async fn queues(&self) -> Vec<String>;
}
