use async_trait::async_trait;

use crate::{message::SerializableMessage, WorkerState};

#[async_trait]
pub trait Broker {
    async fn enqueue(&self, queue: &str, message: SerializableMessage);
    async fn dequeue(&self, queue: &str) -> Option<SerializableMessage>;
    async fn list_workers(&self) -> Vec<WorkerState>;
    async fn list_queues(&self) -> Vec<String>;
    async fn heartbeat(&self, application_id: &str);
}
