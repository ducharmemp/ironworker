mod process;

use async_trait::async_trait;

use crate::{message::SerializableMessage, QueueState, WorkerState};
pub use process::InProcessBroker;

#[async_trait]
pub trait Broker {
    async fn enqueue(&self, queue: &str, message: SerializableMessage);
    async fn deadletter(&self, queue: &str, message: SerializableMessage);
    async fn dequeue(&self, application_id: &str, queue: &str) -> Option<SerializableMessage>;
    async fn list_workers(&self) -> Vec<WorkerState>;
    async fn list_queues(&self) -> Vec<QueueState>;
    async fn heartbeat(&self, application_id: &str);
    async fn deregister_worker(&self, application_id: &str);
    async fn put_back(&self, message: SerializableMessage);
    async fn mark_done(&self, application_id: &str);
}
