mod process;

use async_trait::async_trait;

use crate::message::SerializableMessage;
pub use process::InProcessBroker;

#[async_trait]
pub trait Broker: Send + Sync {
    async fn enqueue(&self, queue: &str, message: SerializableMessage);
    async fn deadletter(&self, queue: &str, message: SerializableMessage);
    async fn dequeue(&self, from: &str) -> Option<SerializableMessage>;
    async fn heartbeat(&self, application_id: &str);
    async fn deregister_worker(&self, application_id: &str);
}
