use async_trait::async_trait;

use crate::message::SerializableMessage;

#[async_trait]
pub trait Worker {
    fn id(&self) -> &str;
    async fn work(&self, item: SerializableMessage);
    async fn register(&self);
    async fn heartbeat(&self);
    async fn deregister(&self);
}