mod extract;

use async_trait::async_trait;

use crate::SerializableMessage;

#[async_trait]
pub trait IronworkerMiddleware: Send + Sync + 'static {
    async fn before_enqueue(&self) {}
    async fn after_enqueue(&self) {}
    async fn before_perform(&self, message: SerializableMessage) -> SerializableMessage { message }
    async fn after_perform(&self) {}
    async fn around_enqueue(&self) {}
    async fn around_perform(&self) {}
}
