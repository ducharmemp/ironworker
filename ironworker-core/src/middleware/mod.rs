pub mod extract;

use async_trait::async_trait;

use crate::SerializableMessage;

#[async_trait]
pub trait IronworkerMiddleware: Send + Sync + 'static {
    async fn before_enqueue(&self) {}
    async fn after_enqueue(&self) {}
    async fn before_perform(&self, _message: &mut SerializableMessage) {}
    async fn after_perform(&self) {}
}
