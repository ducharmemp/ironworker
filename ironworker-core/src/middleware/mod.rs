mod extract;

use async_trait::async_trait;

#[async_trait]
pub trait IronworkerMiddleware: Send + Sync + 'static {
    async fn before_enqueue(&self) {}
    async fn after_enqueue(&self) {}
    async fn before_perform(&self) {}
    async fn after_perform(&self) {}
    async fn around_enqueue(&self) {}
    async fn around_perform(&self) {}
}
