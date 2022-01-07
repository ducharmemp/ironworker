use async_trait::async_trait;

#[async_trait]
pub trait IronworkerMiddleware: Send + Sync + 'static {
    async fn on_task_start(&self);
    async fn on_task_completion(&self);
    async fn on_task_failure(&self);
}
