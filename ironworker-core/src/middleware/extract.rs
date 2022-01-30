use async_trait::async_trait;

use super::IronworkerMiddleware;

#[derive(Clone, Copy, Debug)]
pub struct AddExtractMiddleware<T> {
    value: T,
}

#[async_trait]
impl<T: Send + Sync + 'static> IronworkerMiddleware for AddExtractMiddleware<T> {
    async fn before_perform(&self) {}
}
