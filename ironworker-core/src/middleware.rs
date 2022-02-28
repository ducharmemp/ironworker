use async_trait::async_trait;

use crate::message::SerializableMessage;

/// A trait for implementing middleware for Ironworker applications. These methods currently must not fail
#[async_trait]
pub trait IronworkerMiddleware: Send + Sync + 'static {
    /// Called before the job is enqueued
    async fn before_enqueue(&self, _message: &SerializableMessage) {}

    /// Called after the job is enqueued.
    async fn after_enqueue(&self, _message: &SerializableMessage) {}

    /// Called before the job is executed.
    async fn before_perform(&self, _message: &mut SerializableMessage) {}

    /// Called after the job is executed.
    async fn after_perform(&self, _message: &SerializableMessage) {}
}
