use async_trait::async_trait;
use serde::Serialize;

use crate::task::Config as TaskConfig;
use crate::{IronworkerError, Message};

#[async_trait]
pub trait Enqueuer: Send + Sync {
    /// Sends a task to a given broker, utilizing the task's configuration to determine routing. If no configuration is provided, the special "default" queue is chosen
    async fn enqueue<P: Serialize + Send + Into<Message<P>>>(
        &self,
        task: &str,
        payload: P,
        task_config: TaskConfig,
    ) -> Result<(), IronworkerError>;
}
