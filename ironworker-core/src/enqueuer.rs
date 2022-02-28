use async_trait::async_trait;
#[cfg(test)]
use mockall::automock;
#[cfg(test)]
use mockall::predicate::*;
use serde::Serialize;

use crate::{error::IronworkerError, message::Message, task::Config as TaskConfig};

#[cfg_attr(test, automock)]
#[async_trait]
pub trait Enqueuer: Send + Sync {
    /// Sends a task to a given broker, utilizing the task's configuration to determine routing. If no configuration is provided, the special "default" queue is chosen
    async fn enqueue<P: Serialize + Send + Into<Message<P>> + 'static>(
        &self,
        task: &str,
        payload: P,
        task_config: TaskConfig,
    ) -> Result<(), IronworkerError>;
}
