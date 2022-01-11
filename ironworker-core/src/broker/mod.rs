mod config;
mod process;

use async_trait::async_trait;

pub use self::config::{BrokerConfig, HeartbeatStrategy, RetryStrategy};
use crate::message::SerializableMessage;
pub use process::InProcessBroker;

#[async_trait]
pub trait Broker: Send + Sync {
    type Error: std::fmt::Debug;

    async fn enqueue(&self, queue: &str, message: SerializableMessage) -> Result<(), Self::Error>;
    async fn deadletter(
        &self,
        _queue: &str,
        _message: SerializableMessage,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn dequeue(&self, from: &str) -> Option<SerializableMessage>;
    async fn heartbeat(&self, _application_id: &str) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn deregister_worker(&self, _application_id: &str) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn acknowledge_processed(
        &self,
        _from: &str,
        _message: SerializableMessage,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}
