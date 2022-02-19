mod process;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::message::SerializableMessage;
pub use process::InProcessBroker;

/// A broker implementation is the main way to communicate with a backing datastore. The backing datastore can be a queue, database, or even in memory,
/// but these details are left to the implementors of this trait. Because of the differences between different services (ex. redis vs SQS vs RabbitMQ),
/// not all methods on the broker are required to be implemented and will default to noops if the method is not applicable for a given datastore. An
/// example of this is the heartbeat function, which only makes sense with datastores where manual health tracking of clients is necessary.
#[async_trait]
pub trait Broker: Send + Sync + 'static {
    type Error: std::fmt::Debug;

    // Sets up a worker in the backing datastore. Useful for things like initalization of local structs for queues or general setup logic.
    async fn register_worker(&self, _worker_id: &str, _queue: &str) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Sends a given message to a broker. The serialization and format of the message is handled by the broker,
    /// `SerializableMessage` implements serde's Serialize/Deserialize traits. Returns a broker-specific error on failure
    /// to communicate or enqueue the task into the backing database.
    async fn enqueue(
        &self,
        queue: &str,
        message: SerializableMessage,
        _at: Option<DateTime<Utc>>,
    ) -> Result<(), Self::Error>;

    /// Sends a failed message to a special deadletter queue when the task has failed more than the maximum allowable tries.
    /// If the broker is not configured to manually deadletter (for example, in RabbitMQ or SQS where deadlettering is automatic),
    /// this function is a noop.
    async fn deadletter(
        &self,
        _queue: &str,
        _message: SerializableMessage,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Receives a message from the backing database. This function should block on a timeout until a message is received, or return `None` if
    /// no message has been received within the time period allotted.
    async fn dequeue(&self, from: &str) -> Result<Option<SerializableMessage>, Self::Error>;

    /// Sends a heartbeat to the backing database. If the broker is not configured to perform heartbeats, this function is a noop.
    async fn heartbeat(&self, _worker_id: &str) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Removes a worker from the known set of workers from the backing store. If the broker is not configured to know about consumers, this function is a noop.
    async fn deregister_worker(&self, _worker_id: &str) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Sends an acknowledgement to the backing store. If the backing store does not need to have messages acknowledged, this function is a noop.
    async fn acknowledge_processed(
        &self,
        _from: &str,
        _message: SerializableMessage,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}
