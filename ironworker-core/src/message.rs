use std::convert::Infallible;

use anymap::{CloneAny, Map};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{from_value, to_value, Value};
use uuid::Uuid;

use crate::{task::TaskError, FromPayload};

#[derive(Debug)]
pub struct Message<T>(pub T);

#[async_trait]
impl<T> FromPayload for Message<T>
where
    T: for<'de> Deserialize<'de> + Serialize + 'static + Send,
{
    type Error = Infallible;

    async fn from_payload(message: &SerializableMessage) -> Result<Self, Self::Error> {
        let deserialized = Message(from_value::<T>(message.payload.clone()).unwrap());
        Ok(deserialized)
    }
}

impl<T: Serialize> From<T> for Message<T> {
    fn from(payload: T) -> Self {
        Self(payload)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SerializableError {
    message: String,
}

impl SerializableError {
    pub(crate) fn new(err: Box<dyn TaskError>) -> Self {
        Self {
            message: format!("{:?}", err),
        }
    }
}

/// Serializable representation of a job given to a broker. A broker receiving this message should
/// persist it in the way that makes the most sense for the backing datastore. Most of the time
/// this is going to mean converting the whole message to a JSON string.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SerializableMessage {
    /// The unique identifier of the job.
    pub job_id: String,
    /// The queue that the message is destined for
    pub queue: String,
    /// The performing function name
    pub task: String,
    /// A JSON representation of the arguments provided to the job
    pub payload: Value,
    /// When the job was enqueued, at UTC time
    pub enqueued_at: DateTime<Utc>,
    /// If there was an error performing the job and it needs to be retried, this field will have a serializable representation of
    /// the error returned by the function
    pub err: Option<SerializableError>,
    /// An incrementing number representing the number of times this job has been retried
    pub retries: usize,
    /// A broker-specific field, mainly for backends like SQS or RabbitMQ where a message needs to be acknowledged with a specific identifier provided by
    /// the datastore.
    #[serde(default)]
    pub delivery_tag: Option<String>,

    #[serde(skip)]
    pub message_state: Map<dyn CloneAny + Send + Sync>,
}

impl SerializableMessage {
    /// Converts from a given message into a serializable representation
    pub fn from_message<T: Serialize>(task: &str, queue: &str, Message(value): Message<T>) -> Self {
        Self {
            job_id: Uuid::new_v4().to_string(),
            task: task.to_string(),
            queue: queue.to_string(),
            payload: to_value(value).unwrap(),
            enqueued_at: Utc::now(),
            err: None,
            retries: 0,
            delivery_tag: None,
            message_state: Default::default(),
        }
    }
}

impl PartialEq for SerializableMessage {
    fn eq(&self, other: &Self) -> bool {
        self.job_id == other.job_id
            && self.queue == other.queue
            && self.task == other.task
            && self.payload == other.payload
            && self.enqueued_at == other.enqueued_at
            && self.err == other.err
            && self.retries == other.retries
            && self.delivery_tag == other.delivery_tag
    }
}

impl Eq for SerializableMessage {}
