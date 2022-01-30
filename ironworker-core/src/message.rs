use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{to_value, Value};
use uuid::Uuid;

use crate::task::TaskError;

#[derive(Debug)]
pub struct Message<T>(pub T);

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
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
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
        }
    }
}
