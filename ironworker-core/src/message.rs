use anymap::{CloneAny, Map};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use derive_builder::Builder;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, Error, Value};
use uuid::Uuid;

use crate::{error::IronworkerError, from_payload::FromPayload};

/// A job payload. This struct describes the argument to a given task (passed in by `perform_now`/`perform_later`).
/// This is a mandatory field for all tasks, even if the internal value is an empty tuple.
#[derive(Debug)]
pub struct Message<T>(pub T);

#[async_trait]
impl<T> FromPayload for Message<T>
where
    T: for<'de> Deserialize<'de> + Serialize + 'static + Send,
{
    type Error = Error;

    async fn from_payload(message: &SerializableMessage) -> Result<Self, Self::Error> {
        let deserialized = Message(from_value::<T>(message.payload.clone())?);
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
    pub message: String,
}

impl SerializableError {
    pub fn new(err: IronworkerError) -> Self {
        Self {
            message: format!("{:#?}: {}", &err, &err),
        }
    }
}

/// Serializable representation of a job given to a broker. A broker receiving this message should
/// persist it in the way that makes the most sense for the backing datastore. Most of the time
/// this is going to mean converting the whole message to a JSON string.
#[derive(Serialize, Deserialize, Clone, Debug, Builder)]
#[builder(pattern = "mutable")]
pub struct SerializableMessage {
    /// The unique identifier of the job.
    #[builder(default = "Uuid::new_v4()")]
    pub job_id: Uuid,
    /// The queue that the message is destined for
    pub queue: String,
    /// The performing function name
    pub task: String,
    /// A JSON representation of the arguments provided to the job
    pub payload: Value,
    /// When the job was created, at UTC time
    #[builder(default = "Utc::now()")]
    pub created_at: DateTime<Utc>,
    /// When the job was enqueued, at UTC time
    #[builder(default)]
    pub enqueued_at: Option<DateTime<Utc>>,
    /// The scheduled time for the job. This may not be the exact time a message is processed.
    #[builder(default)]
    pub at: Option<DateTime<Utc>>,
    /// If there was an error performing the job and it needs to be retried, this field will have a serializable representation of
    /// the error returned by the function
    #[builder(default)]
    pub err: Option<SerializableError>,
    /// An incrementing number representing the number of times this job has been retried
    pub retries: u64,
    /// A broker-specific field, mainly for backends like SQS or RabbitMQ where a message needs to be acknowledged with a specific identifier provided by
    /// the datastore.
    #[serde(default)]
    #[builder(default)]
    pub delivery_tag: Option<String>,

    /// A data bag, useful for middlewares to add state that should be `Extract`-ed when the task is run
    #[serde(skip)]
    #[builder(default)]
    pub message_state: Map<dyn CloneAny + Send + Sync>,
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
