use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{to_value, Value};
use uuid::Uuid;

use crate::task::TaskError;

#[derive(Debug)]
pub struct Message<T>(T);

impl<T: Serialize> From<T> for Message<T> {
    fn from(payload: T) -> Self {
        Self(payload)
    }
}

impl<T> Message<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SerializableError {
    message: String,
}

impl SerializableError {
    pub(crate) fn new(err: Box<dyn TaskError>) -> Self {
        Self {
            message: format!("{:?}", err)
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SerializableMessage {
    pub job_id: String,
    pub queue: String,
    pub task: String,
    pub payload: Value,
    pub enqueued_at: DateTime<Utc>,
    pub err: Option<SerializableError>,
    pub retries: usize,
    #[serde(default)]
    pub delivery_tag: Option<String>,
}

impl SerializableMessage {
    pub fn from_message<T: Serialize>(task: &str, queue: &str, message: Message<T>) -> Self {
        Self {
            job_id: Uuid::new_v4().to_string(),
            task: task.to_string(),
            queue: queue.to_string(),
            payload: to_value(message.into_inner()).unwrap(),
            enqueued_at: Utc::now(),
            err: None,
            retries: 0,
            delivery_tag: None,
        }
    }
}
