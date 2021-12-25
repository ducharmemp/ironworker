use std::error::Error;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{to_value, Value};
use uuid::Uuid;

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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SerializableMessage {
    pub job_id: String,
    pub task: String,
    pub payload: Value,
    pub enqueued_at: DateTime<Utc>,
}

impl SerializableMessage {
    pub fn from_message<T: Serialize>(task: &str, message: Message<T>) -> Self {
        Self {
            job_id: Uuid::new_v4().to_string(),
            task: task.to_string(),
            payload: to_value(message.into_inner()).unwrap(),
            enqueued_at: Utc::now(),
        }
    }
}

pub struct DeadLetterMessage {
    pub job_id: String,
    pub task: String,
    pub queue: String,
    pub payload: Value,
    pub enqueued_at: DateTime<Utc>,
    pub err: Box<dyn Error + Send>,
}

impl DeadLetterMessage {
    pub fn new(message: SerializableMessage, queue: &str, err: Box<dyn Error + Send>) -> Self {
        Self {
            job_id: message.job_id,
            task: message.task,
            payload: message.payload,
            enqueued_at: message.enqueued_at,
            queue: queue.to_string(),
            err,
        }
    }
}
