use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{to_value, Value};

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

#[derive(Serialize, Deserialize, Debug)]
pub struct SerializableMessage {
    pub task: String,
    pub payload: Value,
    pub enqueued_at: DateTime<Utc>,
}

impl SerializableMessage {
    pub fn from_message<T: Serialize>(task: &str, message: Message<T>) -> Self {
        Self {
            task: task.to_string(),
            payload: to_value(message.into_inner()).unwrap(),
            enqueued_at: Utc::now(),
        }
    }
}
