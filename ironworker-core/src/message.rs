use serde::{Serialize, Deserialize};
use serde_json::{Value, to_value};
use chrono::{DateTime, Utc};

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

#[derive(Serialize, Deserialize)]
pub struct SerializableMessage {
    pub payload: Value,
    pub enqueued_at: DateTime<Utc>
}

impl <T: Serialize> From<Message<T>> for SerializableMessage {
    fn from(payload: Message<T>) -> Self {
        Self {
            payload: to_value(payload.into_inner()).unwrap(),
            enqueued_at: Utc::now()
        }
    }
}
