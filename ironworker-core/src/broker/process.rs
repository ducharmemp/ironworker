use std::collections::{HashMap, VecDeque};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio::sync::Mutex;

use crate::{Broker, SerializableMessage};

#[derive(Default, Debug)]
pub struct InProcessBroker {
    pub queues: Mutex<HashMap<String, VecDeque<SerializableMessage>>>,
    pub deadletter: Mutex<HashMap<String, SerializableMessage>>,
}

#[async_trait]
impl Broker for InProcessBroker {
    type Error = ();

    async fn enqueue(
        &self,
        queue: &str,
        message: SerializableMessage,
        _at: Option<DateTime<Utc>>,
    ) -> Result<(), Self::Error> {
        let mut write_guard = self.queues.lock().await;
        let queue = write_guard.entry(queue.to_string()).or_default();
        queue.push_back(message);
        Ok(())
    }

    async fn deadletter(
        &self,
        queue: &str,
        message: SerializableMessage,
    ) -> Result<(), Self::Error> {
        let mut write_guard = self.deadletter.lock().await;
        write_guard
            .entry(queue.to_string())
            .or_insert_with(|| message);
        Ok(())
    }

    async fn dequeue(&self, from: &str) -> Result<Option<SerializableMessage>, Self::Error> {
        let mut write_guard = self.queues.lock().await;
        let queue = write_guard.entry(from.to_string()).or_default();
        Ok(queue.pop_front())
    }
}
