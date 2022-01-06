use std::collections::{HashMap, VecDeque};

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::{Broker, QueueState, SerializableMessage, WorkerState};

#[derive(Default)]
pub struct InProcessBroker {
    pub queues: Mutex<HashMap<String, VecDeque<SerializableMessage>>>,
    pub deadletter: Mutex<HashMap<String, SerializableMessage>>,
}

#[async_trait]
impl Broker for InProcessBroker {
    async fn enqueue(&self, queue: &str, message: SerializableMessage) {
        let mut write_guard = self.queues.lock().await;
        let queue = write_guard.entry(queue.to_string()).or_default();
        queue.push_back(message);
    }

    async fn deadletter(&self, queue: &str, message: SerializableMessage) {
        let mut write_guard = self.deadletter.lock().await;
        write_guard
            .entry(queue.to_string())
            .or_insert_with(|| message);
    }

    async fn dequeue(&self, from: &str) -> Option<SerializableMessage> {
        let mut write_guard = self.queues.lock().await;
        let queue = write_guard.entry(from.to_string()).or_default();
        queue.pop_front()
    }

    async fn list_workers(&self) -> Vec<WorkerState> {
        vec![]
    }

    async fn list_queues(&self) -> Vec<QueueState> {
        vec![]
    }

    async fn heartbeat(&self, _application_id: &str) {}
    async fn deregister_worker(&self, _application_id: &str) {}
}
