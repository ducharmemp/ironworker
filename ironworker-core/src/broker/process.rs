use std::collections::HashMap;

use async_trait::async_trait;
use flume::{unbounded, Receiver, Sender};
use tokio::sync::Mutex;

use crate::{Broker, DeadLetterMessage, SerializableMessage, WorkerState};

#[derive(Default)]
pub struct InProcessBroker {
    queues: Mutex<HashMap<String, (Sender<SerializableMessage>, Receiver<SerializableMessage>)>>,
}

#[async_trait]
impl Broker for InProcessBroker {
    async fn enqueue(&self, queue: &str, message: SerializableMessage) {
        let tx = {
            let mut write_guard = self.queues.lock().await;
            let (tx, _) = write_guard
                .entry(queue.to_string())
                .or_insert_with(|| unbounded());
            tx.clone()
        };
        tx.send_async(message);
    }

    async fn deadletter(&self, _message: DeadLetterMessage) {}

    async fn dequeue(&self, queue: &str) -> Option<SerializableMessage> {
        let rx = {
            let mut write_guard = self.queues.lock().await;
            let (_, rx) = write_guard
                .entry(queue.to_string())
                .or_insert_with(|| unbounded());
            rx.clone()
        };
        rx.recv_async().await.ok()
    }

    async fn list_workers(&self) -> Vec<WorkerState> {
        vec![]
    }

    async fn list_queues(&self) -> Vec<String> {
        vec![]
    }

    async fn heartbeat(&self, _application_id: &str) {}
    async fn deregister_worker(&self, _application_id: &str) {}
    async fn put_back(&self, _message: SerializableMessage) {}
}
