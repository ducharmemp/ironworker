use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use ironworker_core::{Broker, WorkerState};
use ironworker_core::{DeadLetterMessage, SerializableMessage};
use redis::{Client, Commands};
use serde_json::{from_str, to_string};

pub struct RedisBroker {
    client: Client,
}

impl RedisBroker {
    pub async fn new(uri: &str) -> RedisBroker {
        Self {
            client: Client::open(uri).unwrap(),
        }
    }
}

#[async_trait]
impl Broker for RedisBroker {
    async fn enqueue(&self, queue: &str, message: SerializableMessage) {
        let mut conn = self.client.get_connection().unwrap();
        let _: () = conn
            .rpush(queue, vec![to_string(&message).unwrap()])
            .unwrap();
    }

    async fn deadletter(&self, message: DeadLetterMessage) {
        let mut conn = self.client.get_connection().unwrap();
        let key = format!("failed:{}", message.job_id);
        conn.hset::<_, _, _, ()>(&key, "task", message.task).unwrap();
        conn.hset::<_, _, _, ()>(&key, "enqueued_at", message.enqueued_at.to_string()).unwrap();
        conn.hset::<_, _, _, ()>(&key, "payload", to_string(&message.payload).unwrap()).unwrap();
        conn.hset::<_, _, _, ()>(&key, "queue", message.queue).unwrap();
        conn.hset::<_, _, _, ()>(&key, "err", message.err.to_string()).unwrap();
    }

    async fn dequeue(&self, queue: &str) -> Option<SerializableMessage> {
        let mut conn = self.client.get_connection().unwrap();
        let enqueued_items = conn
            .lpop::<&str, Vec<String>>(queue, std::num::NonZeroUsize::new(1))
            .unwrap();
        let item = enqueued_items.first()?;
        let item = from_str::<SerializableMessage>(item).unwrap();
        Some(item)
    }

    async fn list_workers(&self) -> Vec<WorkerState> {
        let mut conn = self.client.get_connection().unwrap();
        let worker_ids = conn.keys::<&str, Vec<String>>("worker:*").unwrap();
        worker_ids
            .iter()
            .map(|worker_id| {
                let worker_hash: HashMap<String, String> = conn.hgetall(worker_id).unwrap();

                WorkerState {
                    name: worker_id.clone(),
                    queues: worker_hash.get("queue").cloned(),
                    last_seen_at: Some(Utc.timestamp_millis(
                        str::parse::<i64>(&worker_hash["last_seen_at"]).unwrap(),
                    )),
                }
            })
            .collect()
    }

    async fn list_queues(&self) -> Vec<String> {
        vec![]
    }

    async fn heartbeat(&self, application_id: &str) {
        let mut conn = self.client.get_connection().unwrap();
        conn.hset::<_, _, _, ()>(
            application_id,
            "last_seen_at",
            Utc::now().timestamp_millis(),
        )
        .unwrap();
    }

    async fn deregister_worker(&self, application_id: &str) {
        let mut conn = self.client.get_connection().unwrap();
        conn.del::<_, ()>(application_id).unwrap();
    }

    async fn put_back(&self, _message: SerializableMessage) {

    }
}

#[cfg(test)]
mod test {}
