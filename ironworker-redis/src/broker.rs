use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use ironworker_core::SerializableMessage;
use ironworker_core::{Broker, WorkerState};
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

    async fn heartbeat(&self, worker_name: &str) {
        let mut conn = self.client.get_connection().unwrap();
        conn.hset::<_, _, _, ()>(worker_name, "last_seen_at", Utc::now().timestamp_millis())
            .unwrap();
    }
}

#[cfg(test)]
mod test {}
