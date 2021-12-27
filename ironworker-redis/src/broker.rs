use std::collections::HashMap;
use std::num::NonZeroUsize;

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

impl RedisBroker {
    pub fn format_deadletter_key(job_id: &str) -> String {
        format!("failed:{}", job_id)
    }

    pub fn format_worker_info_key(worker_id: &str) -> String {
        format!("worker:{}", worker_id)
    }

    pub fn format_reserved_key(worker_id: &str) -> String {
        format!("reserved:{}", worker_id)
    }
}

#[async_trait]
impl Broker for RedisBroker {
    async fn enqueue(&self, queue: &str, message: SerializableMessage) {
        let mut conn = self.client.get_connection().unwrap();
        let _: () = conn
            .lpush(queue, vec![to_string(&message).unwrap()])
            .unwrap();
    }

    async fn deadletter(&self, message: DeadLetterMessage) {
        let mut conn = self.client.get_connection().unwrap();
        let key = Self::format_deadletter_key(&message.job_id);

        conn.hset::<_, _, _, ()>(&key, "task", message.task)
            .unwrap();
        conn.hset::<_, _, _, ()>(&key, "enqueued_at", message.enqueued_at.to_string())
            .unwrap();
        conn.hset::<_, _, _, ()>(&key, "payload", to_string(&message.payload).unwrap())
            .unwrap();
        conn.hset::<_, _, _, ()>(&key, "queue", message.queue)
            .unwrap();
        conn.hset::<_, _, _, ()>(&key, "err", message.err.to_string())
            .unwrap();
    }

    async fn dequeue(&self, application_id: &str, queue: &str) -> Option<SerializableMessage> {
        let mut conn = self.client.get_connection().unwrap();
        let reserved_key = Self::format_reserved_key(application_id);

        let item = conn.brpoplpush::<_, String>(queue, &reserved_key, 5);
        let item = item.ok()?;
        let item = from_str::<SerializableMessage>(&item).unwrap();
        Some(item)
    }

    async fn list_workers(&self) -> Vec<WorkerState> {
        let mut conn = self.client.get_connection().unwrap();
        let worker_info_key = Self::format_worker_info_key("*");

        let worker_ids = conn.keys::<_, Vec<String>>(&worker_info_key).unwrap();
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
        let worker_key = Self::format_worker_info_key(application_id);

        conn.hset::<_, _, _, ()>(worker_key, "last_seen_at", Utc::now().timestamp_millis())
            .unwrap();
    }

    async fn deregister_worker(&self, application_id: &str) {
        let mut conn = self.client.get_connection().unwrap();
        conn.del::<_, ()>(application_id).unwrap();
    }

    async fn put_back(&self, _message: SerializableMessage) {}

    async fn mark_done(&self, application_id: &str) {
        let mut conn = self.client.get_connection().unwrap();
        let reserved_key = Self::format_reserved_key(application_id);

        conn.lpop::<_, ()>(reserved_key, NonZeroUsize::new(1))
            .unwrap();
    }
}

#[cfg(test)]
mod test {}
