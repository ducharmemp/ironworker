use std::collections::HashMap;
use std::num::NonZeroUsize;

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use futures::future;
use ironworker_core::{Broker, QueueState, WorkerState};
use ironworker_core::{DeadLetterMessage, SerializableMessage};
use redis::{AsyncCommands, Client};
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

    pub fn format_queue_key(queue_name: &str) -> String {
        format!("queue:{}", queue_name)
    }
}

#[async_trait]
impl Broker for RedisBroker {
    async fn enqueue(&self, queue: &str, message: SerializableMessage) {
        let mut conn = self.client.get_async_connection().await.unwrap();
        let queue_key = Self::format_queue_key(queue);
        let _: () = conn
            .lpush(&queue_key, vec![to_string(&message).unwrap()])
            .await
            .unwrap();
    }

    async fn deadletter(&self, message: DeadLetterMessage) {
        let err = message.err.to_string();
        let mut conn = self.client.get_async_connection().await.unwrap();
        let key = Self::format_deadletter_key(&message.job_id);

        conn.hset::<_, _, _, ()>(&key, "task", message.task)
            .await
            .unwrap();
        conn.hset::<_, _, _, ()>(&key, "enqueued_at", message.enqueued_at.to_string())
            .await
            .unwrap();
        conn.hset::<_, _, _, ()>(&key, "payload", to_string(&message.payload).unwrap())
            .await
            .unwrap();
        conn.hset::<_, _, _, ()>(&key, "queue", message.queue)
            .await
            .unwrap();
        conn.hset::<_, _, _, ()>(&key, "err", err).await.unwrap();
    }

    async fn dequeue(&self, application_id: &str, queue: &str) -> Option<SerializableMessage> {
        let mut conn = self.client.get_async_connection().await.unwrap();
        let reserved_key = Self::format_reserved_key(application_id);
        let queue_key = Self::format_queue_key(queue);

        let item = conn
            .brpoplpush::<_, String>(&queue_key, &reserved_key, 5)
            .await;
        let item = item.ok()?;
        let item = from_str::<SerializableMessage>(&item).unwrap();
        Some(item)
    }

    async fn list_workers(&self) -> Vec<WorkerState> {
        let worker_ids = {
            let mut conn = self.client.get_async_connection().await.unwrap();
            let worker_info_key = Self::format_worker_info_key("*");
            conn.keys::<_, Vec<String>>(&worker_info_key).await.unwrap()
        };

        let futs: Vec<_> = worker_ids
            .iter()
            .map(|worker_id| async move {
                let mut conn = self.client.get_async_connection().await.unwrap();
                let worker_hash: HashMap<String, String> = conn.hgetall(worker_id).await.unwrap();

                WorkerState {
                    name: worker_id.clone(),
                    queues: worker_hash.get("queue").cloned(),
                    last_seen_at: Some(Utc.timestamp_millis(
                        str::parse::<i64>(&worker_hash["last_seen_at"]).unwrap(),
                    )),
                }
            })
            .collect();
        future::join_all(futs).await
    }

    async fn list_queues(&self) -> Vec<QueueState> {
        let queue_ids = {
            let mut conn = self.client.get_async_connection().await.unwrap();
            let queue_key = Self::format_queue_key("*");
            conn.keys::<_, Vec<String>>(&queue_key).await.unwrap()
        };

        let futs: Vec<_> = queue_ids
            .iter()
            .map(|worker_id| async move {
                let mut conn = self.client.get_async_connection().await.unwrap();
                let queue_size = conn.llen(worker_id).await.unwrap();

                QueueState {
                    name: worker_id.clone(),
                    size: queue_size,
                }
            })
            .collect();
        future::join_all(futs).await
    }

    async fn heartbeat(&self, application_id: &str) {
        let mut conn = self.client.get_async_connection().await.unwrap();
        let worker_key = Self::format_worker_info_key(application_id);

        conn.hset::<_, _, _, ()>(worker_key, "last_seen_at", Utc::now().timestamp_millis())
            .await
            .unwrap();
    }

    async fn deregister_worker(&self, application_id: &str) {
        let mut conn = self.client.get_async_connection().await.unwrap();
        conn.del::<_, ()>(application_id).await.unwrap();
    }

    async fn put_back(&self, _message: SerializableMessage) {}

    async fn mark_done(&self, application_id: &str) {
        let mut conn = self.client.get_async_connection().await.unwrap();
        let reserved_key = Self::format_reserved_key(application_id);

        conn.lpop::<_, ()>(reserved_key, NonZeroUsize::new(1))
            .await
            .unwrap();
    }
}

#[cfg(test)]
mod test {}
