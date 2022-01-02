use std::collections::HashMap;
use std::num::NonZeroUsize;

use async_trait::async_trait;
use bb8_redis::{bb8::Pool, RedisConnectionManager};
use chrono::{TimeZone, Utc};
use futures::future;
use ironworker_core::SerializableMessage;
use ironworker_core::{Broker, QueueState, WorkerState};
use redis::{pipe, AsyncCommands};

use crate::message::RedisMessage;

pub struct RedisBroker {
    pool: Pool<RedisConnectionManager>,
}

impl RedisBroker {
    pub async fn new(uri: &str) -> RedisBroker {
        let manager = RedisConnectionManager::new(uri).unwrap();
        Self {
            pool: Pool::builder().max_size(50).build(manager).await.unwrap(),
        }
    }
}

impl RedisBroker {
    pub fn format_deadletter_key(queue_name: &str) -> String {
        format!("failed:{}", queue_name)
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
        let mut conn = self.pool.get().await.unwrap();
        let queue_key = Self::format_queue_key(queue);
        let message = RedisMessage::from(message);

        conn.lpush::<_, _, ()>(&queue_key, message).await.unwrap();
    }

    async fn deadletter(&self, queue: &str, message: SerializableMessage) {
        let mut conn = self.pool.get().await.unwrap();
        let deadletter_key = Self::format_deadletter_key(queue);
        let message = RedisMessage::from(message);

        conn.lpush::<_, _, ()>(&deadletter_key, message)
            .await
            .unwrap();
    }

    async fn dequeue(&self, application_id: &str, queue: &str) -> Option<SerializableMessage> {
        let mut conn = self.pool.get().await.unwrap();
        let reserved_key = Self::format_reserved_key(application_id);
        let queue_key = Self::format_queue_key(queue);

        let item = conn
            .brpoplpush::<_, RedisMessage>(&queue_key, &reserved_key, 5)
            .await;
        let item = item.ok()?;
        Some(item.into())
    }

    async fn list_workers(&self) -> Vec<WorkerState> {
        let worker_ids = {
            let mut conn = self.pool.get().await.unwrap();
            let worker_info_key = Self::format_worker_info_key("*");
            conn.keys::<_, Vec<String>>(&worker_info_key).await.unwrap()
        };

        let futs: Vec<_> = worker_ids
            .iter()
            .map(|worker_id| async move {
                let mut conn = self.pool.get().await.unwrap();
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
            let mut conn = self.pool.get().await.unwrap();
            let queue_key = Self::format_queue_key("*");
            conn.keys::<_, Vec<String>>(&queue_key).await.unwrap()
        };

        let futs: Vec<_> = queue_ids
            .iter()
            .map(|worker_id| async move {
                let mut conn = self.pool.get().await.unwrap();
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
        let mut conn = self.pool.get().await.unwrap();
        let worker_key = Self::format_worker_info_key(application_id);

        pipe()
            .atomic()
            .hset(&worker_key, "last_seen_at", Utc::now().timestamp_millis())
            .expire(&worker_key, 60)
            .query_async::<_, ()>(&mut *conn)
            .await
            .unwrap();
    }

    async fn deregister_worker(&self, application_id: &str) {
        let mut conn = self.pool.get().await.unwrap();
        conn.del::<_, ()>(application_id).await.unwrap();
    }

    async fn mark_done(&self, application_id: &str) {
        let mut conn = self.pool.get().await.unwrap();
        let reserved_key = Self::format_reserved_key(application_id);

        conn.lpop::<_, ()>(reserved_key, NonZeroUsize::new(1))
            .await
            .unwrap();
    }
}

#[cfg(test)]
mod test {
    #[tokio::test]
    async fn enqueue_pushed_to_a_list() {}
}
