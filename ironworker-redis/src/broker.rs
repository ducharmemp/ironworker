use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use futures::future;
use ironworker_core::info::{
    BrokerInfo, DeadletteredInfo, QueueInfo, ScheduledInfo, Stats, WorkerInfo,
};
use ironworker_core::Broker;
use ironworker_core::SerializableMessage;
use redis::aio::ConnectionManager;
use redis::{pipe, AsyncCommands, Client, RedisError, RedisResult};

use crate::message::RedisMessage;

pub struct RedisBroker {
    connection: ConnectionManager,
}

impl RedisBroker {
    pub async fn new(uri: &str) -> RedisResult<RedisBroker> {
        let client = Client::open(uri)?;
        let connection = client.get_tokio_connection_manager().await?;
        Ok(Self {
            connection,
        })
    }

    pub async fn from_cliet(client: &Client) -> RedisResult<Self> {
        let connection = client.get_tokio_connection_manager().await?;
        Ok(Self {
            connection
        })
    }
}

impl RedisBroker {
    fn format_deadletter_key(queue_name: &str) -> String {
        format!("failed:{}", queue_name)
    }

    fn format_worker_info_key(worker_id: &str) -> String {
        format!("worker:{}", worker_id)
    }

    fn format_queue_key(queue_name: &str) -> String {
        format!("queue:{}", queue_name)
    }
}

#[async_trait]
impl Broker for RedisBroker {
    type Error = RedisError;

    async fn enqueue(&self, queue: &str, message: SerializableMessage) -> Result<(), Self::Error> {
        let mut conn = self.connection.clone();
        let queue_key = Self::format_queue_key(queue);
        let message = RedisMessage::from(message);
        conn.lpush::<_, _, ()>(&queue_key, message).await?;
        conn.publish::<&str, &str, ()>(&queue_key, "publish").await?;
        Ok(())
    }

    async fn deadletter(
        &self,
        queue: &str,
        message: SerializableMessage,
    ) -> Result<(), Self::Error> {
        let mut conn = self.connection.clone();
        let deadletter_key = Self::format_deadletter_key(queue);
        let message = RedisMessage::from(message);

        conn.lpush::<_, _, ()>(&deadletter_key, message).await?;
        Ok(())
    }

    async fn dequeue(&self, from: &str) -> Option<SerializableMessage> {
        let mut conn = self.connection.clone();
        let from = Self::format_queue_key(from);

        let items = conn.rpop::<_, Vec<RedisMessage>>(&from, NonZeroUsize::new(1)).await;
        let mut items = items.ok()?;
        if let Some(item) = items.pop() {
            return Some(item.into())
        }

        tokio::time::sleep(Duration::from_millis(5000)).await;
        None
    }

    async fn heartbeat(&self, worker_id: &str) -> Result<(), Self::Error> {
        let mut conn = self.connection.clone();
        let worker_key = Self::format_worker_info_key(worker_id);

        pipe()
            .atomic()
            .hset(&worker_key, "last_seen_at", Utc::now().timestamp_millis())
            .expire(&worker_key, 60)
            .query_async::<_, ()>(&mut conn)
            .await?;
        Ok(())
    }

    async fn deregister_worker(&self, worker_id: &str) -> Result<(), Self::Error> {
        let mut conn = self.connection.clone();
        conn.del::<_, ()>(worker_id).await?;
        Ok(())
    }
}

#[async_trait]
impl BrokerInfo for RedisBroker {
    async fn workers(&self) -> Vec<WorkerInfo> {
        let worker_ids = {
            let mut conn = self.connection.clone();
            let worker_info_key = Self::format_worker_info_key("*");
            conn.keys::<_, Vec<String>>(&worker_info_key).await.unwrap()
        };

        let futs: Vec<_> = worker_ids
            .iter()
            .map(|worker_id| async move {
                let mut conn = self.connection.clone();
                let worker_hash: HashMap<String, String> = conn.hgetall(worker_id).await.unwrap();

                WorkerInfo {
                    name: worker_id.clone(),
                    queue: worker_hash.get("queue").cloned(),
                    last_seen_at: Some(Utc.timestamp_millis(
                        str::parse::<i64>(&worker_hash["last_seen_at"]).unwrap(),
                    )),
                }
            })
            .collect();
        future::join_all(futs).await
    }

    async fn queues(&self) -> Vec<QueueInfo> {
        let queue_ids = {
            let mut conn = self.connection.clone();
            let queue_key = Self::format_queue_key("*");
            conn.keys::<_, Vec<String>>(&queue_key).await.unwrap()
        };

        let futs: Vec<_> = queue_ids
            .iter()
            .map(|worker_id| async move {
                let mut conn = self.connection.clone();
                let queue_size = conn.llen(worker_id).await.unwrap();

                QueueInfo {
                    name: worker_id.clone(),
                    size: queue_size,
                }
            })
            .collect();
        future::join_all(futs).await
    }

    async fn stats(&self) -> Stats {
        Stats {
            processed: 0,
            failed: 0,
            scheduled: 0,
            enqueued: 0,
        }
    }

    async fn deadlettered(&self) -> Vec<DeadletteredInfo> {
        vec![]
    }

    async fn scheduled(&self) -> Vec<ScheduledInfo> {
        vec![]
    }
}
