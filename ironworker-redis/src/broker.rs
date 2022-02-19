use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use futures::future;
use ironworker_core::info::{
    BrokerInfo, DeadletteredInfo, QueueInfo, ScheduledInfo, Stats, WorkerInfo,
};
use ironworker_core::Broker;
use ironworker_core::SerializableMessage;
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Client, RedisError, RedisResult, Script};

use crate::message::RedisMessage;
static SCRIPT_SRC: &str = r#"
local key, now = KEYS[1], ARGV[1]
local jobs = redis.call("zrangebyscore", key, "-inf", now, "limit", 0, 1)
if jobs[1] then
    redis.call("zrem", key, jobs[1])
    return jobs[1]
end
"#;

/// An Ironworker broker for communicating with Redis.
pub struct RedisBroker {
    connection: ConnectionManager,
}

impl RedisBroker {
    /// Constructs a RedisBroker instance from a provided URI.
    pub async fn new(uri: &str) -> RedisResult<RedisBroker> {
        let client = Client::open(uri)?;
        let connection = client.get_tokio_connection_manager().await?;
        Ok(Self { connection })
    }

    /// Constructs a RedisBroker instance from an existing client.
    pub async fn from_client(client: &Client) -> RedisResult<Self> {
        let connection = client.get_tokio_connection_manager().await?;
        Ok(Self { connection })
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

    fn format_scheduled_set_key(queue_name: &str) -> String {
        format!("scheduled:{}", queue_name)
    }

    fn format_stats_key() -> String {
        "ironworker:stats".to_string()
    }

    fn format_failed_queues_key() -> String {
        "ironworker:failed".to_string()
    }

    fn format_queues_key() -> String {
        "ironworker:queues".to_string()
    }
}

#[async_trait]
impl Broker for RedisBroker {
    type Error = RedisError;

    async fn enqueue(
        &self,
        queue: &str,
        message: SerializableMessage,
        at: Option<DateTime<Utc>>,
    ) -> Result<(), Self::Error> {
        let mut conn = self.connection.clone();

        if let Some(at) = at {
            let scheduled_set_key = Self::format_scheduled_set_key(queue);
            let message = RedisMessage::from(message);
            conn.zadd::<_, _, _, ()>(scheduled_set_key, message, at.timestamp_millis())
                .await?;
            conn.hincr(Self::format_stats_key(), "scheduled", 1).await?;
        } else {
            let queue_key = Self::format_queue_key(queue);
            let message = RedisMessage::from(message);
            conn.lpush(&queue_key, message).await?;
            conn.sadd(Self::format_queues_key(), queue_key).await?;
            conn.hincr(Self::format_stats_key(), "enqueued", 1).await?;
        }

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
        let now = Utc::now();
        conn.zadd(&deadletter_key, message, now.timestamp_millis())
            .await?;

        conn.sadd(Self::format_failed_queues_key(), deadletter_key)
            .await?;
        Ok(())
    }

    async fn dequeue(&self, queue: &str) -> Result<Option<SerializableMessage>, Self::Error> {
        let mut conn = self.connection.clone();
        let from = Self::format_queue_key(queue);

        let item = conn.rpop::<_, RedisMessage>(&from, None).await.ok();

        if let Some(item) = item {
            return Ok(Some(item.into()));
        }

        // Try and get something that's scheduled now
        let from = Self::format_scheduled_set_key(queue);
        let script = Script::new(SCRIPT_SRC);

        let item = script
            .prepare_invoke()
            .key(from)
            .arg(Utc::now().timestamp_millis())
            .invoke_async::<_, Option<RedisMessage>>(&mut conn)
            .await?;

        if let Some(item) = item {
            self.enqueue(queue, item.into(), None).await?;
        }

        tokio::time::sleep(Duration::from_millis(5000)).await;
        Ok(None)
    }

    async fn heartbeat(&self, worker_id: &str) -> Result<(), Self::Error> {
        let mut conn = self.connection.clone();
        let worker_key = Self::format_worker_info_key(worker_id);

        conn.hset(&worker_key, "last_seen_at", Utc::now().timestamp_millis())
            .await?;
        conn.expire(&worker_key, 60).await?;
        Ok(())
    }

    async fn acknowledge_processed(
        &self,
        _from: &str,
        message: SerializableMessage,
    ) -> Result<(), Self::Error> {
        let mut connection = self.connection.clone();
        let is_retried = message.err.is_some();

        connection
            .hincr(Self::format_stats_key(), "processed", 1)
            .await?;
        if is_retried {
            connection
                .hincr(Self::format_stats_key(), "failed", 1)
                .await?;
        }
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
        let mut conn = self.connection.clone();
        let queues = conn
            .smembers::<_, Vec<String>>(Self::format_queues_key())
            .await
            .unwrap();

        let futs: Vec<_> = queues
            .into_iter()
            .map(|queue_name| async move {
                let mut conn = self.connection.clone();
                let queue_size = conn.llen(&queue_name).await.unwrap();

                QueueInfo {
                    name: queue_name,
                    size: queue_size,
                }
            })
            .collect();
        future::join_all(futs).await
    }

    async fn stats(&self) -> Stats {
        let mut connection = self.connection.clone();
        let stats = connection
            .get::<_, HashMap<String, u64>>(Self::format_stats_key())
            .await
            .unwrap_or_default();

        Stats {
            processed: stats.get("processed").cloned().unwrap_or_default(),
            failed: stats.get("failed").cloned().unwrap_or_default(),
            scheduled: stats.get("scheduled").cloned().unwrap_or_default(),
            enqueued: stats.get("enqueued").cloned().unwrap_or_default(),
        }
    }

    async fn deadlettered(&self) -> Vec<DeadletteredInfo> {
        let mut connection = self.connection.clone();
        let failed_queues = connection
            .smembers::<_, Vec<String>>(Self::format_failed_queues_key())
            .await
            .unwrap_or_default();

        let futs: Vec<_> = failed_queues
            .into_iter()
            .map(|queue| async {
                let mut conn = self.connection.clone();
                let messages = conn
                    .zrange::<_, Vec<RedisMessage>>(queue, 0, -1)
                    .await
                    .unwrap_or_default();
                let messages: Vec<SerializableMessage> =
                    messages.into_iter().map(Into::into).collect();
                let messages: Vec<DeadletteredInfo> = messages
                    .into_iter()
                    .map(|message| DeadletteredInfo {
                        err: message.err,
                        job_id: message.job_id,
                    })
                    .collect();
                messages
            })
            .collect();

        future::join_all(futs).await.into_iter().flatten().collect()
    }

    async fn scheduled(&self) -> Vec<ScheduledInfo> {
        vec![]
    }
}
