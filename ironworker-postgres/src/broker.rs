use async_trait::async_trait;
use ironworker_core::{Broker, SerializableMessage, WorkerState, DeadLetterMessage};
use serde_json::{from_str, to_string, Value};
use sqlx::postgres::{PgPool, PgPoolOptions};

pub struct PostgresBroker {
    pool: PgPool,
}

impl PostgresBroker {
    pub async fn new(uri: &str) -> PostgresBroker {
        Self {
            pool: PgPoolOptions::new()
                .max_connections(1)
                .connect(uri)
                .await
                .unwrap(),
        }
    }
}

#[async_trait]
impl Broker for PostgresBroker {
    async fn enqueue(&self, queue: &str, message: SerializableMessage) {
        sqlx::query!(
            "insert into jobs (queue, payload, enqueued_at) values ($1, $2, $3)",
            queue,
            to_string(&message.payload).unwrap(),
            message.enqueued_at
        )
        .execute(&self.pool)
        .await
        .unwrap();
    }

    async fn deadletter(&self, message: DeadLetterMessage) {

    }

    async fn dequeue(&self, queue: &str) -> Option<SerializableMessage> {
        self.pool.begin().await.unwrap();
        let job =
                sqlx::query!("select job_id, task, payload, enqueued_at from jobs where queue = $1 order by enqueued_at desc for update skip locked limit 1", queue)
                    .fetch_optional(&self.pool)
                    .await
                    .unwrap();
        let job = job?;
        Some(SerializableMessage {
            job_id: job.job_id.unwrap(),
            task: job.task.unwrap(),
            payload: from_str::<Value>(&job.payload.unwrap()).unwrap(),
            enqueued_at: job.enqueued_at.unwrap(),
        })
    }

    async fn list_workers(&self) -> Vec<WorkerState> {
        vec![]
    }

    async fn list_queues(&self) -> Vec<String> {
        vec![]
    }

    async fn heartbeat(&self, application_id: &str) {}
    async fn deregister_worker(&self, application_id: &str) {}
}
