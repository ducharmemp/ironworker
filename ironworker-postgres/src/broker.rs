use std::collections::HashMap;

use async_trait::async_trait;
use serde::Serialize;
use serde_json::to_string;
use ironworker_core::{Broker, SerializableMessage};
use ironworker_core::{Message, Task, Worker};
use sqlx::postgres::{PgPoolOptions, PgPool};

use crate::error::Result;
use crate::worker::PostgresWorker;

pub struct PostgresBroker<'application> {
    uri: &'application str,
    workers: HashMap<&'application str, Box<dyn Worker + Sync + Send>>,
    pool: PgPool
}

impl<'application> PostgresBroker<'application> {
    pub async fn new(uri: &'application str) -> PostgresBroker<'application> {
        Self {
            uri,
            workers: HashMap::new(),
            pool: PgPoolOptions::new().max_connections(1).connect(uri).await.unwrap()
        }
    }
}

#[async_trait]
impl<'application> Broker for PostgresBroker<'application> {
    async fn register_task<T: Task + Send>(&mut self, task: T) {
        self.workers.insert(
            task.name(),
            Box::new(PostgresWorker::from_uri(self.uri, task.name())
                .await
                .expect("Could not create worker")),
        );
    }

    async fn enqueue<T: Into<SerializableMessage> + Send + Serialize>(&self, queue: &str, payload: T) {
        let message: SerializableMessage = payload.into();
        sqlx::query!("insert into jobs (queue, payload, enqueued_at) values ($1, $2, now())", queue, to_string(&message.payload).unwrap()).execute(&self.pool).await;
    }

    async fn work(self) {
        loop {
            self.pool.begin().await.unwrap();
            let jobs = sqlx::query!("select * from jobs order by enqueued_at desc for update limit 1").fetch_one(&self.pool).await.unwrap();
            dbg!(jobs);
        }
    }
}