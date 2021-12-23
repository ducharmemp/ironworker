use async_trait::async_trait;
use ironworker_core::{Worker, SerializableMessage};
use sqlx::postgres::{PgListener, PgPool};

use crate::error::Result;

pub struct PostgresWorker<'worker> {
    listener: PgListener,
    queue: &'worker str
}

impl<'worker> PostgresWorker<'worker> {
    pub async fn from_uri(uri: &str, queue: &'worker str) -> Result<PostgresWorker<'worker>> {
        Ok(Self{
            listener: PgListener::connect(uri).await?,
            queue
        })
    }

    pub async fn from_sqlx_pool(pool: &PgPool, queue: &'worker str) -> Result<PostgresWorker<'worker>> {
        Ok(Self{
            listener: PgListener::connect_with(pool).await?,
            queue
        })
    }
}

#[async_trait]
impl<'worker> Worker for PostgresWorker<'worker> {
    async fn work(&self, item: SerializableMessage) {
    }
    async fn register(&self) {}
    async fn heartbeat(&self) {}
    async fn deregister(&self) {}
}
