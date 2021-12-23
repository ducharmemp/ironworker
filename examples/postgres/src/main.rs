use anyhow::Result;
use ironworker_core::{Broker, IntoTask, Message, Task};
use ironworker_postgres::PostgresBroker;
use sqlx::postgres::PgPoolOptions;

fn my_task(message: Message<u32>) {
    dbg!("CALLED", 123);
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut broker = PostgresBroker::new("postgres://test:test@localhost/test").await;
    broker.register_task(my_task.task()).await;
    // my_task.task().perform_now(&broker,123).await;
    my_task.task().perform_later(&broker, 123).await;
    broker.work().await;
    Ok(())
}
