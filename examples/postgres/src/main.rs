#![deny(clippy::all)]

use anyhow::Result;
use ironworker_core::{IntoTask, IronworkerApplicationBuilder, Message, PerformableTask};
use ironworker_postgres::PostgresBroker;
use std::{error::Error, sync::Arc};

fn my_task(message: Message<u32>) -> Result<(), Box<dyn Error + Send>> {
    dbg!("CALLED", message.into_inner());
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let app = IronworkerApplicationBuilder::default()
        .broker(PostgresBroker::new("postgres://test:test@localhost/test").await)
        .register_task(my_task.task())
        .build();
    my_task.task().perform_later(&app, 123).await;
    let app = Arc::new(app);
    let app2 = app.clone();
    tokio::spawn(async move { app.run().await });
    my_task.task().perform_later(&app2, 123).await;
    Ok(())
}
