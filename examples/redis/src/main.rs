use std::{error::Error, sync::Arc};

use anyhow::Result;
use ironworker_core::{IntoTask, IronworkerApplication, Message, PerformableTask};
use ironworker_redis::RedisBroker;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Complex {
    val: String,
    other: i32,
}

impl Complex {
    fn new(val: String, other: i32) -> Complex {
        Complex { val, other }
    }
}

fn my_task(_message: Message<u32>) -> Result<(), Box<dyn Error + Send>> {
    // dbg!("CALLED", message.into_inner());
    Ok(())
}

async fn my_async_task(_message: Message<u32>) -> Result<(), Box<dyn Error + Send>> {
    // dbg!("async", message.into_inner());s
    Ok(())
}

fn my_complex_task(_message: Message<Complex>) -> Result<(), Box<dyn Error + Send>> {
    // dbg!("CALLED", message.into_inner());
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut app = IronworkerApplication::new(RedisBroker::new("redis://localhost:6379").await);
    app.register_task(my_task.task()).await;
    app.register_task(my_complex_task.task()).await;
    app.register_task(my_async_task.task()).await;

    my_task.task().perform_now(&app, 123).await;
    my_task.task().perform_later(&app, 123).await;
    my_complex_task
        .task()
        .perform_later(&app, Complex::new("Hello world".to_string(), 123421))
        .await;
    let app = Arc::new(app);
    let app2 = app.clone();
    tokio::spawn(async move { app.run().await });
    for _ in 0..10000 {
        my_task.task().perform_later(&app2, 123).await;
        my_async_task.task().perform_later(&app2, 123).await;
    }
    Ok(())
}
