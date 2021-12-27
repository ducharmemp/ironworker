#![deny(clippy::all)]

use std::error::Error;

use anyhow::Result;
use ironworker_core::{
    ConfigurableTask, IntoTask, IronworkerApplicationBuilder, Message, PerformableTask,
};
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
    // dbg!("async", message.into_inner());
    Ok(())
}

fn my_complex_task(_message: Message<Complex>) -> Result<(), Box<dyn Error + Send>> {
    // dbg!("CALLED", message.into_inner());
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let app = IronworkerApplicationBuilder::default()
        .broker(RedisBroker::new("redis://localhost:6379").await)
        .register_task(my_task.task().queue_as("fake").retries(2))
        .register_task(my_complex_task.task().queue_as("complex"))
        .register_task(my_async_task.task().queue_as("async"))
        .build();

    my_task.task().perform_now(&app, 123).await.unwrap();
    my_task.task().perform_later(&app, 123).await;
    my_complex_task
        .task()
        .perform_later(&app, Complex::new("Hello world".to_string(), 123421))
        .await;

    for _ in 0..10000 {
        my_task.task().perform_later(&app, 123).await;
        my_async_task.task().perform_later(&app, 123).await;
    }

    app.run().await;

    Ok(())
}
