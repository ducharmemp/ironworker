#![deny(clippy::all)]

use std::error::Error;

use anyhow::Result;
use ironworker_core::{
    ConfigurableTask, ErrorRetryConfiguration, IntoTask, IronworkerApplicationBuilder, Message,
    PerformableTask,
};
use ironworker_redis::RedisBroker;
use serde::{Deserialize, Serialize};
use thiserror::Error;

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
    Ok(())
}

async fn my_async_task(_message: Message<u32>) -> Result<(), Box<dyn Error + Send>> {
    Ok(())
}

fn my_complex_task(_message: Message<Complex>) -> Result<(), Box<dyn Error + Send>> {
    Ok(())
}

fn test_multiple(_message: Message<Complex>, _test: &u32) -> Result<(), Box<dyn Error + Send>> {
    Ok(())
}

fn my_panicking_task(_message: Message<u32>) -> Result<(), Box<dyn Error + Send>> {
    panic!("Panic!");
    Ok(())
}

#[derive(Error, Debug)]
enum TestEnum {
    #[error("The task failed")]
    Failed,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let app = IronworkerApplicationBuilder::default()
        .broker(RedisBroker::new("redis://localhost:6379").await)
        .register_task(my_task.task().queue_as("fake").retries(2))
        .register_task(my_complex_task.task().queue_as("complex"))
        .register_task(my_async_task.task().queue_as("async"))
        .register_task(test_multiple.task())
        .register_task(my_panicking_task.task())
        .build();

    // my_task.task().perform_now(&app, 123).await.unwrap();
    my_task.task().perform_later(&app, 123).await;
    my_complex_task
        .task()
        .perform_later(&app, Complex::new("Hello world".to_string(), 123421))
        .await;

    my_complex_task
        .task()
        .retry_on(
            TestEnum::Failed,
            ErrorRetryConfiguration::default().with_attempts(5),
        )
        .perform_later(&app, Complex::new("Hello world".to_string(), 123421))
        .await;

    for _ in 0..20000 {
        my_panicking_task.task().perform_later(&app, 123).await;
        my_task.task().perform_later(&app, 123).await;
        my_async_task.task().perform_later(&app, 123).await;
    }

    app.run().await;

    Ok(())
}
