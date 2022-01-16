#![deny(clippy::all)]
use async_trait::async_trait;
use ironworker_core::{
    ConfigurableTask, ErrorRetryConfiguration, IntoTask, IronworkerApplicationBuilder,
    IronworkerMiddleware, Message, PerformableTask,
};
use ironworker_redis::RedisBroker;
use serde::{Deserialize, Serialize};
use snafu::Snafu;

struct Middleware;

#[async_trait]
impl IronworkerMiddleware for Middleware {
    async fn on_task_start(&self) {
        // dbg!("Started");
    }

    async fn on_task_completion(&self) {
        // dbg!("Completed");
    }

    async fn on_task_failure(&self) {
        // dbg!("Failed");
    }
}

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

fn my_task(_message: Message<u32>) -> Result<(), TestEnum> {
    Ok(())
}

async fn my_async_task(_message: Message<u32>) -> Result<(), TestEnum> {
    Ok(())
}

fn my_complex_task(_message: Message<Complex>) -> Result<(), TestEnum> {
    Ok(())
}

fn test_multiple(_message: Message<Complex>, _test: &u32) -> Result<(), TestEnum> {
    Ok(())
}

fn my_panicking_task(_message: Message<u32>) -> Result<(), TestEnum> {
    Err(TestEnum::Failed)
}

#[derive(Snafu, Debug)]
enum TestEnum {
    #[snafu(display("The task failed"))]
    Failed,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let app = IronworkerApplicationBuilder::default()
        .broker(RedisBroker::new("redis://localhost:6379").await)
        .register_task(my_task.task().queue_as("fake").retries(2))
        .register_task(my_complex_task.task().queue_as("complex"))
        .register_task(my_async_task.task().queue_as("async"))
        .register_task(test_multiple.task())
        .register_task(my_panicking_task.task().retries(5))
        .register_middleware(Middleware)
        .build();

    my_task.task().perform_later(&app, 123).await?;
    my_complex_task
        .task()
        .perform_later(&app, Complex::new("Hello world".to_string(), 123421))
        .await?;

    my_complex_task
        .task()
        .retry_on::<TestEnum>(ErrorRetryConfiguration::default().with_attempts(5))
        .perform_later(&app, Complex::new("Hello world".to_string(), 123421))
        .await?;

    for _ in 0..5 {
        my_panicking_task.task().perform_later(&app, 123).await?;
        my_task.task().perform_later(&app, 123).await?;
        my_async_task.task().perform_later(&app, 123).await?;
    }

    app.run().await;

    Ok(())
}
