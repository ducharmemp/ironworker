#![deny(clippy::all)]

use std::error::Error;

use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_sqs::Endpoint;
use ironworker_core::{
    ConfigurableTask, ErrorRetryConfiguration, IntoTask, IronworkerApplicationBuilder,
    IronworkerMiddleware, Message, PerformableTask,
};
use ironworker_sqs::SqsBroker;
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

fn my_panicking_task(_message: Message<u32>) -> Result<(), TestEnum> {
    Err(TestEnum::Failed)
}

#[derive(Snafu, Debug)]
enum TestEnum {
    #[snafu(display("The task failed"))]
    Failed,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let shared_config = aws_config::load_from_env().await;
    let sqs_config_builder = aws_sdk_sqs::config::Builder::from(&shared_config);
    let sqs_config_builder = sqs_config_builder.endpoint_resolver(Endpoint::immutable(
        http::Uri::from_static("http://localhost:9324"),
    ));

    let app = IronworkerApplicationBuilder::default()
        .broker(SqsBroker::from_builder(sqs_config_builder).await)
        .register_task(my_task.task().queue_as("fake").retries(2))
        .register_task(my_complex_task.task().queue_as("complex"))
        .register_task(my_async_task.task().queue_as("async"))
        .register_task(test_multiple.task())
        .register_task(my_panicking_task.task().retries(5))
        .register_middleware(Middleware)
        .build();

    // my_task.task().perform_now(&app, 123).await.unwrap();
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

    for _ in 0..2 {
        my_panicking_task.task().perform_later(&app, 123).await?;
        my_task.task().perform_later(&app, 123).await?;
        my_async_task.task().perform_later(&app, 123).await?;
    }

    app.run().await;

    Ok(())
}
