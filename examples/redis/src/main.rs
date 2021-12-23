use anyhow::Result;
use ironworker_core::{Broker, IntoTask, Message, Task};
use ironworker_redis::RedisBroker;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Complex {
    val: String,
    other: i32,
}

impl Complex {
    fn new(val: String, other: i32) -> Complex {
        Complex {
            val: val,
            other: other,
        }
    }
}

fn my_task(message: Message<u32>) {
    dbg!("CALLED", message.into_inner());
}

fn my_complex_task(message: Message<Complex>) {
    dbg!("CALLED", message.into_inner());
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut broker = RedisBroker::new("redis://localhost:6379").await;
    broker.register_task(my_task.task()).await;
    broker.register_task(my_complex_task.task()).await;
    my_task.task().perform_now(&broker, 123).await;
    my_task.task().perform_later(&broker, 123).await;
    my_complex_task
        .task()
        .perform_later(&broker, Complex::new("Hello world".to_string(), 123421))
        .await;
    broker.work().await;
    Ok(())
}
