use std::sync::Arc;

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
        Complex {
            val,
            other,
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
    let mut app = IronworkerApplication::new(RedisBroker::new("redis://localhost:6379").await);
    app.register_task(my_task.task()).await;
    app.register_task(my_complex_task.task()).await;
    my_task.task().perform_now(&app, 123).await;
    my_task.task().perform_later(&app, 123).await;
    my_complex_task
        .task()
        .perform_later(&app, Complex::new("Hello world".to_string(), 123421))
        .await;
    let app = Arc::new(app);
    let app2 = app.clone();
    tokio::spawn(async move { app.run().await });
    my_task.task().perform_later(&app2, 123).await;
    Ok(())
}
