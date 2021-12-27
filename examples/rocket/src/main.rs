#![deny(clippy::all)]

#[macro_use]
extern crate rocket;

use std::{error::Error, sync::Arc};

use ironworker_core::{
    IntoTask, IronworkerApplication, IronworkerApplicationBuilder, Message, PerformableTask,
};
use ironworker_redis::RedisBroker;
use ironworker_rocket::IronworkerFairing;
use rocket::State;

fn test(_message: Message<u32>) -> Result<(), Box<dyn Error + Send>> {
    Ok(())
}

#[get("/")]
async fn index(app: &State<Arc<IronworkerApplication<RedisBroker>>>) {
    let futs: Vec<_> = (0..5)
        .map(|_| {
            let app = app.inner().clone();
            tokio::task::spawn(async move {
                for _ in 0..20000 {
                    test.task().perform_later(&app, 123).await;
                }
            })
        })
        .collect();

    futures::future::join_all(futs).await;
}

#[rocket::launch]
async fn rocket() -> _ {
    let ironworker_app = IronworkerApplicationBuilder::default()
        .broker(RedisBroker::new("redis://localhost:6379").await)
        .register_task(test.task())
        .build();
    rocket::build()
        .attach(IronworkerFairing::new("/queues", ironworker_app))
        .mount("/", routes![index])
}
