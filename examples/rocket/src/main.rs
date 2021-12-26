#[macro_use]
extern crate rocket;

use std::{error::Error, sync::Arc};

use ironworker_core::{IntoTask, IronworkerApplicationBuilder, Message, PerformableTask, IronworkerApplication};
use ironworker_redis::RedisBroker;
use ironworker_rocket::IronworkerFairing;
use rocket::State;

fn test(message: Message<u32>) -> Result<(), Box<dyn Error + Send>> {
    dbg!(message);
    Ok(())
}

#[get("/")]
async fn index(app: &State<Arc<IronworkerApplication<RedisBroker>>>) {
    for _ in 0..10 {
        test.task().perform_later(app, 123).await;
    }
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
