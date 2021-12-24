#[macro_use]
extern crate rocket;

use std::sync::Arc;

use ironworker_core::{IntoTask, IronworkerApplication, Message, PerformableTask};
use ironworker_redis::RedisBroker;
use ironworker_rocket::IronworkerFairing;
use rocket::State;

fn test(message: Message<u32>) {
    dbg!(message);
}

#[get("/")]
async fn index(app: &State<Arc<IronworkerApplication<RedisBroker>>>) {
    for _ in 0..10 {
        test.task().perform_later(app, 123).await;
    }
}

#[rocket::launch]
async fn rocket() -> _ {
    let mut ironworker_app =
        IronworkerApplication::new(RedisBroker::new("redis://localhost:6379").await);
    ironworker_app.register_task(test.task()).await;
    rocket::build()
        .attach(IronworkerFairing::new("/queues", ironworker_app))
        .mount("/", routes![index])
}
