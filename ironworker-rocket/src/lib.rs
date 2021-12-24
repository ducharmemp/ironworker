#[macro_use]
extern crate rocket;

use std::sync::Arc;

use askama::Template;
use async_trait::async_trait;
use ironworker_core::{IntoTask, IronworkerApplication, Message, PerformableTask, WorkerState};
use ironworker_redis::RedisBroker;
use rocket::fairing::{Fairing, Info, Kind, Result};
use rocket::{tokio, State};
use rocket::{Build, Orbit, Rocket};

#[derive(Template)]
#[template(path = "index.html")]
struct OverviewTemplate {
    workers: Vec<WorkerState>,
    queues: Vec<String>,
}

fn test(message: Message<u32>) {
    dbg!(message);
}

#[get("/")]
async fn index(app: &State<Arc<IronworkerApplication<RedisBroker>>>) -> OverviewTemplate {
    for _ in 0..10 {
        test.task().perform_later(app, 123).await;
    }
    let workers = app.list_workers().await;
    let queues = app.list_queues().await;

    OverviewTemplate { workers, queues }
}

pub struct IronworkerFairing(&'static str);

impl IronworkerFairing {
    pub fn new(base: &'static str) -> Self {
        Self(base)
    }
}

#[async_trait]
impl Fairing for IronworkerFairing {
    fn info(&self) -> Info {
        Info {
            name: "Ironworker",
            kind: Kind::Ignite | Kind::Liftoff,
        }
    }

    async fn on_ignite(&self, rocket: Rocket<Build>) -> Result {
        let mut ironworker_app =
            IronworkerApplication::new(RedisBroker::new("redis://localhost:6379").await);
        ironworker_app.register_task(test.task()).await;
        let rocket = rocket.mount(self.0, routes![index]);
        let rocket = rocket.manage(Arc::new(ironworker_app));
        Ok(rocket)
    }

    async fn on_liftoff(&'_ self, rocket: &'_ Rocket<Orbit>) {
        if let Some(app) = rocket.state::<Arc<IronworkerApplication<RedisBroker>>>() {
            let app = app.clone();
            tokio::spawn(async move {
                app.run().await;
            });
        }
    }
}
