#[macro_use]
extern crate rocket;

use std::sync::Arc;

use askama::Template;
use async_trait::async_trait;
use ironworker_core::info::{ApplicationInfo, QueueInfo, WorkerInfo};
use ironworker_core::{Broker, IronworkerApplication};
use ironworker_redis::RedisBroker;
use rocket::fairing::{Fairing, Info, Kind, Result};
use rocket::{tokio, State};
use rocket::{Build, Orbit, Rocket};

#[derive(Template)]
#[template(path = "index.html")]
struct OverviewTemplate {
    workers: Vec<WorkerInfo>,
    queues: Vec<QueueInfo>,
}

#[get("/")]
async fn index(app: &State<Arc<IronworkerApplication<RedisBroker>>>) -> OverviewTemplate {
    let workers = app.workers().await;
    let queues = app.queues().await;

    OverviewTemplate { workers, queues }
}

pub struct IronworkerFairing<B: Broker>(&'static str, Arc<IronworkerApplication<B>>);

impl<B: Broker> IronworkerFairing<B> {
    pub fn new(base: &'static str, app: IronworkerApplication<B>) -> IronworkerFairing<B> {
        IronworkerFairing(base, Arc::new(app))
    }
}

#[async_trait]
impl<B: Broker + 'static> Fairing for IronworkerFairing<B> {
    fn info(&self) -> Info {
        Info {
            name: "Ironworker",
            kind: Kind::Ignite | Kind::Liftoff,
        }
    }

    async fn on_ignite(&self, rocket: Rocket<Build>) -> Result {
        let rocket = rocket.mount(self.0, routes![index]);
        let rocket = rocket.manage(self.1.clone());
        Ok(rocket)
    }

    async fn on_liftoff(&'_ self, rocket: &'_ Rocket<Orbit>) {
        if let Some(app) = rocket.state::<Arc<IronworkerApplication<RedisBroker>>>() {
            let run_handle = app.clone();
            let shutdown_handle = app.clone();
            let shutdown = rocket.shutdown();
            tokio::spawn(async move {
                run_handle.run().await;
            });

            tokio::spawn(async move {
                shutdown.await;
                shutdown_handle.shutdown();
            });
        }
    }
}
