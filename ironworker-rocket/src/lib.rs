#[macro_use]
extern crate rocket;

mod broker;

use std::collections::HashMap;

use askama::Template;
use async_trait::async_trait;
use ironworker_core::{Broker, IntoTask, Message, Task, Worker};
use ironworker_redis::RedisBroker;
use rocket::fairing::{Fairing, Info, Kind, Result};
use rocket::http::{ContentType, Method, Status};
use rocket::local::blocking::Client;
use rocket::{Build, Data, Request, Response, Rocket};

#[derive(Template)]
#[template(path = "index.html")]
struct OverviewTemplate {
    workers: Vec<String>,
    queues: Vec<String>,
}

fn test(message: Message<u32>) {}

#[get("/")]
async fn index(broker: &broker::Broker<'_>) -> OverviewTemplate {
    let broker = &broker.0;
    let workers = broker.workers().await;
    let queues = broker.queues().await;

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
            kind: Kind::Ignite,
        }
    }

    async fn on_ignite(&self, rocket: Rocket<Build>) -> Result {
        let mut broker = RedisBroker::new("redis://localhost:6379").await;
        broker.register_task(test.task()).await;
        let rocket = rocket.mount(self.0, routes![index]);
        let rocket = rocket.manage(broker::Broker(broker));
        Ok(rocket)
    }
}
