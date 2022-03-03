use actix_web::{web, HttpResponse, Scope};
use askama_actix::Template;
use ironworker_core::{
    broker::Broker,
    enqueuer::Enqueuer,
    info::{ApplicationInfo, BrokerInfo, QueueInfo, WorkerInfo},
    message::SerializableMessage,
};
use serde::Deserialize;
use serde_json::Value;

use crate::application::IronworkerApplication;

#[derive(Template)]
#[template(path = "index.html")]
struct OverviewTemplate {
    workers: Vec<WorkerInfo>,
    queues: Vec<QueueInfo>,
}

#[derive(Template)]
#[template(path = "stats.html")]
struct StatsTemplate {
    processed: u64,
    failed: u64,
    enqueued: u64,
    scheduled: u64,
}

#[derive(Template)]
#[template(path = "failed.html")]
struct FailedTemplate {
    deadlettered: Vec<SerializableMessage>,
}

#[derive(Deserialize, Debug)]
struct JobRetry {
    task: String,
    payload: Value,
}

async fn overview_get<B: Broker + BrokerInfo>(
    ironworker: web::Data<IronworkerApplication<B>>,
) -> OverviewTemplate {
    let workers = ironworker.workers().await;
    let queues = ironworker.queues().await;

    OverviewTemplate { workers, queues }
}

async fn stats_get<B: Broker + BrokerInfo>(
    ironworker: web::Data<IronworkerApplication<B>>,
) -> StatsTemplate {
    let stats = ironworker.stats().await;
    StatsTemplate {
        processed: stats.processed,
        failed: stats.failed,
        enqueued: stats.enqueued,
        scheduled: stats.scheduled,
    }
}

async fn failed_get<B: Broker + BrokerInfo>(
    ironworker: web::Data<IronworkerApplication<B>>,
) -> FailedTemplate {
    let deadlettered = ironworker.deadlettered().await;

    FailedTemplate { deadlettered }
}

async fn failed_post<B: Broker + BrokerInfo>(
    web::Form(job_retry): web::Form<JobRetry>,
    ironworker: web::Data<IronworkerApplication<B>>,
) -> HttpResponse {
    let retry_result = ironworker
        .enqueue(&job_retry.task, job_retry.payload, Default::default())
        .await;
    if retry_result.is_err() {
        return HttpResponse::UnprocessableEntity().finish();
    }
    HttpResponse::Ok().finish()
}

pub fn endpoints<B: Broker + BrokerInfo>(ironworker: IronworkerApplication<B>) -> Scope {
    web::scope("ironworker")
        .service(web::resource("/").route(web::get().to(overview_get::<B>)))
        .service(
            web::resource("failed")
                .route(web::get().to(failed_get::<B>))
                .route(web::post().to(failed_post::<B>)),
        )
        .service(web::resource("stats").route(web::get().to(stats_get::<B>)))
        .app_data(web::Data::new(ironworker))
}
