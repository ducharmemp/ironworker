#![deny(clippy::all, clippy::cargo)]
use std::sync::Arc;

use askama_axum::Template;
use axum::{
    extract::{Extension, Form},
    routing::get,
    AddExtensionLayer, Router,
};
use ironworker_core::{
    info::{ApplicationInfo, BrokerInfo, QueueInfo, WorkerInfo},
    Broker, Enqueuer, IronworkerApplication, SerializableMessage,
};
use serde::Deserialize;
use serde_json::Value;

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
    Extension(ironworker): Extension<Arc<IronworkerApplication<B>>>,
) -> OverviewTemplate {
    let workers = ironworker.workers().await;
    let queues = ironworker.queues().await;

    OverviewTemplate { workers, queues }
}

async fn stats_get<B: Broker + BrokerInfo>(
    Extension(ironworker): Extension<Arc<IronworkerApplication<B>>>,
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
    Extension(ironworker): Extension<Arc<IronworkerApplication<B>>>,
) -> FailedTemplate {
    let deadlettered = ironworker.deadlettered().await;

    FailedTemplate { deadlettered }
}

async fn failed_post<B: Broker + BrokerInfo>(
    Form(job_retry): Form<JobRetry>,
    Extension(ironworker): Extension<Arc<IronworkerApplication<B>>>,
) {
    ironworker
        .enqueue(&job_retry.task, job_retry.payload, Default::default())
        .await
        .unwrap();
}

pub fn endpoints<B: Broker + BrokerInfo>(ironworker: Arc<IronworkerApplication<B>>) -> Router {
    Router::new()
        .route("/ironworker/", get(overview_get::<B>))
        .route(
            "/ironworker/failed",
            get(failed_get::<B>).post(failed_post::<B>),
        )
        .route("/ironworker/stats", get(stats_get::<B>))
        .layer(AddExtensionLayer::new(ironworker))
}
