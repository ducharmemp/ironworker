#![deny(clippy::all, clippy::cargo)]
use std::sync::Arc;

use serde::Deserialize;
use askama_axum::Template;
use axum::{extract::{Extension, Form}, routing::get, AddExtensionLayer, Router};
use ironworker_core::{
    SerializableMessage,
    info::{ApplicationInfo, BrokerInfo, QueueInfo, WorkerInfo},
    Broker, IronworkerApplication,
};
use uuid::Uuid;

#[derive(Template)]
#[template(path = "index.html")]
struct OverviewTemplate {
    workers: Vec<WorkerInfo>,
    queues: Vec<QueueInfo>,
}

async fn overview_get(
    Extension(ironworker): Extension<Arc<dyn ApplicationInfo>>,
) -> OverviewTemplate {
    let workers = ironworker.workers().await;
    let queues = ironworker.queues().await;

    OverviewTemplate { workers, queues }
}

#[derive(Template)]
#[template(path = "stats.html")]
struct StatsTemplate {
    processed: u64,
    failed: u64,
    enqueued: u64,
}

async fn stats_get(Extension(ironworker): Extension<Arc<dyn ApplicationInfo>>) -> StatsTemplate {
    let stats = ironworker.stats().await;
    StatsTemplate {
        processed: stats.processed,
        failed: stats.failed,
        enqueued: stats.enqueued,
    }
}

#[derive(Template)]
#[template(path = "failed.html")]
struct FailedTemplate {
    deadlettered: Vec<SerializableMessage>,
}

#[derive(Deserialize, Debug)]
struct JobRetry {
    job_id: Uuid
}

async fn failed_get(Extension(ironworker): Extension<Arc<dyn ApplicationInfo>>) -> FailedTemplate {
    let deadlettered = ironworker.deadlettered().await;

    FailedTemplate { deadlettered }
}

async fn failed_post(Form(job_retry): Form<JobRetry>, Extension(ironworker): Extension<Arc<dyn ApplicationInfo>>) {
    
}

pub fn endpoints<B: Broker + BrokerInfo>(ironworker: Arc<IronworkerApplication<B>>) -> Router {
    let ironworker_info: Arc<dyn ApplicationInfo> = ironworker;
    Router::new()
        .route("/ironworker/", get(overview_get))
        .route("/ironworker/failed", get(failed_get).post(failed_post))
        .route("/ironworker/stats", get(stats_get))
        .layer(AddExtensionLayer::new(ironworker_info))
}
