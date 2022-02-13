#![deny(clippy::all, clippy::cargo)]
use std::sync::Arc;

use askama_axum::Template;
use axum::{extract::Extension, routing::get, AddExtensionLayer, Router};
use ironworker_core::{
    info::{ApplicationInfo, BrokerInfo, DeadletteredInfo, QueueInfo, WorkerInfo},
    Broker, IronworkerApplication,
};

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
    deadlettered: Vec<DeadletteredInfo>,
}

async fn failed_get(Extension(ironworker): Extension<Arc<dyn ApplicationInfo>>) -> FailedTemplate {
    let deadlettered = ironworker.deadlettered().await;

    FailedTemplate { deadlettered }
}

pub fn endpoints<B: Broker + BrokerInfo>(ironworker: Arc<IronworkerApplication<B>>) -> Router {
    let ironworker_info: Arc<dyn ApplicationInfo> = ironworker.clone();
    Router::new()
        .route("/ironworker/", get(overview_get))
        .route("/ironworker/failed", get(failed_get))
        .route("/ironworker/stats", get(stats_get))
        .layer(AddExtensionLayer::new(ironworker_info))
}
