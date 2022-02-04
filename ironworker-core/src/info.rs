//! Informational structs for Ironworker applications.
//!
//! These structs provide insight into worker/queue/application internals, such as number of items processed, sizes of queues, and
//! jobs that have failed.
use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::{Broker, IronworkerApplication};

/// A struct describing a Worker, including the queue it's listening on, the name of the worker (auto-generated),
/// and the last time there was a heartbeat.
#[derive(Debug)]
pub struct WorkerInfo {
    /// The name of the worker, usually auto-generated
    pub name: String,
    /// The queue that the worker is listening on
    pub queue: Option<String>,
    /// The last time the worker posted a heartbeat to the backing datastore
    pub last_seen_at: Option<DateTime<Utc>>,
}

/// Metadata struct describing a given queue
#[derive(Debug)]
pub struct QueueInfo {
    /// The name of the queue
    pub name: String,
    /// The rough size of the queue
    pub size: usize,
}

/// Metadata struct describing
#[derive(Clone, Copy, Debug)]
pub struct Stats {
    pub processed: usize,
    pub failed: usize,
    pub scheduled: usize,
    pub enqueued: usize,
}

#[allow(missing_copy_implementations, missing_debug_implementations)]
pub struct DeadletteredInfo {}

#[allow(missing_copy_implementations, missing_debug_implementations)]
pub struct ScheduledInfo {}

#[async_trait]
pub trait ApplicationInfo {
    async fn workers(&self) -> Vec<WorkerInfo>;
    async fn queues(&self) -> Vec<QueueInfo>;
    async fn stats(&self) -> Stats;
    async fn deadlettered(&self) -> Vec<DeadletteredInfo>;
    async fn scheduled(&self) -> Vec<ScheduledInfo>;
}

/// This trait allows a given broker to report metadata, usually to a frontend application of some kind.
#[async_trait]
pub trait BrokerInfo: Broker {
    async fn workers(&self) -> Vec<WorkerInfo>;
    async fn queues(&self) -> Vec<QueueInfo>;
    async fn stats(&self) -> Stats;
    async fn deadlettered(&self) -> Vec<DeadletteredInfo>;
    async fn scheduled(&self) -> Vec<ScheduledInfo>;
}

#[async_trait]
impl<B: BrokerInfo> ApplicationInfo for IronworkerApplication<B> {
    async fn workers(&self) -> Vec<WorkerInfo> {
        self.shared_data.broker.workers().await
    }
    async fn queues(&self) -> Vec<QueueInfo> {
        self.shared_data.broker.queues().await
    }
    async fn stats(&self) -> Stats {
        self.shared_data.broker.stats().await
    }
    async fn deadlettered(&self) -> Vec<DeadletteredInfo> {
        self.shared_data.broker.deadlettered().await
    }
    async fn scheduled(&self) -> Vec<ScheduledInfo> {
        self.shared_data.broker.scheduled().await
    }
}
