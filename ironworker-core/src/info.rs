//! Informational structs for Ironworker applications.
//!
//! These structs provide insight into worker/queue/application internals, such as number of items processed, sizes of queues, and
//! jobs that have failed.
//!
//! These are intended to be used by brokers and web interfaces to show the current state of the queue, if possible. Some data stores like
//! RabbitMq have built-in viewers, and others provide no introspection at all like SQS. These are meant to bridge the gap in Ops when utilizing
//! data stores like Redis or Postgres.
use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::{broker::Broker, message::SerializableMessage};

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
    pub size: u64,
}

/// Metadata struct describing statistics of Ironworker
#[derive(Clone, Copy, Debug)]
pub struct Stats {
    /// The number of jobs that have been processed
    pub processed: u64,
    /// The number of jobs that have failed
    pub failed: u64,
    /// The number of jobs that have been scheduled
    pub scheduled: u64,
    /// The number of jobs that have been enqueued
    pub enqueued: u64,
}
#[async_trait]
pub trait ApplicationInfo: Send + Sync {
    async fn workers(&self) -> Vec<WorkerInfo>;
    async fn queues(&self) -> Vec<QueueInfo>;
    async fn stats(&self) -> Stats;
    async fn deadlettered(&self) -> Vec<SerializableMessage>;
    async fn scheduled(&self) -> Vec<SerializableMessage>;
}

/// This trait allows a given broker to report metadata, usually to a frontend application of some kind.
#[async_trait]
pub trait BrokerInfo: Broker {
    async fn workers(&self) -> Vec<WorkerInfo>;
    async fn queues(&self) -> Vec<QueueInfo>;
    async fn stats(&self) -> Stats;
    async fn deadlettered(&self) -> Vec<SerializableMessage>;
    async fn scheduled(&self) -> Vec<SerializableMessage>;
}
