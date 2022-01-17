use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::{Broker, IronworkerApplication};

#[derive(Debug)]
pub struct WorkerInfo {
    pub name: String,
    pub queue: Option<String>,
    pub last_seen_at: Option<DateTime<Utc>>,
}

#[derive(Debug)]
pub struct QueueInfo {
    pub name: String,
    pub size: usize,
}

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
