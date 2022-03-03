mod builder;
mod config;
mod shared;
mod worker;

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use serde::Serialize;
use serde_json::to_value;
use snafu::ResultExt;
use tokio::select;
use tokio::sync::broadcast::channel;
use tokio::sync::Notify;
use tracing::debug;

use ironworker_core::broker::Broker;
use ironworker_core::enqueuer::Enqueuer;
use ironworker_core::error::IronworkerError;
use ironworker_core::error::{
    CouldNotConstructSerializableMessageSnafu, CouldNotSerializePayloadSnafu,
};
use ironworker_core::info::{ApplicationInfo, BrokerInfo, QueueInfo, Stats, WorkerInfo};
use ironworker_core::message::SerializableMessageBuilder;
use ironworker_core::message::{Message, SerializableMessage};
use ironworker_core::task::Config as TaskConfig;

use self::config::IronworkerConfig;
pub use builder::IronworkerApplicationBuilder;
pub(crate) use shared::SharedData;
use worker::IronWorkerPool;

/// The main handle that coordinates workers, tasks, and configuration in order to drive a set of async queues.
#[derive(Debug)]
pub struct IronworkerApplication<B: Broker> {
    id: String,
    name: String,
    queues: HashSet<&'static str>,
    config: IronworkerConfig,
    notify_shutdown: Notify,
    shared_data: Arc<SharedData<B>>,
}

impl<B: Broker + Send + 'static> IronworkerApplication<B> {
    /// Sends a signal to all worker pools to cease their processing. The user should `.await` the `work` function until all processing is done.
    /// Failing to do so could result in jobs being lost.
    pub fn shutdown(&self) {
        self.notify_shutdown.notify_one()
    }

    /// Boots up the application and worker pools for every known queue. This function will not return when `shutdown` until all of the workers have ceased processing jobs.
    pub async fn run(&self) {
        let (shutdown_tx, _) = channel(1);

        let handles = self.queues.iter().map(|queue| {
            let default_count = self.config.concurrency;
            let worker_count = self
                .config
                .queues
                .get(&(*queue).to_string())
                .map(|queue| queue.concurrency)
                .unwrap_or(default_count);

            let pool = IronWorkerPool::new(
                self.id.clone(),
                queue,
                worker_count,
                shutdown_tx.subscribe(),
                self.shared_data.clone(),
            );

            pool.work()
        });

        let mut handles: FuturesUnordered<_> = handles.collect();

        select!(
            _ = self.notify_shutdown.notified() => {
                #[allow(clippy::expect_used)]
                shutdown_tx.send(()).expect("All workers have already been dropped");
            },
            _ = handles.next() => {},
        );
    }
}

#[async_trait]
impl<B: Broker> Enqueuer for IronworkerApplication<B> {
    /// Sends a task to a given broker, utilizing the task's configuration to determine routing. If no configuration is provided, the special "default" queue is chosen
    async fn enqueue<P: Serialize + Send + Into<Message<P>>>(
        &self,
        task: &str,
        payload: P,
        task_config: TaskConfig,
    ) -> Result<(), IronworkerError> {
        let handler_config = {
            let (_, handler_config) = self.shared_data.tasks.get(task).unwrap();
            *handler_config
        };
        let merged_config = task_config.merge(handler_config);
        let unwrapped_config = merged_config.unwrap();
        let value = to_value(payload).context(CouldNotSerializePayloadSnafu {})?;

        let serializable = SerializableMessageBuilder::default()
            .task(task.to_string())
            .queue(unwrapped_config.queue.to_string())
            .payload(value)
            .at(unwrapped_config.at)
            .retries(0)
            .build()
            .context(CouldNotConstructSerializableMessageSnafu {})?;

        debug!(id=?self.id, "Enqueueing job {}", serializable.job_id);
        for middleware in self.shared_data.middleware.iter() {
            middleware.before_enqueue(&serializable).await;
        }

        self.shared_data
            .broker
            .enqueue(unwrapped_config.queue, serializable, unwrapped_config.at)
            .await
            .map_err(|_| IronworkerError::CouldNotEnqueue)?;

        // FIXME: This needs the serializable message even though we rightfully hand off ownership
        // for middleware in self.shared_data.middleware.iter() {
        //     middleware.after_enqueue(&serializable).await;
        // }

        Ok(())
    }
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
    async fn deadlettered(&self) -> Vec<SerializableMessage> {
        self.shared_data.broker.deadlettered().await
    }
    async fn scheduled(&self) -> Vec<SerializableMessage> {
        self.shared_data.broker.scheduled().await
    }
}

impl<B: Broker> Clone for IronworkerApplication<B> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            name: self.name.clone(),
            queues: self.queues.clone(),
            config: self.config.clone(),
            notify_shutdown: Notify::new(),
            shared_data: self.shared_data.clone(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, iter::FromIterator};

    use ironworker_core::task::{IntoTask, Task};

    use crate::test::{
        assert_send, assert_sync, boxed_task, successful, MockTestBroker, MockTestMiddleware,
    };

    use super::*;

    #[tokio::test]
    async fn enqueuing_message_goes_to_broker() {
        let mut broker = MockTestBroker::new();
        broker.expect_enqueue().times(1).return_const(Ok(()));
        let task = successful.task();

        let app = IronworkerApplication {
            id: "test".to_string(),
            name: "test-name".to_string(),
            queues: HashSet::from_iter(["default"]),
            config: IronworkerConfig::default(),
            notify_shutdown: Notify::new(),
            shared_data: Arc::new(SharedData {
                broker,
                tasks: HashMap::from_iter([(
                    task.name(),
                    (boxed_task(task), TaskConfig::default()),
                )]),
                middleware: vec![],
            }),
        };

        successful.task().perform_later(&app, 123).await.unwrap();
    }

    #[tokio::test]
    async fn enqueueing_message_calls_before_enqueue() {
        let mut broker = MockTestBroker::new();
        broker.expect_enqueue().times(1).return_const(Ok(()));
        let task = successful.task();

        let mut middleware = MockTestMiddleware::new();
        middleware.expect_before_enqueue().times(1).return_const(());

        let app = IronworkerApplication {
            id: "test".to_string(),
            name: "test-name".to_string(),
            queues: HashSet::from_iter(["default"]),
            config: IronworkerConfig::default(),
            notify_shutdown: Notify::new(),
            shared_data: Arc::new(SharedData {
                broker,
                tasks: HashMap::from_iter([(
                    task.name(),
                    (boxed_task(task), TaskConfig::default()),
                )]),
                middleware: vec![Box::new(middleware)],
            }),
        };

        successful.task().perform_later(&app, 123).await.unwrap();
    }

    #[test]
    fn assertions() {
        assert_send::<IronworkerApplication<MockTestBroker>>();
        assert_sync::<IronworkerApplication<MockTestBroker>>();
    }
}
