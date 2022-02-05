mod builder;
mod shared;
mod worker;

use std::collections::HashSet;
use std::sync::Arc;

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use serde::Serialize;

use tokio::select;
use tokio::sync::broadcast::channel;
use tokio::sync::Notify;
use tracing::debug;

use crate::config::IronworkerConfig;
use crate::{Broker, IronworkerError, Message, SerializableMessage};
pub use builder::IronworkerApplicationBuilder;
use shared::SharedData;
use worker::IronWorkerPool;

#[allow(missing_debug_implementations)]
/// The main handle that coordinates workers, tasks, and configuration in order to drive a set of async queues.
pub struct IronworkerApplication<B: Broker> {
    pub(crate) id: String,
    pub(crate) queues: HashSet<&'static str>,
    pub(crate) config: IronworkerConfig,
    pub(crate) notify_shutdown: Notify,
    pub(crate) shared_data: Arc<SharedData<B>>,
}

impl<B: Broker + Send + 'static> IronworkerApplication<B> {
    /// Sends a signal to all worker pools to cease their processing. The user should `.await` the `work` function until all processing is done.
    /// Failing to do so could result in jobs being lost.
    pub fn shutdown(&self) {
        self.notify_shutdown.notify_one()
    }

    /// Sends a task to a given broker, utilizing the task's configuration to determine routing. If no configuration is provided, the special "default" queue is chosen
    pub async fn enqueue<P: Serialize + Send + Into<Message<P>>>(
        &self,
        task: &str,
        payload: P,
    ) -> Result<(), IronworkerError> {
        let message: Message<P> = payload.into();
        let handler_config = {
            let (_, handler_config) = self.shared_data.tasks.get(task).unwrap();
            *handler_config
        };

        let serializable = SerializableMessage::from_message(task, handler_config.queue, message);
        debug!(id=?self.id, "Enqueueing job {}", serializable.job_id);

        self.shared_data
            .broker
            .enqueue(handler_config.queue, serializable)
            .await
            .map_err(|_| IronworkerError::CouldNotEnqueue)?;
        Ok(())
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        test::{assert_send, assert_sync},
        InProcessBroker,
    };

    #[tokio::test]
    async fn enqueuing_message_goes_to_broker() {}

    #[test]
    fn assertions() {
        assert_send::<IronworkerApplication<InProcessBroker>>();
        assert_sync::<IronworkerApplication<InProcessBroker>>();
    }
}
