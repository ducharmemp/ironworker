mod builder;
mod shared;
mod worker;

use std::collections::HashSet;
use std::sync::Arc;

use futures::future::join_all;
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

pub struct IronworkerApplication<B: Broker> {
    pub(crate) id: String,
    pub(crate) queues: HashSet<&'static str>,
    pub(crate) config: IronworkerConfig,
    pub(crate) notify_shutdown: Notify,
    pub(crate) shared_data: Arc<SharedData<B>>,
}

impl<B: Broker + Sync + Send + 'static> IronworkerApplication<B> {
    pub fn manage<T>(&self, state: T)
    where
        T: Send + Sync + 'static,
    {
        // let type_name = std::any::type_name::<T>();
        if !self.shared_data.state.set(state) {
            // error!("state for type '{}' is already being managed", type_name);
            panic!("aborting due to duplicately managed state");
        }
    }

    pub fn shutdown(&self) {
        self.notify_shutdown.notify_one()
    }

    pub async fn enqueue<P: Serialize + Send + Into<Message<P>>>(
        &self,
        task: &str,
        payload: P,
    ) -> Result<(), IronworkerError> {
        let message: Message<P> = payload.into();
        let handler = self.shared_data.tasks.get(task).unwrap();
        let handler_config = handler.config();

        let serializable = SerializableMessage::from_message(task, handler_config.queue, message);
        debug!(id=?self.id, "Enqueueing job {}", serializable.job_id);

        self.shared_data
            .broker
            .enqueue(handler_config.queue, serializable)
            .await
            .map_err(|_| IronworkerError::CouldNotEnqueue)?;
        Ok(())
    }

    pub async fn run(&self) {
        let (shutdown_tx, _) = channel(1);

        let mut handles: Vec<_> = self
            .queues
            .iter()
            .map(|queue| {
                let default_count = self.config.concurrency;
                let worker_count = self
                    .config
                    .queues
                    .get(&queue.to_string())
                    .map(|queue| queue.concurrency)
                    .unwrap_or(default_count);

                let pool = IronWorkerPool::new(
                    self.id.clone(),
                    <&str>::clone(queue),
                    worker_count,
                    shutdown_tx.subscribe(),
                    self.shared_data.clone(),
                );

                tokio::task::spawn(async move { pool.work().await })
            })
            .collect();

        select!(
            _ = self.notify_shutdown.notified() => {
                shutdown_tx.send(()).expect("All workers have already been dropped");
                join_all(&mut handles).await;
            },
            _ = join_all(&mut handles) => {},
        );
    }
}

#[cfg(test)]
mod test {
    #[tokio::test]
    async fn enqueuing_message_goes_to_broker() {}
}
