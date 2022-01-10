use std::sync::atomic::Ordering;
use std::sync::{atomic::AtomicUsize, Arc};
use std::time::Duration;

use futures::future::select_all;

use tokio::select;
use tokio::sync::broadcast::{channel, Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::{interval, timeout};
use tracing::{debug, error, info};

use crate::message::SerializableError;
use crate::task::TaggedError;
use crate::{Broker, SerializableMessage};

use super::shared::SharedData;

pub struct IronWorkerPool<B: Broker> {
    id: String,
    queue: &'static str,
    worker_count: usize,
    shutdown_channel: Receiver<()>,
    current_worker_index: AtomicUsize,
    worker_shutdown_channel: Sender<()>,
    shared_data: Arc<SharedData<B>>,
}

impl<B: Broker + Sync + Send + 'static> IronWorkerPool<B> {
    pub fn new(
        id: String,
        queue: &'static str,
        worker_count: usize,
        shutdown_channel: Receiver<()>,
        shared_data: Arc<SharedData<B>>,
    ) -> Self {
        Self {
            id,
            queue,
            worker_count,
            shutdown_channel,
            current_worker_index: AtomicUsize::new(0),
            worker_shutdown_channel: channel(1).0,
            shared_data,
        }
    }

    fn spawn_worker(&self) -> JoinHandle<()> {
        let index = self.current_worker_index.fetch_add(1, Ordering::SeqCst);
        let id = format!("{}-{}-{}", self.id.clone(), self.queue, index);
        let queue = self.queue;
        let rx = self.worker_shutdown_channel.subscribe();
        info!(id=?id, "Booting worker {}", &id);
        let worker = IronWorker::new(id, queue, self.shared_data.clone());
        tokio::task::spawn(async move { worker.work(rx).await })
    }

    pub async fn work(mut self) {
        info!(id=?self.id, queue=?self.queue, "Booting worker pool with {} workers", self.worker_count);
        let mut worker_handles: Vec<_> = (0..self.worker_count)
            .map(|_| self.spawn_worker())
            .collect();

        loop {
            select!(
                _ = self.shutdown_channel.recv() => {
                    self.worker_shutdown_channel.send(()).expect("All workers were dropped");
                    return;
                },
                (_res, _, rest) = select_all(worker_handles) => {
                    worker_handles = rest;
                    worker_handles.push(self.spawn_worker());
                }
            );
        }
    }
}

pub struct IronWorker<B: Broker> {
    id: String,
    queue: &'static str,
    shared_data: Arc<SharedData<B>>,
}

impl<B: Broker + Sync + Send + 'static> IronWorker<B> {
    pub fn new(id: String, queue: &'static str, shared_data: Arc<SharedData<B>>) -> Self {
        Self {
            id,
            queue,
            shared_data,
        }
    }

    async fn work_task(&self, message: SerializableMessage) {
        let task_name = message.task.clone();

        let handler = self.shared_data.tasks.get(&task_name.as_str());
        if let Some(handler) = handler {
            for middleware in self.shared_data.middleware.iter() {
                middleware.on_task_start().await;
            }
            let handler_config = handler.config();
            let task_future = timeout(
                Duration::from_secs(30),
                handler.perform(message.clone(), &self.shared_data.state),
            );

            let process_failed = |mut message: SerializableMessage, err: TaggedError| async move {
                let retry_on_config = handler_config
                    .retry_on
                    .get(&err.type_id)
                    .cloned()
                    .unwrap_or_default();
                let queue = retry_on_config.queue.unwrap_or(handler_config.queue);
                let retries = retry_on_config.attempts.unwrap_or(handler_config.retries);
                let should_discard = handler_config.discard_on.contains(&err.type_id);

                for middleware in self.shared_data.middleware.iter() {
                    middleware.on_task_failure().await;
                }

                message.err = Some(SerializableError::from_tagged(err));
                if message.retries < retries && !should_discard {
                    message.retries += 1;
                    self.shared_data.broker.enqueue(queue, message).await;
                } else if !should_discard {
                    self.shared_data.broker.deadletter(queue, message).await;
                }
            };

            match task_future.await {
                Ok(task_result) => {
                    if let Err(e) = task_result {
                        error!(id=?self.id, "Task {} failed", message.job_id);
                        process_failed(message, e).await;
                    } else {
                        self.shared_data
                            .broker
                            .acknowledge_processed(self.queue, message)
                            .await;
                    }
                }
                Err(e) => {
                    process_failed(message, e.into()).await;
                }
            }
            for middleware in self.shared_data.middleware.iter() {
                middleware.on_task_completion().await;
            }
        }
    }

    pub async fn work(self, mut shutdown_channel: Receiver<()>) {
        let mut interval = interval(Duration::from_millis(5000));
        self.shared_data.broker.heartbeat(&self.id).await;

        loop {
            select!(
                _ = shutdown_channel.recv() => {
                    info!(id=?self.id, "Shutdown received, exiting...");
                    self.shared_data.broker.deregister_worker(&self.id).await;
                    return;
                },
                message = self.shared_data.broker.dequeue(self.queue) => {
                    if let Some(message) = message {
                        info!(id=?self.id, "Working on job {}", message.job_id);
                        self.work_task(message).await;
                    }
                }
                _ = interval.tick() => {
                    debug!(id=?self.id, "Emitting heartbeat");
                    self.shared_data.broker.heartbeat(&self.id).await;
                },
            );
        }
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, error::Error};

    use chrono::Utc;
    use thiserror::Error;
    use tokio::time::{self, sleep};

    use crate::{broker::InProcessBroker, IntoTask, Message, Task};

    use super::*;

    fn boxed_task<T: Task>(t: T) -> Box<dyn Task> {
        Box::new(t)
    }

    #[tokio::test]
    async fn long_running_task_times_out() {
        async fn long_running(_message: Message<u32>) -> Result<(), Box<dyn Error + Send>> {
            sleep(Duration::from_secs(60)).await;
            Ok(())
        }

        time::pause();

        let message = SerializableMessage {
            enqueued_at: Utc::now(),
            queue: "default".to_string(),
            job_id: "test-id".to_string(),
            task: long_running.task().name().to_string(),
            payload: 123.into(),
            err: None,
            retries: 0,
            delivery_tag: None,
        };

        let shared = Arc::new(SharedData {
            broker: InProcessBroker::default(),
            middleware: Default::default(),
            tasks: HashMap::from_iter([(
                long_running.task().name(),
                boxed_task(long_running.task()),
            )]),
            state: Default::default(),
        });

        let worker = IronWorker::new("test".to_string(), "default", shared.clone());
        worker.work_task(message).await;
        time::advance(Duration::from_secs(45)).await;

        assert_eq!(shared.broker.deadletter.lock().await.keys().len(), 1);
    }

    #[tokio::test]
    async fn erroring_task_deadletters() {
        #[derive(Error, Debug)]
        enum TestEnum {
            #[error("The task failed")]
            Failed,
        }

        async fn erroring(_message: Message<u32>) -> Result<(), TestEnum> {
            Err(TestEnum::Failed)
        }

        let message = SerializableMessage {
            enqueued_at: Utc::now(),
            queue: "default".to_string(),
            job_id: "test-id".to_string(),
            task: erroring.task().name().to_string(),
            payload: 123.into(),
            err: None,
            retries: 0,
            delivery_tag: None,
        };

        let shared = Arc::new(SharedData {
            broker: InProcessBroker::default(),
            middleware: Default::default(),
            tasks: HashMap::from_iter([(erroring.task().name(), boxed_task(erroring.task()))]),
            state: Default::default(),
        });

        let worker = IronWorker::new("test".to_string(), "default", shared.clone());
        worker.work_task(message).await;
        assert_eq!(shared.broker.deadletter.lock().await.keys().len(), 1);
    }

    #[tokio::test]
    async fn successful_task_marked_done() {
        async fn successful(_message: Message<u32>) -> Result<(), Box<dyn Error + Send>> {
            Ok(())
        }

        let message = SerializableMessage {
            enqueued_at: Utc::now(),
            queue: "default".to_string(),
            job_id: "test-id".to_string(),
            task: successful.task().name().to_string(),
            payload: 123.into(),
            err: None,
            retries: 0,
            delivery_tag: None,
        };

        let broker = InProcessBroker::default();

        let worker = IronWorker::new(
            "test".to_string(),
            "default",
            Arc::new(SharedData {
                broker,
                middleware: Default::default(),
                tasks: HashMap::from_iter([(
                    successful.task().name(),
                    boxed_task(successful.task()),
                )]),
                state: Default::default(),
            }),
        );
        worker.work_task(message).await;
    }

    #[tokio::test]
    async fn retryable_task_gets_reenqueued() {
        #[derive(Error, Debug)]
        enum TestEnum {
            #[error("The task failed")]
            Failed,
        }

        async fn erroring(_message: Message<u32>) -> Result<(), TestEnum> {
            Err(TestEnum::Failed)
        }

        let message = SerializableMessage {
            enqueued_at: Utc::now(),
            queue: "default".to_string(),
            job_id: "test-id".to_string(),
            task: erroring.task().name().to_string(),
            payload: 123.into(),
            err: None,
            retries: 0,
            delivery_tag: None,
        };

        let shared = Arc::new(SharedData {
            broker: InProcessBroker::default(),
            middleware: Default::default(),
            tasks: HashMap::from_iter([(erroring.task().name(), boxed_task(erroring.task()))]),
            state: Default::default(),
        });

        let worker = IronWorker::new("test".to_string(), "default", shared.clone());
        worker.work_task(message).await;
        assert_eq!(shared.broker.queues.lock().await["default"].len(), 1);
    }

    #[tokio::test]
    async fn retryable_task_on_max_retries_gets_deadlettered() {
        #[derive(Error, Debug)]
        enum TestEnum {
            #[error("The task failed")]
            Failed,
        }

        async fn erroring(_message: Message<u32>) -> Result<(), TestEnum> {
            Err(TestEnum::Failed)
        }

        let message = SerializableMessage {
            enqueued_at: Utc::now(),
            queue: "default".to_string(),
            job_id: "test-id".to_string(),
            task: erroring.task().name().to_string(),
            payload: 123.into(),
            err: None,
            retries: 0,
            delivery_tag: None,
        };

        let shared = Arc::new(SharedData {
            broker: InProcessBroker::default(),
            middleware: Default::default(),
            tasks: HashMap::from_iter([(erroring.task().name(), boxed_task(erroring.task()))]),
            state: Default::default(),
        });

        let worker = IronWorker::new("test".to_string(), "default", shared.clone());
        worker.work_task(message).await;
        worker
            .work_task(shared.broker.dequeue("default").await.unwrap())
            .await;
        assert_eq!(shared.broker.deadletter.lock().await.keys().len(), 1);
    }
}
