use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{atomic::AtomicUsize, Arc};
use std::time::Duration;

use futures::future::select_all;
use state::Container;
use tokio::select;
use tokio::sync::broadcast::{channel, Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::{interval, timeout};
use tracing::{debug, info};

use crate::task::TaggedError;
use crate::{Broker, SerializableMessage, Task};

pub struct IronWorkerPool<B: Broker> {
    id: String,
    broker: Arc<B>,
    tasks: Arc<HashMap<&'static str, Box<dyn Task>>>,
    queue: &'static str,
    state: Arc<Container![Send + Sync]>,
    worker_count: usize,
    shutdown_channel: Receiver<()>,
    current_worker_index: AtomicUsize,
    worker_shutdown_channel: Sender<()>,
}

impl<B: Broker + Sync + Send + 'static> IronWorkerPool<B> {
    pub fn new(
        id: String,
        broker: Arc<B>,
        tasks: Arc<HashMap<&'static str, Box<dyn Task>>>,
        queue: &'static str,
        state: Arc<Container![Send + Sync]>,
        worker_count: usize,
        shutdown_channel: Receiver<()>,
    ) -> Self {
        Self {
            id,
            broker,
            tasks,
            queue,
            state,
            worker_count,
            shutdown_channel,
            current_worker_index: AtomicUsize::new(0),
            worker_shutdown_channel: channel(1).0,
        }
    }

    fn spawn_worker(&self) -> JoinHandle<()> {
        let broker = self.broker.clone();
        let index = self.current_worker_index.fetch_add(1, Ordering::SeqCst);
        let id = format!("{}-{}-{}", self.id.clone(), self.queue, index);
        let tasks = self.tasks.clone();
        let state = self.state.clone();
        let queue = self.queue;
        let rx = self.worker_shutdown_channel.subscribe();
        tokio::task::spawn(async move {
            info!(id=?id, "Booting worker {}", id);
            IronWorker::new(id, broker, tasks, queue, state)
                .work(rx)
                .await
        })
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
    broker: Arc<B>,
    tasks: Arc<HashMap<&'static str, Box<dyn Task>>>,
    queue: &'static str,
    state: Arc<Container![Send + Sync]>,
}

impl<B: Broker + Sync + Send + 'static> IronWorker<B> {
    pub fn new(
        id: String,
        broker: Arc<B>,
        tasks: Arc<HashMap<&'static str, Box<dyn Task>>>,
        queue: &'static str,
        state: Arc<Container![Send + Sync]>,
    ) -> Self {
        Self {
            id,
            broker,
            tasks,
            queue,
            state,
        }
    }

    async fn work_task(&self, message: SerializableMessage) {
        let task_name = message.task.clone();

        let handler = self.tasks.get(&task_name.as_str());
        if let Some(handler) = handler {
            let handler_config = handler.config();
            let task_future = timeout(
                Duration::from_secs(30),
                handler.perform(message.clone(), &self.state),
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

                message.err = Some(err.wrapped.into());
                if message.retries < retries && !should_discard {
                    message.retries += 1;
                    self.broker.enqueue(queue, message).await;
                } else if !should_discard {
                    self.broker.deadletter(queue, message).await;
                }
            };

            match task_future.await {
                Ok(task_result) => {
                    if let Err(e) = task_result {
                        process_failed(message, e).await;
                    }
                }
                Err(e) => {
                    process_failed(message, e.into()).await;
                }
            }
            self.broker.mark_done(&self.id).await;
        }
    }

    pub async fn work(self, mut shutdown_channel: Receiver<()>) {
        let mut interval = interval(Duration::from_millis(5000));
        self.broker.heartbeat(&self.id).await;

        loop {
            select!(
                _ = shutdown_channel.recv() => {
                    self.broker.deregister_worker(&self.id).await;
                    return;
                },
                message = self.broker.dequeue(&self.id, self.queue) => {
                    if let Some(message) = message {
                        info!(id=?self.id, "Working on job {}", message.job_id);
                        self.work_task(message).await;
                    }
                }
                _ = interval.tick() => {
                    debug!(id=?self.id, "Emitting heartbeat");
                    self.broker.heartbeat(&self.id).await;
                },
            );
        }
    }
}

#[cfg(test)]
mod test {
    use std::error::Error;

    use chrono::Utc;
    use thiserror::Error;
    use tokio::time::{self, sleep};

    use crate::{broker::InProcessBroker, ConfigurableTask, IntoTask, Message};

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
            job_id: "test-id".to_string(),
            task: long_running.task().name().to_string(),
            payload: 123.into(),
            err: None,
            retries: 0,
        };

        let broker = Arc::new(InProcessBroker::default());

        let worker = IronWorker::new(
            "test".to_string(),
            broker.clone(),
            Arc::new(HashMap::from_iter([(
                long_running.task().name(),
                boxed_task(long_running.task()),
            )])),
            "default",
            Default::default(),
        );
        worker.work_task(message).await;
        time::advance(Duration::from_secs(45)).await;

        assert_eq!(broker.deadletter.lock().await.keys().len(), 1);
    }

    #[tokio::test]
    async fn erroring_task_deadletters() {
        #[derive(Error, Debug)]
        enum TestEnum {
            #[error("The task failed")]
            Failed,
        }

        async fn erroring(_message: Message<u32>) -> Result<(), Box<dyn Error + Send>> {
            Err(Box::new(TestEnum::Failed))
        }

        let message = SerializableMessage {
            enqueued_at: Utc::now(),
            job_id: "test-id".to_string(),
            task: erroring.task().name().to_string(),
            payload: 123.into(),
            err: None,
            retries: 0,
        };

        let broker = Arc::new(InProcessBroker::default());

        let worker = IronWorker::new(
            "test".to_string(),
            broker.clone(),
            Arc::new(HashMap::from_iter([(
                erroring.task().name(),
                boxed_task(erroring.task()),
            )])),
            "default",
            Default::default(),
        );
        worker.work_task(message).await;
        assert_eq!(broker.deadletter.lock().await.keys().len(), 1);
    }

    #[tokio::test]
    async fn successful_task_marked_done() {
        async fn successful(_message: Message<u32>) -> Result<(), Box<dyn Error + Send>> {
            Ok(())
        }

        let message = SerializableMessage {
            enqueued_at: Utc::now(),
            job_id: "test-id".to_string(),
            task: successful.task().name().to_string(),
            payload: 123.into(),
            err: None,
            retries: 0,
        };

        let broker = Arc::new(InProcessBroker::default());

        let worker = IronWorker::new(
            "test".to_string(),
            broker.clone(),
            Arc::new(HashMap::from_iter([(
                successful.task().name(),
                boxed_task(successful.task()),
            )])),
            "default",
            Default::default(),
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

        async fn erroring(_message: Message<u32>) -> Result<(), Box<dyn Error + Send>> {
            Err(Box::new(TestEnum::Failed))
        }

        let message = SerializableMessage {
            enqueued_at: Utc::now(),
            job_id: "test-id".to_string(),
            task: erroring.task().name().to_string(),
            payload: 123.into(),
            err: None,
            retries: 0,
        };

        let broker = Arc::new(InProcessBroker::default());

        let worker = IronWorker::new(
            "test".to_string(),
            broker.clone(),
            Arc::new(HashMap::from_iter([(
                erroring.task().name(),
                boxed_task(erroring.task().retries(1)),
            )])),
            "default",
            Default::default(),
        );
        worker.work_task(message).await;
        assert_eq!(broker.queues.lock().await["default"].len(), 1);
    }

    #[tokio::test]
    async fn retryable_task_on_max_retries_gets_deadlettered() {
        #[derive(Error, Debug)]
        enum TestEnum {
            #[error("The task failed")]
            Failed,
        }

        async fn erroring(_message: Message<u32>) -> Result<(), Box<dyn Error + Send>> {
            Err(Box::new(TestEnum::Failed))
        }

        let message = SerializableMessage {
            enqueued_at: Utc::now(),
            job_id: "test-id".to_string(),
            task: erroring.task().name().to_string(),
            payload: 123.into(),
            err: None,
            retries: 0,
        };

        let broker = Arc::new(InProcessBroker::default());

        let worker = IronWorker::new(
            "test".to_string(),
            broker.clone(),
            Arc::new(HashMap::from_iter([(
                erroring.task().name(),
                boxed_task(erroring.task().retries(1)),
            )])),
            "default",
            Default::default(),
        );
        worker.work_task(message).await;
        worker
            .work_task(broker.dequeue("test", "default").await.unwrap())
            .await;
        assert_eq!(broker.deadletter.lock().await.keys().len(), 1);
    }
}
