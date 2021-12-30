use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use state::Container;
use tokio::select;
use tokio::sync::broadcast::Receiver;
use tokio::time::interval;
use tokio::time::timeout;

use crate::{Broker, SerializableMessage, Task};

pub struct IronWorker<B: Broker> {
    id: String,
    pub(crate) broker: Arc<B>,
    pub(crate) tasks: Arc<HashMap<&'static str, Box<dyn Task>>>,
    pub(crate) queue: &'static str,
    pub(crate) state: Arc<Container![Send + Sync]>,
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

    async fn reenquee_or_deadletter(
        &self,
        mut message: SerializableMessage,
        max_retries: usize,
        err: Box<dyn Error + Send>,
    ) {
        message.err = Some(err.into());
        if message.retries < max_retries {
            message.retries += 1;
            self.broker.enqueue(self.queue, message).await;
        } else {
            self.broker.deadletter(self.queue, message).await;
        }
    }

    async fn work_task(&self, message: SerializableMessage) {
        let task_name = message.task.clone();

        let handler = self.tasks.get(&task_name.as_str());
        if let Some(handler) = handler {
            let handler_config = handler.config();
            let max_retries = handler_config.retries;

            match timeout(
                Duration::from_secs(30),
                handler.perform(message.clone(), &self.state),
            )
            .await
            {
                Ok(task_result) => {
                    if let Err(e) = task_result {
                        self.reenquee_or_deadletter(message, max_retries, e).await;
                    }
                }
                Err(e) => {
                    self.reenquee_or_deadletter(message, max_retries, Box::new(e))
                        .await;
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
                        self.work_task(message).await;
                    }
                }
                _ = interval.tick() => {
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
