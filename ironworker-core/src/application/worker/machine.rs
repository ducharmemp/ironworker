use std::sync::Arc;
use std::time::Duration;

use tokio::select;
use tokio::sync::broadcast::Receiver;
use tokio::time::{interval, timeout, Interval};
use tracing::{debug, error};

use crate::task::TaggedError;
use crate::{Broker, SerializableMessage};

use super::super::shared::SharedData;

#[derive(PartialEq, Eq)]
pub enum WorkerState {
    Initialize,
    WaitForTask,
    HeartBeat,
    PreExecute(SerializableMessage),
    Execute(SerializableMessage),
    PostExecute(SerializableMessage),
    ExecuteFailed(SerializableMessage, TaggedError),
    RetryTask(&'static str, SerializableMessage),
    DeadletterTask(&'static str, SerializableMessage),
    Shutdown,
}

#[derive(PartialEq, Eq)]
pub enum WorkerEvent {
    Initialized,
    TaskReceived(SerializableMessage),
    NoTaskReceived,
    ShouldTryHeartbeat,
    HeartBeatCompleted,
    PreExecuteCompleted,
    PreExecuteFailed,
    ExecuteCompleted,
    ExecuteFailed(TaggedError),
    ShouldRetry(&'static str, SerializableMessage),
    ShouldDeadletter(&'static str, SerializableMessage),
    PostExecuteCompleted,
    RetryCompleted,
    DeadletterCompleted,
    Shutdown,
}

impl WorkerState {
    fn next(self, event: WorkerEvent) -> Self {
        match (self, event) {
            (Self::Initialize, WorkerEvent::Initialized) => Self::WaitForTask,
            (Self::WaitForTask, WorkerEvent::TaskReceived(message)) => Self::PreExecute(message),
            (Self::WaitForTask, WorkerEvent::NoTaskReceived) => Self::WaitForTask,
            (Self::WaitForTask, WorkerEvent::ShouldTryHeartbeat) => Self::HeartBeat,
            (Self::HeartBeat, WorkerEvent::HeartBeatCompleted) => Self::WaitForTask,
            (Self::PreExecute(message), WorkerEvent::PreExecuteCompleted) => Self::Execute(message),
            (Self::Execute(message), WorkerEvent::ExecuteCompleted) => Self::PostExecute(message),
            (Self::Execute(message), WorkerEvent::ExecuteFailed(error)) => {
                Self::ExecuteFailed(message, error)
            }
            // (Self::ExecuteFailed())
            (Self::PostExecute(_), WorkerEvent::PostExecuteCompleted)
            | (Self::RetryTask(_, _), WorkerEvent::RetryCompleted)
            | (Self::DeadletterTask(_, _), WorkerEvent::DeadletterCompleted) => Self::WaitForTask,
            (Self::ExecuteFailed(_, _), WorkerEvent::ShouldRetry(queue, message)) => {
                Self::RetryTask(queue, message)
            }
            (Self::ExecuteFailed(_, _), WorkerEvent::ShouldDeadletter(queue, message)) => {
                Self::DeadletterTask(queue, message)
            }
            (_, WorkerEvent::Shutdown) => Self::Shutdown,
            _ => unimplemented!(),
        }
    }

    /// Returns `true` if the worker state is [`Shutdown`].
    ///
    /// [`Shutdown`]: WorkerState::Shutdown
    pub fn is_shutdown(&self) -> bool {
        matches!(self, Self::Shutdown)
    }
}

pub struct WorkerStateMachine<B: Broker> {
    id: String,
    queue: &'static str,
    shared_data: Arc<SharedData<B>>,
    heartbeat_interval: Interval,
    state: WorkerState,
}

impl<B: Broker> WorkerStateMachine<B> {
    pub fn new(id: String, queue: &'static str, shared_data: Arc<SharedData<B>>) -> Self {
        WorkerStateMachine {
            id,
            queue,
            shared_data,
            heartbeat_interval: interval(Duration::from_millis(5000)),
            state: WorkerState::Initialize,
        }
    }

    async fn step(&mut self) -> WorkerEvent {
        match &self.state {
            WorkerState::Initialize => {
                self.shared_data.broker.heartbeat(&self.id).await;
                WorkerEvent::Initialized
            }
            WorkerState::WaitForTask => {
                select!(
                    _ = self.heartbeat_interval.tick() => {
                        WorkerEvent::ShouldTryHeartbeat
                    },
                    message = self.shared_data.broker.dequeue(self.queue) => {
                        if let Some(message) = message {
                            debug!(id=?self.id, "Received job {}", message.job_id);
                            WorkerEvent::TaskReceived(message)
                        } else {
                            debug!(id=?self.id, "No job received, polling again");
                            WorkerEvent::NoTaskReceived
                        }
                    }
                )
            }
            WorkerState::HeartBeat => {
                debug!(id=?self.id, "Emitting heartbeat");
                self.shared_data.broker.heartbeat(&self.id).await;
                WorkerEvent::HeartBeatCompleted
            }
            WorkerState::PreExecute(message) => {
                if !self.shared_data.tasks.contains_key(&message.task.as_str()) {
                    return WorkerEvent::PreExecuteFailed;
                }
                debug!(id=?self.id, "Running pre-execution hooks");
                for middleware in self.shared_data.middleware.iter() {
                    middleware.on_task_start().await;
                }
                WorkerEvent::PreExecuteCompleted
            }
            WorkerState::Execute(message) => {
                let handler = self.shared_data.tasks.get(&message.task.as_str());
                let handler = handler.unwrap();
                let task_future = timeout(
                    Duration::from_secs(30),
                    handler.perform(message.clone(), &self.shared_data.state),
                );

                match task_future.await {
                    Ok(task_result) => {
                        if let Err(e) = task_result {
                            WorkerEvent::ExecuteFailed(e)
                        } else {
                            WorkerEvent::ExecuteCompleted
                        }
                    }
                    Err(e) => WorkerEvent::ExecuteFailed(e.into()),
                }
            }
            WorkerState::PostExecute(message) => {
                debug!(id=?self.id, "Running post-execution hooks");
                for middleware in self.shared_data.middleware.iter() {
                    middleware.on_task_completion().await;
                }
                self.shared_data
                    .broker
                    .acknowledge_processed(self.queue, message.clone())
                    .await;
                WorkerEvent::PostExecuteCompleted
            }
            WorkerState::ExecuteFailed(message, error) => {
                error!(id=?self.id, "Task {} failed", message.job_id);
                let mut message = message.clone();
                let handler = self.shared_data.tasks.get(&message.task.as_str());
                let handler = handler.unwrap();
                let handler_config = handler.config();
                let retry_on_config = handler_config
                    .retry_on
                    .get(&error.type_id)
                    .cloned()
                    .unwrap_or_default();
                let queue = retry_on_config.queue.unwrap_or(handler_config.queue);
                let retries = retry_on_config.attempts.unwrap_or(handler_config.retries);
                let should_discard = handler_config.discard_on.contains(&error.type_id);

                debug!(id=?self.id, "Running failed hooks");
                for middleware in self.shared_data.middleware.iter() {
                    middleware.on_task_failure().await;
                }

                // message.err = Some(error.into());
                if message.retries < retries && !should_discard {
                    message.retries += 1;
                    WorkerEvent::ShouldRetry(queue, message)
                } else if !should_discard {
                    WorkerEvent::ShouldDeadletter(queue, message)
                } else {
                    WorkerEvent::PostExecuteCompleted
                }
            }
            WorkerState::RetryTask(queue, message) => {
                self.shared_data
                    .broker
                    .enqueue(queue, message.clone())
                    .await;
                WorkerEvent::RetryCompleted
            }
            WorkerState::DeadletterTask(queue, message) => {
                // TODO: Remove these clones
                self.shared_data
                    .broker
                    .deadletter(queue, message.clone())
                    .await;
                WorkerEvent::DeadletterCompleted
            }
            WorkerState::Shutdown => {
                // For completeness, if we're shut down we should just shut downs
                WorkerEvent::Shutdown
            }
        }
    }

    pub async fn run(mut self, mut shutdown_channel: Receiver<()>) {
        while !self.state.is_shutdown() {
            let event = select!(
                _ = shutdown_channel.recv() => {
                    WorkerEvent::Shutdown
                },
                e = self.step() => {
                    e
                }
            );
            self.state = self.state.next(event);
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

        let mut worker = WorkerStateMachine::new("test".to_string(), "default", shared.clone());
        worker.state = WorkerState::Execute(message);
        let _event = worker.step().await;
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

        let mut worker = WorkerStateMachine::new("test".to_string(), "default", shared.clone());
        worker.state = WorkerState::Execute(message);
        worker.step().await;
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

        let mut worker = WorkerStateMachine::new(
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
        worker.state = WorkerState::Execute(message);
        let _event = worker.step().await;
        // assert_eq!(event, WorkerEvent::ExecuteCompleted);
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

        let mut worker = WorkerStateMachine::new("test".to_string(), "default", shared.clone());
        worker.state = WorkerState::Execute(message);
        worker.step().await;
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

        let mut worker = WorkerStateMachine::new("test".to_string(), "default", shared.clone());
        worker.state = WorkerState::Execute(message);
        worker.step().await;
        worker.state = WorkerState::Execute(shared.broker.dequeue("default").await.unwrap());
        worker.step().await;
        assert_eq!(shared.broker.deadletter.lock().await.keys().len(), 1);
    }
}
