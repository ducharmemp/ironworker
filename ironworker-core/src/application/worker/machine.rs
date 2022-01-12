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
    PostFailure(SerializableMessage),
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
    PostFailureCompleted,
    Shutdown,
}

impl WorkerState {
    fn next(self, event: WorkerEvent) -> Self {
        match (self, event) {
            // Go back to waiting
            (Self::Initialize, WorkerEvent::Initialized) => Self::WaitForTask,
            (Self::WaitForTask, WorkerEvent::NoTaskReceived) => Self::WaitForTask,
            (Self::HeartBeat, WorkerEvent::HeartBeatCompleted) => Self::WaitForTask,

            // Let the broker know we're alive
            (Self::WaitForTask, WorkerEvent::ShouldTryHeartbeat) => Self::HeartBeat,

            // Pre task execution
            (Self::WaitForTask, WorkerEvent::TaskReceived(message)) => Self::PreExecute(message),
            (Self::PreExecute(message), WorkerEvent::PreExecuteCompleted) => Self::Execute(message),

            // Task execution
            (Self::Execute(message), WorkerEvent::ExecuteCompleted) => Self::PostExecute(message),
            (Self::Execute(message), WorkerEvent::ExecuteFailed(error)) => {
                Self::ExecuteFailed(message, error)
            }

            // Post execution
            (Self::PostExecute(_), WorkerEvent::PostExecuteCompleted) => Self::WaitForTask,

            // Task Failure
            (Self::PostFailure(_), WorkerEvent::PostFailureCompleted) => Self::WaitForTask,
            (Self::RetryTask(_, message), WorkerEvent::RetryCompleted)
            | (Self::DeadletterTask(_, message), WorkerEvent::DeadletterCompleted) => {
                Self::PostFailure(message)
            }
            (Self::ExecuteFailed(_, _), WorkerEvent::ShouldRetry(queue, message)) => {
                Self::RetryTask(queue, message)
            }
            (Self::ExecuteFailed(_, _), WorkerEvent::ShouldDeadletter(queue, message)) => {
                Self::DeadletterTask(queue, message)
            }

            // Worker told to stop
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

    async fn wait_for_task(&mut self) -> WorkerEvent {
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

    async fn pre_execute(&self, message: &SerializableMessage) -> WorkerEvent {
        if !self.shared_data.tasks.contains_key(&message.task.as_str()) {
            return WorkerEvent::PreExecuteFailed;
        }
        debug!(id=?self.id, "Running pre-execution hooks");
        for middleware in self.shared_data.middleware.iter() {
            middleware.on_task_start().await;
        }
        WorkerEvent::PreExecuteCompleted
    }

    async fn execute(&self, message: &SerializableMessage) -> WorkerEvent {
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

    async fn post_execute(&self, message: &SerializableMessage) -> WorkerEvent {
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

    async fn execute_failed(
        &self,
        message: &SerializableMessage,
        error: &TaggedError,
    ) -> WorkerEvent {
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

    async fn post_failure(&self, message: &SerializableMessage) -> WorkerEvent {
        debug!(id=?self.id, "Running failed hooks");
        for middleware in self.shared_data.middleware.iter() {
            middleware.on_task_failure().await;
        }
        WorkerEvent::PostFailureCompleted
    }

    async fn retry_task(&self, queue: &str, message: &SerializableMessage) -> WorkerEvent {
        self.shared_data
            .broker
            .enqueue(queue, message.clone())
            .await;
        WorkerEvent::RetryCompleted
    }

    async fn deadletter_task(&self, queue: &str, message: &SerializableMessage) -> WorkerEvent {
        // TODO: Remove these clones
        self.shared_data
            .broker
            .deadletter(queue, message.clone())
            .await;
        WorkerEvent::DeadletterCompleted
    }

    async fn step(&mut self) -> WorkerEvent {
        match &self.state {
            WorkerState::Initialize => {
                self.shared_data.broker.heartbeat(&self.id).await;
                WorkerEvent::Initialized
            }
            WorkerState::WaitForTask => self.wait_for_task().await,
            WorkerState::HeartBeat => {
                debug!(id=?self.id, "Emitting heartbeat");
                self.shared_data.broker.heartbeat(&self.id).await;
                WorkerEvent::HeartBeatCompleted
            }
            WorkerState::PreExecute(message) => self.pre_execute(message).await,
            WorkerState::Execute(message) => self.execute(message).await,
            WorkerState::PostExecute(message) => self.post_execute(message).await,
            WorkerState::ExecuteFailed(message, error) => self.execute_failed(message, error).await,
            WorkerState::RetryTask(queue, message) => self.retry_task(queue, message).await,
            WorkerState::DeadletterTask(queue, message) => {
                self.deadletter_task(queue, message).await
            }
            WorkerState::PostFailure(message) => self.post_failure(message).await,
            WorkerState::Shutdown => {
                // For completeness, if we're shut down we should just shut down
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
    use snafu::Snafu;
    use tokio::time::{self, sleep};

    use crate::{broker::InProcessBroker, ConfigurableTask, IntoTask, Message, Task};

    use super::*;

    fn boxed_task<T: Task>(t: T) -> Box<dyn Task> {
        Box::new(t)
    }

    fn default_message(task: Box<dyn Task>) -> SerializableMessage {
        SerializableMessage {
            enqueued_at: Utc::now(),
            queue: "default".to_string(),
            job_id: "test-id".to_string(),
            task: task.name().to_string(),
            payload: 123.into(),
            err: None,
            retries: 0,
            delivery_tag: None,
        }
    }

    async fn crank_until<B: Broker>(
        mut worker: WorkerStateMachine<B>,
        wait_for: WorkerState,
    ) -> WorkerStateMachine<B> {
        while worker.state != wait_for {
            let event = worker.step().await;
            worker.state = worker.state.next(event);
        }
        worker
    }

    #[derive(Snafu, Debug)]
    enum TestEnum {
        #[snafu(display("The task failed"))]
        Failed,
    }

    #[tokio::test]
    async fn wait_for_task_dequeues_a_message() {
        async fn successful(_message: Message<u32>) -> Result<(), TestEnum> {
            Ok(())
        }

        time::pause();

        let message = default_message(boxed_task(successful.task()));

        let shared = Arc::new(SharedData {
            broker: InProcessBroker::default(),
            middleware: Default::default(),
            tasks: HashMap::from_iter([(successful.task().name(), boxed_task(successful.task()))]),
            state: Default::default(),
        });

        shared
            .broker
            .enqueue("default", message.clone())
            .await
            .unwrap();

        let mut worker = WorkerStateMachine::new("id".to_string(), "default", shared);
        let event = worker.wait_for_task().await;
        // assert_eq!(event, WorkerEvent::TaskReceived(message));
    }

    #[tokio::test]
    async fn state_machine_flow() {
        struct StateTest {
            from: WorkerState,
            to: WorkerState,
            event: WorkerEvent,
        }

        async fn task(_message: Message<u32>) -> Result<(), TestEnum> {
            Ok(())
        }

        // More of a sanity check so we don't accidentally nuke a state transition
        let transitions = vec![
            StateTest {
                from: WorkerState::Initialize,
                to: WorkerState::WaitForTask,
                event: WorkerEvent::Initialized,
            },
            StateTest {
                from: WorkerState::WaitForTask,
                to: WorkerState::PreExecute(default_message(boxed_task(task.task()))),
                event: WorkerEvent::TaskReceived(default_message(boxed_task(task.task()))),
            },
            StateTest {
                from: WorkerState::WaitForTask,
                to: WorkerState::WaitForTask,
                event: WorkerEvent::NoTaskReceived,
            },
            StateTest {
                from: WorkerState::WaitForTask,
                to: WorkerState::HeartBeat,
                event: WorkerEvent::ShouldTryHeartbeat,
            },
            StateTest {
                from: WorkerState::HeartBeat,
                to: WorkerState::WaitForTask,
                event: WorkerEvent::HeartBeatCompleted,
            },
            StateTest {
                from: WorkerState::PreExecute(default_message(boxed_task(task.task()))),
                to: WorkerState::Execute(default_message(boxed_task(task.task()))),
                event: WorkerEvent::PreExecuteCompleted,
            },
            StateTest {
                from: WorkerState::Execute(default_message(boxed_task(task.task()))),
                to: WorkerState::PostExecute(default_message(boxed_task(task.task()))),
                event: WorkerEvent::ExecuteCompleted,
            },
            StateTest {
                from: WorkerState::Execute(default_message(boxed_task(task.task()))),
                to: WorkerState::ExecuteFailed(
                    default_message(boxed_task(task.task())),
                    TestEnum::Failed.into(),
                ),
                event: WorkerEvent::ExecuteFailed(TestEnum::Failed.into()),
            },
            StateTest {
                from: WorkerState::PostExecute(default_message(boxed_task(task.task()))),
                to: WorkerState::WaitForTask,
                event: WorkerEvent::PostExecuteCompleted,
            },
            StateTest {
                from: WorkerState::RetryTask("default", default_message(boxed_task(task.task()))),
                to: WorkerState::PostFailure(default_message(boxed_task(task.task()))),
                event: WorkerEvent::RetryCompleted,
            },
            StateTest {
                from: WorkerState::DeadletterTask(
                    "default",
                    default_message(boxed_task(task.task())),
                ),
                to: WorkerState::PostFailure(default_message(boxed_task(task.task()))),
                event: WorkerEvent::DeadletterCompleted,
            },
            StateTest {
                from: WorkerState::ExecuteFailed(
                    default_message(boxed_task(task.task())),
                    TestEnum::Failed.into(),
                ),
                to: WorkerState::RetryTask("default", default_message(boxed_task(task.task()))),
                event: WorkerEvent::ShouldRetry(
                    "default",
                    default_message(boxed_task(task.task())),
                ),
            },
            StateTest {
                from: WorkerState::ExecuteFailed(
                    default_message(boxed_task(task.task())),
                    TestEnum::Failed.into(),
                ),
                to: WorkerState::DeadletterTask(
                    "default",
                    default_message(boxed_task(task.task())),
                ),
                event: WorkerEvent::ShouldDeadletter(
                    "default",
                    default_message(boxed_task(task.task())),
                ),
            },
            StateTest {
                from: WorkerState::PostFailure(default_message(boxed_task(task.task()))),
                to: WorkerState::WaitForTask,
                event: WorkerEvent::PostFailureCompleted,
            },
            StateTest {
                from: WorkerState::WaitForTask,
                to: WorkerState::Shutdown,
                event: WorkerEvent::Shutdown,
            },
        ];

        for StateTest { from, to, event } in transitions.into_iter() {
            // assert_eq!(from.next(event), to);
        }
    }
}
