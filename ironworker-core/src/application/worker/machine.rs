use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use snafu::futures::TryFutureExt as SnafuTryFutureExt;
use tokio::select;
use tokio::sync::broadcast::Receiver;
use tokio::time::timeout;
use tracing::{debug, error, info};

use crate::error::{PerformLaterSnafu, TimeoutSnafu};
use crate::message::SerializableError;
use crate::{Broker, SerializableMessage};

use super::super::shared::SharedData;

#[derive(PartialEq, Eq, Debug)]
pub(crate) enum WorkerState {
    Initialize,
    WaitForTask,
    HeartBeat,
    PreExecute(SerializableMessage),
    Execute(SerializableMessage),
    PostExecute(SerializableMessage),
    ExecuteFailed(SerializableMessage),
    RetryTask(&'static str, SerializableMessage),
    DeadletterTask(&'static str, SerializableMessage),
    PreShutdown(Option<SerializableMessage>),
    Shutdown,
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub(crate) enum WorkerEvent {
    Initialized,
    TaskReceived(Box<SerializableMessage>),
    ShouldTryHeartbeat,
    HeartBeatCompleted,
    PreExecuteCompleted,
    PreExecuteFailed,
    ExecuteCompleted,
    ExecuteFailed,
    ShouldRetry(&'static str),
    ShouldDeadletter(&'static str),
    DiscardMessage,
    PostExecuteCompleted,
    RetryCompleted,
    DeadletterCompleted,
    ShouldShutdown,
    Shutdown,
}

impl WorkerState {
    fn next(self, event: WorkerEvent) -> Self {
        match (self, event) {
            // Go back to waiting
            (Self::Initialize, WorkerEvent::Initialized) => Self::WaitForTask,

            // Let the broker know we're alive
            (Self::WaitForTask, WorkerEvent::ShouldTryHeartbeat) => Self::HeartBeat,
            (Self::HeartBeat, WorkerEvent::HeartBeatCompleted) => Self::WaitForTask,

            // Pre task execution
            (Self::WaitForTask, WorkerEvent::TaskReceived(message)) => Self::PreExecute(*message),
            (Self::PreExecute(message), WorkerEvent::PreExecuteCompleted) => Self::Execute(message),
            (Self::PreExecute(message), WorkerEvent::PreExecuteFailed) => {
                Self::ExecuteFailed(message)
            }

            // Task execution
            (Self::Execute(message), WorkerEvent::ExecuteCompleted) => Self::PostExecute(message),
            (Self::Execute(message), WorkerEvent::ExecuteFailed) => Self::ExecuteFailed(message),

            // Task Failure
            (Self::ExecuteFailed(message), WorkerEvent::ShouldRetry(queue)) => {
                Self::RetryTask(queue, message)
            }
            (Self::ExecuteFailed(message), WorkerEvent::ShouldDeadletter(queue)) => {
                Self::DeadletterTask(queue, message)
            }
            (Self::ExecuteFailed(message), WorkerEvent::DiscardMessage)
            | (Self::RetryTask(_, message), WorkerEvent::RetryCompleted)
            | (Self::DeadletterTask(_, message), WorkerEvent::DeadletterCompleted) => {
                Self::PostExecute(message)
            }

            // Post execution
            (Self::PostExecute(_), WorkerEvent::PostExecuteCompleted) => Self::WaitForTask,

            // Worker told to stop
            (
                Self::PreExecute(message)
                | Self::Execute(message)
                | Self::PostExecute(message)
                | Self::ExecuteFailed(message),
                WorkerEvent::ShouldShutdown,
            ) => Self::PreShutdown(Some(message)),
            (_, WorkerEvent::ShouldShutdown) => Self::PreShutdown(None),
            (_, WorkerEvent::Shutdown) => Self::Shutdown,
            state => unreachable!("{:?}", state),
        }
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        matches!(self, Self::Shutdown)
    }
}

pub(crate) struct WorkerStateMachine<B: Broker> {
    id: String,
    queue: &'static str,
    shared_data: Arc<SharedData<B>>,
}

impl<B: Broker> WorkerStateMachine<B> {
    pub(crate) fn new(id: String, queue: &'static str, shared_data: Arc<SharedData<B>>) -> Self {
        WorkerStateMachine {
            id,
            queue,
            shared_data,
        }
    }

    async fn wait_for_task(&mut self) -> WorkerEvent {
        #[allow(clippy::expect_used)]
        let message = self
            .shared_data
            .broker
            .dequeue(self.queue)
            .await
            .expect("Could not dequeue message");

        if let Some(message) = message {
            debug!(id=?self.id, "Received job {}", message.job_id);
            WorkerEvent::TaskReceived(Box::new(message))
        } else {
            debug!(id=?self.id, "No job received, sending heartbeat");
            WorkerEvent::ShouldTryHeartbeat
        }
    }

    async fn pre_execute(&mut self, message: &mut SerializableMessage) -> WorkerEvent {
        if !self.shared_data.tasks.contains_key(&message.task.as_str()) {
            return WorkerEvent::PreExecuteFailed;
        }
        debug!(id=?self.id, "Running pre-execution hooks");
        for middleware in self.shared_data.middleware.iter() {
            middleware.before_perform(message).await;
        }
        WorkerEvent::PreExecuteCompleted
    }

    async fn execute(&mut self, message: &mut SerializableMessage) -> WorkerEvent {
        let handler_entry = self.shared_data.tasks.get(&message.task.as_str());
        message.message_state.insert(self.shared_data.clone());

        // This is safe because we already check the tasks in pre-execute for the task key
        #[allow(clippy::unwrap_used)]
        let (handler, config) = handler_entry.unwrap();
        let mut handler = handler.clone_box();
        let config = config.unwrap();
        let max_run_time = config.timeout;
        let task_future = timeout(
            Duration::from_secs(max_run_time),
            handler
                .perform(message.clone())
                .context(PerformLaterSnafu {}),
        );

        let result = task_future.context(TimeoutSnafu {}).await;
        match result {
            Ok(task_result) => {
                if let Err(err) = task_result {
                    message.err.replace(SerializableError::new(err));
                    WorkerEvent::ExecuteFailed
                } else {
                    WorkerEvent::ExecuteCompleted
                }
            }
            Err(e) => {
                message.err.replace(SerializableError::new(e));
                WorkerEvent::ExecuteFailed
            }
        }
    }

    async fn post_execute(&mut self, message: &mut SerializableMessage) -> WorkerEvent {
        debug!(id=?self.id, "Running post-execution hooks");
        for middleware in self.shared_data.middleware.iter() {
            middleware.after_perform(message).await;
        }
        if let Err(broker_error) = self
            .shared_data
            .broker
            .acknowledge_processed(self.queue, message.clone())
            .await
        {
            error!(id=?self.id, "Acknowledging job failed, {:?}", broker_error);
        }
        WorkerEvent::PostExecuteCompleted
    }

    async fn execute_failed(&mut self, message: &mut SerializableMessage) -> WorkerEvent {
        error!(id=?self.id, "Task {} failed", message.job_id);
        let handler_entry = self.shared_data.tasks.get(&message.task.as_str());
        // This is safe because we already know that we have a handler for this
        #[allow(clippy::unwrap_used)]
        let (_, handler_config) = handler_entry.unwrap();
        let handler_config = handler_config.unwrap();
        let retries = handler_config.retries;
        let should_discard = false;
        let queue = handler_config.queue;

        if message.retries < retries && !should_discard {
            message.retries += 1;
            WorkerEvent::ShouldRetry(queue)
        } else if !should_discard {
            WorkerEvent::ShouldDeadletter(queue)
        } else {
            WorkerEvent::DiscardMessage
        }
    }

    async fn retry_task(&mut self, queue: &str, message: &mut SerializableMessage) -> WorkerEvent {
        debug!(id=?self.id, "Marking job {} for retry", message.job_id);
        let future_time = Utc::now()
            + chrono::Duration::seconds(
                (message.retries.pow(4) + 15 + 30 * (message.retries + 1)) as i64, // Really should never have more than 64 bits of retries. Impossible even.
            );
        if let Err(broker_error) = self
            .shared_data
            .broker
            .enqueue(queue, message.clone(), Some(future_time))
            .await
        {
            error!(id=?self.id, "Retrying job failed, {:?}", broker_error);
        }
        WorkerEvent::RetryCompleted
    }

    async fn deadletter_task(
        &mut self,
        queue: &str,
        message: &mut SerializableMessage,
    ) -> WorkerEvent {
        // TODO: Remove these clones
        if let Err(broker_error) = self
            .shared_data
            .broker
            .deadletter(queue, message.clone())
            .await
        {
            error!(id=?self.id, "Deadlettering job failed, {:?}", broker_error);
        }
        WorkerEvent::DeadletterCompleted
    }

    async fn step(&mut self, state: &mut WorkerState) -> WorkerEvent {
        match state {
            WorkerState::Initialize => {
                if let Err(broker_error) = self
                    .shared_data
                    .broker
                    .register_worker(&self.id, self.queue)
                    .await
                {
                    // TODO: Should we panic here? Or abort? We couldn't even set up the worker
                    error!(id=?self.id, "Registering worker failed, {:?}", broker_error);
                }
                if let Err(broker_error) = self.shared_data.broker.heartbeat(&self.id).await {
                    error!(id=?self.id, "Heartbeat failed, {:?}", broker_error);
                }
                WorkerEvent::Initialized
            }
            WorkerState::WaitForTask => self.wait_for_task().await,
            WorkerState::HeartBeat => {
                debug!(id=?self.id, "Emitting heartbeat");
                if let Err(broker_error) = self.shared_data.broker.heartbeat(&self.id).await {
                    error!(id=?self.id, "Heartbeat failed, {:?}", broker_error);
                }
                WorkerEvent::HeartBeatCompleted
            }
            WorkerState::PreExecute(message) => self.pre_execute(message).await,
            WorkerState::Execute(message) => self.execute(message).await,
            WorkerState::PostExecute(message) => self.post_execute(message).await,
            WorkerState::ExecuteFailed(message) => self.execute_failed(message).await,
            WorkerState::RetryTask(queue, message) => self.retry_task(queue, message).await,
            WorkerState::DeadletterTask(queue, message) => {
                self.deadletter_task(queue, message).await
            }
            WorkerState::PreShutdown(message) => {
                info!(id=?self.id, "Shutting down worker");
                if let Some(message) = message {
                    debug!(id=?self.id, "Re-enqueueing message");
                    self.retry_task(self.queue, message).await;
                }
                WorkerEvent::Shutdown
            }
            WorkerState::Shutdown => {
                // For completeness, if we're shut down we should just shut down
                WorkerEvent::Shutdown
            }
        }
    }

    pub(crate) async fn run(mut self, mut shutdown_channel: Receiver<()>) {
        let mut state = WorkerState::Initialize;
        while !state.is_shutdown() {
            let event = select!(
                _ = shutdown_channel.recv() => {
                    WorkerEvent::ShouldShutdown
                },
                e = self.step(&mut state) => {
                    e
                }
            );
            state = state.next(event);
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::iter::FromIterator;
    use std::sync::atomic::{AtomicU64, Ordering};

    use snafu::Snafu;
    use tokio::time;

    use crate::middleware::extract::Extract;
    use crate::middleware::MockIronworkerMiddleware;
    use crate::test::{
        assert_send, assert_sync, boxed_task, enqueued_successful_message, failed, failed_message,
        message, successful, successful_message,
    };
    use crate::{broker::InProcessBroker, IntoTask, Task};
    use crate::{IronworkerMiddleware, Message};

    use super::*;

    fn shared_context() -> Arc<SharedData<InProcessBroker>> {
        Arc::new(SharedData {
            broker: InProcessBroker::default(),
            middleware: Default::default(),
            tasks: HashMap::from_iter([
                (
                    successful.task().name(),
                    (boxed_task(successful.task()), successful.task().config()),
                ),
                (
                    failed.task().name(),
                    (boxed_task(failed.task()), failed.task().config()),
                ),
            ]),
        })
    }

    fn shared_context_with_middleware(
        middleware: Vec<Box<dyn IronworkerMiddleware>>,
    ) -> Arc<SharedData<InProcessBroker>> {
        Arc::new(SharedData {
            broker: InProcessBroker::default(),
            middleware,
            tasks: HashMap::from_iter([
                (
                    successful.task().name(),
                    (boxed_task(successful.task()), successful.task().config()),
                ),
                (
                    failed.task().name(),
                    (boxed_task(failed.task()), failed.task().config()),
                ),
            ]),
        })
    }

    #[derive(Snafu, Debug)]
    enum TestEnum {
        #[snafu(display("The task failed"))]
        Failed,
    }

    #[tokio::test]
    async fn worker_machine_send_and_sync() {
        assert_send::<WorkerStateMachine<InProcessBroker>>();
        assert_sync::<WorkerStateMachine<InProcessBroker>>();
    }

    #[tokio::test]
    async fn wait_for_task_dequeues_a_message() {
        time::pause();

        let message = enqueued_successful_message();
        let shared = shared_context();

        shared
            .broker
            .enqueue("default", message.clone(), None)
            .await
            .unwrap();

        let mut worker = WorkerStateMachine::new("id".to_string(), "default", shared);
        let event = worker.wait_for_task().await;
        assert_eq!(event, WorkerEvent::TaskReceived(Box::new(message)));
    }

    #[tokio::test]
    async fn execute_calls_the_task() {
        time::pause();

        let counter = Arc::new(AtomicU64::new(0));

        async fn incr_counter(
            Message(count): Message<u64>,
            Extract(counter): Extract<Arc<AtomicU64>>,
        ) -> Result<(), TestEnum> {
            counter.fetch_add(count, Ordering::SeqCst);
            Ok(())
        }

        let mut message = message(
            incr_counter.task().name(),
            Box::new(incr_counter.task().into_performable_task()),
        );
        message.message_state.insert(counter.clone());

        let shared = Arc::new(SharedData {
            broker: InProcessBroker::default(),
            middleware: Default::default(),
            tasks: HashMap::from_iter([(
                incr_counter.task().name(),
                (
                    boxed_task(incr_counter.task()),
                    incr_counter.task().config(),
                ),
            )]),
        });

        let mut worker = WorkerStateMachine::new("id".to_string(), "default", shared);
        worker.execute(&mut message).await;
        assert_eq!(counter.load(Ordering::SeqCst), 123);
    }

    #[tokio::test]
    async fn execute_transitions_to_failed_on_err() {
        time::pause();

        let shared = shared_context();

        let mut worker = WorkerStateMachine::new("id".to_string(), "default", shared);
        let event = worker.execute(&mut failed_message()).await;
        assert_eq!(event, WorkerEvent::ExecuteFailed);
    }

    #[tokio::test]
    async fn execute_failed_retries_the_task() {
        time::pause();

        let mut message = enqueued_successful_message();
        let mut config = successful.task().config();
        config.retries.replace(1);
        let shared = Arc::new(SharedData {
            broker: InProcessBroker::default(),
            middleware: Default::default(),
            tasks: HashMap::from_iter([(
                successful.task().name(),
                (boxed_task(successful.task()), config),
            )]),
        });

        let mut worker = WorkerStateMachine::new("id".to_string(), "default", shared);
        let event = worker.execute_failed(&mut message).await;
        assert_eq!(event, WorkerEvent::ShouldRetry("default"));
    }

    #[tokio::test]
    async fn execute_failed_deadletters_the_task() {
        time::pause();

        let mut message = enqueued_successful_message();
        let mut config = successful.task().config();
        config.retries.replace(0);
        let shared = Arc::new(SharedData {
            broker: InProcessBroker::default(),
            middleware: Default::default(),
            tasks: HashMap::from_iter([(
                successful.task().name(),
                (boxed_task(successful.task()), config),
            )]),
        });

        let mut worker = WorkerStateMachine::new("id".to_string(), "default", shared);
        let event = worker.execute_failed(&mut message).await;
        assert_eq!(event, WorkerEvent::ShouldDeadletter("default"));
    }

    #[tokio::test]
    async fn state_machine_flow() {
        struct StateTest {
            from: WorkerState,
            to: WorkerState,
            event: WorkerEvent,
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
                to: WorkerState::PreExecute(enqueued_successful_message()),
                event: WorkerEvent::TaskReceived(Box::new(enqueued_successful_message())),
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
                from: WorkerState::PreExecute(enqueued_successful_message()),
                to: WorkerState::Execute(enqueued_successful_message()),
                event: WorkerEvent::PreExecuteCompleted,
            },
            StateTest {
                from: WorkerState::PreExecute(enqueued_successful_message()),
                to: WorkerState::ExecuteFailed(enqueued_successful_message()),
                event: WorkerEvent::PreExecuteFailed,
            },
            StateTest {
                from: WorkerState::Execute(enqueued_successful_message()),
                to: WorkerState::PostExecute(enqueued_successful_message()),
                event: WorkerEvent::ExecuteCompleted,
            },
            StateTest {
                from: WorkerState::Execute(enqueued_successful_message()),
                to: WorkerState::ExecuteFailed({
                    let mut message = failed_message();
                    message.task = "&ironworker_core::test::successful".to_string();
                    message.err = None;
                    message
                }),
                event: WorkerEvent::ExecuteFailed,
            },
            StateTest {
                from: WorkerState::PostExecute(successful_message()),
                to: WorkerState::WaitForTask,
                event: WorkerEvent::PostExecuteCompleted,
            },
            StateTest {
                from: WorkerState::RetryTask("default", failed_message()),
                to: WorkerState::PostExecute(failed_message()),
                event: WorkerEvent::RetryCompleted,
            },
            StateTest {
                from: WorkerState::DeadletterTask("default", failed_message()),
                to: WorkerState::PostExecute(failed_message()),
                event: WorkerEvent::DeadletterCompleted,
            },
            StateTest {
                from: WorkerState::ExecuteFailed(failed_message()),
                to: WorkerState::RetryTask("default", failed_message()),
                event: WorkerEvent::ShouldRetry("default"),
            },
            StateTest {
                from: WorkerState::ExecuteFailed(failed_message()),
                to: WorkerState::DeadletterTask("default", failed_message()),
                event: WorkerEvent::ShouldDeadletter("default"),
            },
            StateTest {
                from: WorkerState::WaitForTask,
                to: WorkerState::PreShutdown(None),
                event: WorkerEvent::ShouldShutdown,
            },
            StateTest {
                from: WorkerState::PreShutdown(None),
                to: WorkerState::Shutdown,
                event: WorkerEvent::Shutdown,
            },
        ];

        for StateTest { from, to, event } in transitions.into_iter() {
            assert_eq!(from.next(event.clone()), to, "Transitioning on {:?}", event);
        }
    }

    #[tokio::test]
    async fn pre_execute_calls_middleware() {
        let mut middleware = MockIronworkerMiddleware::new();
        middleware.expect_before_perform().times(1).return_const(());

        let mut message = enqueued_successful_message();
        let shared = shared_context_with_middleware(vec![Box::new(middleware)]);
        let mut worker = WorkerStateMachine::new("test-id".to_string(), "default", shared);
        assert_eq!(
            worker.pre_execute(&mut message).await,
            WorkerEvent::PreExecuteCompleted
        );
    }

    #[tokio::test]
    async fn post_execute_calls_middleware() {
        let mut middleware = MockIronworkerMiddleware::new();
        middleware.expect_after_perform().times(1).return_const(());

        let mut message = enqueued_successful_message();
        let shared = shared_context_with_middleware(vec![Box::new(middleware)]);
        let mut worker = WorkerStateMachine::new("test-id".to_string(), "default", shared);
        worker.post_execute(&mut message).await;
    }
}
