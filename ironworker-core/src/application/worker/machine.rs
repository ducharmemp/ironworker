use std::sync::Arc;
use std::time::Duration;

use tokio::select;
use tokio::sync::broadcast::Receiver;
use tokio::time::{interval, timeout, Interval};
use tracing::{debug, error, info};

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
    PostFailure(SerializableMessage),
    PreShutdown(Option<SerializableMessage>),
    Shutdown,
}

#[derive(PartialEq, Eq, Debug)]
pub(crate) enum WorkerEvent {
    Initialized,
    TaskReceived(SerializableMessage),
    NoTaskReceived,
    ShouldTryHeartbeat,
    HeartBeatCompleted,
    PreExecuteCompleted,
    PreExecuteFailed,
    ExecuteCompleted,
    ExecuteFailed(SerializableMessage),
    ShouldRetry(&'static str, SerializableMessage),
    ShouldDeadletter(&'static str, SerializableMessage),
    PostExecuteCompleted,
    RetryCompleted,
    DeadletterCompleted,
    PostFailureCompleted,
    ShouldShutdown,
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
            (Self::Execute(_), WorkerEvent::ExecuteFailed(message)) => Self::ExecuteFailed(message),

            // Post execution
            (Self::PostExecute(_), WorkerEvent::PostExecuteCompleted) => Self::WaitForTask,

            // Task Failure
            (Self::PostFailure(_), WorkerEvent::PostFailureCompleted) => Self::WaitForTask,
            (Self::RetryTask(_, message), WorkerEvent::RetryCompleted)
            | (Self::DeadletterTask(_, message), WorkerEvent::DeadletterCompleted) => {
                Self::PostFailure(message)
            }
            (Self::ExecuteFailed(_), WorkerEvent::ShouldRetry(queue, message)) => {
                Self::RetryTask(queue, message)
            }
            (Self::ExecuteFailed(_), WorkerEvent::ShouldDeadletter(queue, message)) => {
                Self::DeadletterTask(queue, message)
            }

            // Worker told to stop
            (Self::PreExecute(message), WorkerEvent::ShouldShutdown)
            | (Self::Execute(message), WorkerEvent::ShouldShutdown)
            | (Self::PostExecute(message), WorkerEvent::ShouldShutdown)
            | (Self::PostFailure(message), WorkerEvent::ShouldShutdown)
            | (Self::ExecuteFailed(message), WorkerEvent::ShouldShutdown) => {
                Self::PreShutdown(Some(message))
            }
            (_, WorkerEvent::ShouldShutdown) => Self::PreShutdown(None),
            (_, WorkerEvent::Shutdown) => Self::Shutdown,
            _ => unimplemented!(),
        }
    }

    /// Returns `true` if the worker state is [`Shutdown`].
    ///
    /// [`Shutdown`]: WorkerState::Shutdown
    pub(crate) fn is_shutdown(&self) -> bool {
        matches!(self, Self::Shutdown)
    }
}

pub(crate) struct WorkerStateMachine<B: Broker> {
    id: String,
    queue: &'static str,
    shared_data: Arc<SharedData<B>>,
    heartbeat_interval: Interval,
    state: WorkerState,
}

impl<B: Broker> WorkerStateMachine<B> {
    pub(crate) fn new(id: String, queue: &'static str, shared_data: Arc<SharedData<B>>) -> Self {
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

    async fn execute(&self, mut message: SerializableMessage) -> WorkerEvent {
        let handler = self.shared_data.tasks.get(&message.task.as_str());
        let handler = handler.unwrap();
        let max_run_time = handler.config().max_run_time;
        let task_future = timeout(
            Duration::from_secs(max_run_time),
            handler.perform(message.clone(), &self.shared_data.state),
        );

        match task_future.await {
            Ok(task_result) => {
                if let Err(err) = task_result {
                    message.err.replace(SerializableError::new(err));
                    WorkerEvent::ExecuteFailed(message)
                } else {
                    WorkerEvent::ExecuteCompleted
                }
            }
            Err(e) => {
                message
                    .err
                    .replace(SerializableError::new(Box::new(e) as Box<_>));
                WorkerEvent::ExecuteFailed(message)
            }
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

    async fn execute_failed(&self, message: &SerializableMessage) -> WorkerEvent {
        error!(id=?self.id, "Task {} failed", message.job_id);
        let mut message = message.clone();
        let handler = self.shared_data.tasks.get(&message.task.as_str());
        let handler = handler.unwrap();
        let handler_config = handler.config();
        let retries = handler_config.retries;
        let should_discard = false;
        let queue = handler_config.queue;
        // let retry_on_config = handler_config
        //     .retry_on
        //     .get(error_type_id)
        //     .cloned()
        //     .unwrap_or_default();
        // let queue = retry_on_config.queue.unwrap_or(handler_config.queue);
        // let retries = retry_on_config.attempts.unwrap_or(handler_config.retries);
        // let should_discard = handler_config.discard_on.contains(error_type_id);

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

    async fn post_failure(&self, _message: &SerializableMessage) -> WorkerEvent {
        debug!(id=?self.id, "Running failed hooks");
        for middleware in self.shared_data.middleware.iter() {
            middleware.on_task_failure().await;
        }
        WorkerEvent::PostFailureCompleted
    }

    async fn retry_task(&self, queue: &str, message: &SerializableMessage) -> WorkerEvent {
        debug!(id=?self.id, "Marking job {} for retry", message.job_id);
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
            WorkerState::Execute(message) => self.execute(message.clone()).await,
            WorkerState::PostExecute(message) => self.post_execute(message).await,
            WorkerState::ExecuteFailed(message) => self.execute_failed(message).await,
            WorkerState::RetryTask(queue, message) => self.retry_task(queue, message).await,
            WorkerState::DeadletterTask(queue, message) => {
                self.deadletter_task(queue, message).await
            }
            WorkerState::PostFailure(message) => self.post_failure(message).await,
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
        while !self.state.is_shutdown() {
            let event = select!(
                _ = shutdown_channel.recv() => {
                    WorkerEvent::ShouldShutdown
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
    use std::collections::HashMap;

    use async_trait::async_trait;
    use snafu::Snafu;
    use tokio::sync::Mutex;
    use tokio::time::{self, sleep};

    use crate::test::{
        boxed_task, enqueued_successful_message, failed, failed_message, successful,
        successful_message,
    };
    use crate::IronworkerMiddleware;
    use crate::{broker::InProcessBroker, ConfigurableTask, IntoTask, Message, Task};

    use super::*;

    fn shared_context() -> Arc<SharedData<InProcessBroker>> {
        Arc::new(SharedData {
            broker: InProcessBroker::default(),
            middleware: Default::default(),
            tasks: HashMap::from_iter([
                (successful.task().name(), boxed_task(successful.task())),
                (failed.task().name(), boxed_task(failed.task())),
            ]),
            state: Default::default(),
        })
    }

    fn shared_context_with_middleware(
        middleware: Vec<Box<dyn IronworkerMiddleware>>,
    ) -> Arc<SharedData<InProcessBroker>> {
        Arc::new(SharedData {
            broker: InProcessBroker::default(),
            middleware,
            tasks: HashMap::from_iter([
                (successful.task().name(), boxed_task(successful.task())),
                (failed.task().name(), boxed_task(failed.task())),
            ]),
            state: Default::default(),
        })
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
        time::pause();

        let message = enqueued_successful_message();
        let shared = shared_context();

        shared
            .broker
            .enqueue("default", message.clone())
            .await
            .unwrap();

        let mut worker = WorkerStateMachine::new("id".to_string(), "default", shared);
        let event = worker.wait_for_task().await;
        assert_eq!(event, WorkerEvent::TaskReceived(message));
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
                event: WorkerEvent::TaskReceived(enqueued_successful_message()),
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
                from: WorkerState::PreExecute(enqueued_successful_message()),
                to: WorkerState::Execute(enqueued_successful_message()),
                event: WorkerEvent::PreExecuteCompleted,
            },
            StateTest {
                from: WorkerState::Execute(enqueued_successful_message()),
                to: WorkerState::PostExecute(enqueued_successful_message()),
                event: WorkerEvent::ExecuteCompleted,
            },
            StateTest {
                from: WorkerState::Execute(enqueued_successful_message()),
                to: WorkerState::ExecuteFailed(failed_message()),
                event: WorkerEvent::ExecuteFailed(failed_message()),
            },
            StateTest {
                from: WorkerState::PostExecute(successful_message()),
                to: WorkerState::WaitForTask,
                event: WorkerEvent::PostExecuteCompleted,
            },
            StateTest {
                from: WorkerState::RetryTask("default", failed_message()),
                to: WorkerState::PostFailure(failed_message()),
                event: WorkerEvent::RetryCompleted,
            },
            StateTest {
                from: WorkerState::DeadletterTask("default", failed_message()),
                to: WorkerState::PostFailure(failed_message()),
                event: WorkerEvent::DeadletterCompleted,
            },
            StateTest {
                from: WorkerState::ExecuteFailed(failed_message()),
                to: WorkerState::RetryTask("default", failed_message()),
                event: WorkerEvent::ShouldRetry("default", failed_message()),
            },
            StateTest {
                from: WorkerState::ExecuteFailed(failed_message()),
                to: WorkerState::DeadletterTask("default", failed_message()),
                event: WorkerEvent::ShouldDeadletter("default", failed_message()),
            },
            StateTest {
                from: WorkerState::PostFailure(failed_message()),
                to: WorkerState::WaitForTask,
                event: WorkerEvent::PostFailureCompleted,
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
            assert_eq!(from.next(event), to);
        }
    }

    #[tokio::test]
    async fn pre_execute_calls_middleware() {
        let ctr = Arc::new(Mutex::new(0));

        struct TestMiddleware(Arc<Mutex<usize>>);

        #[async_trait]
        impl IronworkerMiddleware for TestMiddleware {
            async fn on_task_start(&self) {
                let mut ctr = self.0.lock().await;
                *ctr += 1;
            }
            async fn on_task_completion(&self) {
                unimplemented!();
            }
            async fn on_task_failure(&self) {
                unimplemented!();
            }
        }

        let message = enqueued_successful_message();
        let shared = shared_context_with_middleware(vec![Box::new(TestMiddleware(ctr.clone()))]);
        let worker = WorkerStateMachine::new("test-id".to_string(), "default", shared);
        worker.pre_execute(&message).await;
        assert_eq!(*ctr.lock().await, 1);
    }

    #[tokio::test]
    async fn post_execute_calls_middleware() {
        let ctr = Arc::new(Mutex::new(0));

        struct TestMiddleware(Arc<Mutex<usize>>);

        #[async_trait]
        impl IronworkerMiddleware for TestMiddleware {
            async fn on_task_start(&self) {
                unimplemented!();
            }
            async fn on_task_completion(&self) {
                let mut ctr = self.0.lock().await;
                *ctr += 1;
            }
            async fn on_task_failure(&self) {
                unimplemented!();
            }
        }

        let message = enqueued_successful_message();
        let shared = shared_context_with_middleware(vec![Box::new(TestMiddleware(ctr.clone()))]);
        let worker = WorkerStateMachine::new("test-id".to_string(), "default", shared);
        worker.post_execute(&message).await;
        assert_eq!(*ctr.lock().await, 1);
    }
}
