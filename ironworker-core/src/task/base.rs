use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::to_value;
use snafu::{AsErrorSource, ResultExt};

use crate::application::IronworkerApplication;
use crate::broker::Broker;
use crate::error::{
    CouldNotConstructSerializableMessageSnafu, CouldNotSerializePayloadSnafu, PerformNowSnafu,
};
use crate::message::{Message, SerializableMessageBuilder};
use crate::{IronworkerError, SerializableMessage};

use super::config::Config;
use super::IntoPerformableTask;

macro_rules! auxiliary_trait{
    ($traitname: ident, $($t:tt)*) => {
        pub trait $traitname : $($t)* {}
        impl<T> $traitname for T where T: $($t)* {}
    }
}

auxiliary_trait!(TaskError, AsErrorSource + Debug + Send + 'static);
auxiliary_trait!(
    TaskPayload,
    for<'de> Deserialize<'de> + Serialize + 'static + Send
);
auxiliary_trait!(SendSyncStatic, Send + Sync + 'static);

/// Represents a task that can be `perform`ed.
#[async_trait]
pub trait Task<T: Serialize + Send + Into<Message<T>> + 'static>:
    SendSyncStatic + Sized + Clone
{
    /// Returns the name of the task
    fn name(&self) -> &'static str;
    /// Returns the config of the task
    fn config(&self) -> Config;

    /// Sets the queue for the task to run on, defaults to "default"
    #[must_use]
    fn queue_as(self, queue_name: &'static str) -> Self;
    /// Sets the number of times this task should be retried on failure. Defaults to 0
    #[must_use]
    fn retries(self, count: u64) -> Self;
    /// Sets up the task to be run at a future time.
    #[must_use]
    fn wait_until(self, future_time: DateTime<Utc>) -> Self;
    /// Sets up a task to be run at a future time, specified by a given offset
    #[must_use]
    fn wait(self, delay: Duration) -> Self;

    async fn perform(self, payload: SerializableMessage) -> Result<(), Box<dyn TaskError>>;

    async fn perform_now<B: Broker + 'static>(
        self,
        _app: &IronworkerApplication<B>,
        payload: T,
    ) -> Result<(), IronworkerError> {
        let unwrapped_config = self.config().unwrap();
        let value = to_value(payload).context(CouldNotSerializePayloadSnafu {})?;

        let message = SerializableMessageBuilder::default()
            .task(self.name().to_string())
            .queue("inline".to_string())
            .payload(value)
            .at(unwrapped_config.at)
            .retries(0)
            .build()
            .context(CouldNotConstructSerializableMessageSnafu {})?;
        self.perform(message).await.context(PerformNowSnafu {})
    }

    /// Enqueues a task in a backing datastore
    async fn perform_later<B: Broker + 'static>(
        self,
        app: &IronworkerApplication<B>,
        payload: T,
    ) -> Result<(), IronworkerError> {
        let fut = app.enqueue(self.name(), payload, self.config());
        fut.await
    }

    fn into_performable_task(self) -> IntoPerformableTask<Self, T> {
        IntoPerformableTask::new(self)
    }
}

pub trait IntoTask<T: Serialize + Send + Into<Message<T>> + 'static, Params> {
    type Task: Task<T>;

    fn task(self) -> Self::Task;
}

#[allow(missing_debug_implementations)]
#[derive(Clone, Copy)]
pub struct AlreadyWasTask;

// Tasks implicitly implement IntoTask
impl<T: Serialize + Send + Into<Message<T>> + 'static, Tsk: Task<T>> IntoTask<T, AlreadyWasTask>
    for Tsk
{
    type Task = Tsk;
    fn task(self) -> Tsk {
        self
    }
}

#[allow(missing_debug_implementations)]
pub struct FunctionTask<Marker, F> {
    pub(crate) func: F,
    pub(crate) marker: PhantomData<fn() -> Marker>,
    pub(crate) config: Config,
}

impl<Marker, F: Clone> Clone for FunctionTask<Marker, F> {
    fn clone(&self) -> FunctionTask<Marker, F> {
        FunctionTask {
            func: self.func.clone(),
            marker: PhantomData,
            config: self.config,
        }
    }
}
