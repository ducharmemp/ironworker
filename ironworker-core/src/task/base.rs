use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use snafu::AsErrorSource;

use crate::application::IronworkerApplication;
use crate::broker::Broker;
use crate::message::{Message, SerializableMessage};
use crate::IronworkerError;

use super::config::Config;
use super::IntoPerformableTask;

macro_rules! auxiliary_trait{
    ($traitname: ident, $($t:tt)*) => {
        pub trait $traitname : $($t)* {}
        impl<T> $traitname for T where T: $($t)* {}
    }
}

auxiliary_trait!(TaskError, AsErrorSource + Debug + 'static);
auxiliary_trait!(
    TaskPayload,
    for<'de> Deserialize<'de> + Serialize + 'static + Send
);
auxiliary_trait!(SendSyncStatic, Send + Sync + 'static);

#[async_trait]
pub trait Task<T: Serialize + Send + Into<Message<T>> + 'static>:
    SendSyncStatic + Sized + Clone
{
    fn name(&self) -> &'static str;
    fn config(&self) -> Config;

    #[must_use]
    fn queue_as(self, queue_name: &'static str) -> Self;
    #[must_use]
    fn retries(self, count: usize) -> Self;

    async fn perform(self, payload: SerializableMessage) -> Result<(), Box<dyn TaskError>>;

    async fn perform_now<B: Broker + 'static>(
        self,
        app: &IronworkerApplication<B>,
        payload: T,
    ) -> Result<(), IronworkerError>;

    async fn perform_later<B: Broker + 'static>(
        self,
        app: &IronworkerApplication<B>,
        payload: T,
    ) -> Result<(), IronworkerError> {
        let fut = app.enqueue(self.name(), payload);
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
