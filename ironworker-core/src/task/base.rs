use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use snafu::AsErrorSource;
use state::Container;

use crate::application::IronworkerApplication;
use crate::broker::Broker;
use crate::message::{Message, SerializableMessage};
use crate::IronworkerError;

use super::config::Config;

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
auxiliary_trait!(ThreadSafeBroker, Broker + Send + Sync);

#[async_trait]
pub trait Task: SendSyncStatic {
    fn name(&self) -> &'static str;
    fn config(&self) -> &Config;

    async fn perform(
        &self,
        payload: SerializableMessage,
        state: &Container![Send + Sync],
    ) -> Result<(), Box<dyn TaskError>>;
}

#[async_trait]
pub trait PerformableTask<T: Serialize + Send + Into<Message<T>> + 'static>: Task {
    async fn perform_now<B: Broker + 'static>(
        &self,
        app: &IronworkerApplication<B>,
        payload: T,
    ) -> Result<(), IronworkerError>;

    async fn perform_later<B: Broker + 'static>(
        &self,
        app: &IronworkerApplication<B>,
        payload: T,
    ) -> Result<(), IronworkerError> {
        app.enqueue(self.name(), payload).await
    }
}

// This trait has to be generic because we have potentially overlapping impls, in particular
// because Rust thinks a type could impl multiple different `FnMut` combinations
// even though none can currently
pub trait IntoTask<Params> {
    type Task: Task;

    fn task(self) -> Self::Task;
}

#[allow(missing_debug_implementations)]
#[derive(Clone, Copy)]
pub struct AlreadyWasTask;

// Tasks implicitly implement IntoTask
impl<Tsk: Task> IntoTask<AlreadyWasTask> for Tsk {
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

pub trait ConfigurableTask: Task {
    #[must_use]
    fn queue_as(self, queue_name: &'static str) -> Self;
    #[must_use]
    fn retries(self, count: usize) -> Self;
}
