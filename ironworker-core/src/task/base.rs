use std::error::Error;
use std::marker::PhantomData;

use async_trait::async_trait;
use serde::Serialize;
use state::Container;

use crate::application::IronworkerApplication;
use crate::broker::Broker;
use crate::message::{Message, SerializableMessage};

use super::config::Config;
use super::error::TaggedError;

#[async_trait]
pub trait Task: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn config(&self) -> &Config;

    async fn perform(
        &self,
        payload: SerializableMessage,
        state: &Container![Send + Sync],
    ) -> Result<(), TaggedError>;
}

#[async_trait]
pub trait PerformableTask<T: Serialize + Send + Into<Message<T>> + 'static>: Task {
    async fn perform_now<B: Broker + 'static>(
        &self,
        app: &IronworkerApplication<B>,
        payload: T,
    ) -> Result<(), Box<dyn Error + Send>>;

    async fn perform_later<B: Broker + 'static>(&self, app: &IronworkerApplication<B>, payload: T) {
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

#[derive(Clone, Copy)]
pub struct AlreadyWasTask;

// Tasks implicitly implement IntoTask
impl<Tsk: Task> IntoTask<AlreadyWasTask> for Tsk {
    type Task = Tsk;
    fn task(self) -> Tsk {
        self
    }
}

pub struct FunctionTask<Marker, F> {
    pub(crate) func: F,
    pub(crate) marker: PhantomData<fn() -> Marker>,
    pub(crate) config: Config,
}

pub trait ConfigurableTask: Task {
    #[must_use]
    fn queue_as(self, queue_name: &'static str) -> Self;
    #[must_use]
    fn retry_on<E: Into<TaggedError>>(self, err: E) -> Self;
    #[must_use]
    fn discard_on<E: Into<TaggedError>>(self, err: E) -> Self;
    #[must_use]
    fn retries(self, count: usize) -> Self;
}
