use std::error::Error;
use std::marker::PhantomData;

use async_trait::async_trait;
use serde::Serialize;

use crate::application::IronworkerApplication;
use crate::broker::Broker;
use crate::message::{Message, SerializableMessage};

use super::config::Config;

#[async_trait]
pub trait Task: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn config(&'_ self) -> &'_ Config;

    async fn perform(&self, payload: SerializableMessage) -> Result<(), Box<dyn Error + Send>>;
}

#[async_trait]
pub trait PerformableTask<T: Serialize + Send + Into<Message<T>> + 'static>: Task {
    async fn perform_now<B: Broker + Send + Sync + 'static>(
        &self,
        app: &IronworkerApplication<B>,
        payload: T,
    ) -> Result<(), Box<dyn Error + Send>>;

    async fn perform_later<B: Broker + Send + Sync + 'static>(
        &self,
        app: &IronworkerApplication<B>,
        payload: T,
    ) {
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
    fn queue_as(self, queue_name: &'static str) -> Self;
    fn retry_on<E: Into<Box<dyn Error + Send>>>(self, err: E) -> Self;
    fn discard_on<E: Into<Box<dyn Error + Send>>>(self, err: E) -> Self;
    fn retries(self, count: usize) -> Self;
}
