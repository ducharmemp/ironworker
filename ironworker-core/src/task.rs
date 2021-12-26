use std::error::Error;
use std::future::Future;
use std::marker::PhantomData;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::from_value;

use crate::application::IronworkerApplication;
use crate::broker::Broker;
use crate::message::{Message, SerializableMessage};

#[async_trait]
pub trait Task: Send + Sync + 'static {
    fn name(&self) -> &'static str;

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
        app.enqueue("default", self.name(), payload).await
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
    func: F,
    marker: PhantomData<fn() -> Marker>,
}

pub struct IsFunctionSystem;
pub struct FunctionMarker;

impl<T, F> IntoTask<(IsFunctionSystem, FunctionMarker, T)> for F
where
    T: for<'de> Deserialize<'de> + Serialize + 'static + Send,
    F: Fn(Message<T>) -> Result<(), Box<dyn Error + Send>> + Send + Sync + 'static,
{
    type Task = FunctionTask<(FunctionMarker, T), F>;
    fn task(self) -> Self::Task {
        FunctionTask {
            func: self,
            marker: PhantomData,
        }
    }
}

#[async_trait]
impl<T, F> Task for FunctionTask<(FunctionMarker, T), F>
where
    T: for<'de> Deserialize<'de> + Serialize + 'static + Send,
    F: Fn(Message<T>) -> Result<(), Box<dyn Error + Send>> + Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        type_name_of(&self.func)
    }

    async fn perform(&self, payload: SerializableMessage) -> Result<(), Box<dyn Error + Send>> {
        let message: Message<T> = from_value::<T>(payload.payload).unwrap().into();
        (self.func)(message)
    }
}

#[async_trait]
impl<T: Serialize + Send + Into<Message<T>>, F> PerformableTask<T>
    for FunctionTask<(FunctionMarker, T), F>
where
    T: for<'de> Deserialize<'de> + Serialize + 'static + Send,
    F: Fn(Message<T>) -> Result<(), Box<dyn Error + Send>> + Send + Sync + 'static,
{
    async fn perform_now<B: Broker + Send + Sync>(
        &self,
        _app: &IronworkerApplication<B>,
        payload: T,
    ) -> Result<(), Box<dyn Error + Send>> {
        let message: Message<T> = payload.into();
        let serializable = SerializableMessage::from_message(self.name(), message);
        self.perform(serializable).await?;
        Ok(())
    }
}

// Async
pub struct IsAsyncFunctionSystem;
pub struct AsyncFunctionMarker;

impl<T, F, Fut> IntoTask<(IsAsyncFunctionSystem, AsyncFunctionMarker, T)> for F
where
    Fut: Future<Output = Result<(), Box<dyn Error + Send>>> + Send,
    T: for<'de> Deserialize<'de> + Serialize + 'static + Send,
    F: Fn(Message<T>) -> Fut + Send + Sync + 'static,
{
    type Task = FunctionTask<(AsyncFunctionMarker, T), F>;
    fn task(self) -> Self::Task {
        FunctionTask {
            func: self,
            marker: PhantomData,
        }
    }
}

#[async_trait]
impl<T, F, Fut> Task for FunctionTask<(AsyncFunctionMarker, T), F>
where
    Fut: Future<Output = Result<(), Box<dyn Error + Send>>> + Send,
    T: for<'de> Deserialize<'de> + Serialize + 'static + Send,
    F: Fn(Message<T>) -> Fut + Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        type_name_of(&self.func)
    }

    async fn perform(&self, payload: SerializableMessage) -> Result<(), Box<dyn Error + Send>> {
        let message: Message<T> = from_value::<T>(payload.payload).unwrap().into();
        (self.func)(message).await
    }
}

#[async_trait]
impl<T, F, Fut> PerformableTask<T> for FunctionTask<(AsyncFunctionMarker, T), F>
where
    Fut: Future<Output = Result<(), Box<dyn Error + Send>>> + Send,
    T: for<'de> Deserialize<'de> + Serialize + 'static + Send + Into<Message<T>>,
    F: Fn(Message<T>) -> Fut + Send + Sync + 'static,
{
    async fn perform_now<B: Broker + Send + Sync>(
        &self,
        _app: &IronworkerApplication<B>,
        payload: T,
    ) -> Result<(), Box<dyn Error + Send>> {
        let message: Message<T> = payload.into();
        let serializable = SerializableMessage::from_message(self.name(), message);
        self.perform(serializable).await?;
        Ok(())
    }
}
