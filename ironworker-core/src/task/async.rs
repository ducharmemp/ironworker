use std::error::Error;
use std::future::Future;
use std::marker::PhantomData;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::from_value;

use crate::application::IronworkerApplication;
use crate::broker::Broker;
use crate::message::{Message, SerializableMessage};
use crate::{ConfigurableTask, IntoTask, PerformableTask, Task};

use super::config::Config;
use super::FunctionTask;

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
            config: Config::default(),
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

    fn config(&'_ self) -> &'_ Config {
        &self.config
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

impl<T, F, Fut> ConfigurableTask for FunctionTask<(AsyncFunctionMarker, T), F>
where
    Fut: Future<Output = Result<(), Box<dyn Error + Send>>> + Send,
    T: for<'de> Deserialize<'de> + Serialize + 'static + Send,
    F: Fn(Message<T>) -> Fut + Send + Sync + 'static,
{
    fn queue_as(mut self, queue_name: &'static str) -> Self {
        self.config.queue = queue_name;
        self
    }

    fn retry_on<E: Into<Box<dyn Error + Send>>>(self, _err: E) -> Self {
        todo!()
    }

    fn discard_on<E: Into<Box<dyn Error + Send>>>(self, _err: E) -> Self {
        todo!()
    }

    fn retries(mut self, count: usize) -> Self {
        self.config.retries = count;
        self
    }
}
