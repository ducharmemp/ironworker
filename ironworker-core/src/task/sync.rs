use std::error::Error;
use std::marker::PhantomData;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::from_value;

use crate::application::IronworkerApplication;
use crate::broker::Broker;
use crate::message::{Message, SerializableMessage};
use crate::{IntoTask, PerformableTask, Task};

use super::config::Config;
use super::{ConfigurableTask, FunctionTask};

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
            config: Config::default(),
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

    fn config(&'_ self) -> &'_ Config {
        &self.config
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

impl<T, F> ConfigurableTask for FunctionTask<(FunctionMarker, T), F>
where
    T: for<'de> Deserialize<'de> + Serialize + 'static + Send,
    F: Fn(Message<T>) -> Result<(), Box<dyn Error + Send>> + Send + Sync + 'static,
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
