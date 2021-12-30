use std::error::Error;
use std::future::Future;
use std::marker::PhantomData;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::from_value;
use state::Container;

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

    async fn perform(
        &self,
        payload: SerializableMessage,
        _state: &Container![Send + Sync],
    ) -> Result<(), Box<dyn Error + Send>> {
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
        _payload: T,
    ) -> Result<(), Box<dyn Error + Send>> {
        // Need to fix this up, we need to have this go to a regular ol queue
        todo!();
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

macro_rules! impl_async_task_function {
    ($($param: ident),*) => {
        impl<T, F, Fut, $($param),*> IntoTask<(IsAsyncFunctionSystem, AsyncFunctionMarker, T, $($param),*)> for F
        where
            Fut: Future<Output = Result<(), Box<dyn Error + Send>>> + Send,
            T: for<'de> Deserialize<'de> + Serialize + 'static + Send,
            F: Fn(Message<T>, $(&$param),*) -> Fut + Send + Sync + 'static,
            $($param: Send + Sync + 'static),*
        {
            type Task = FunctionTask<(AsyncFunctionMarker, T, $($param),*), F>;
            fn task(self) -> Self::Task {
                FunctionTask {
                    func: self,
                    marker: PhantomData,
                    config: Config::default(),
                }
            }
        }

        #[async_trait]
        impl<T, F, Fut, $($param),*> Task for FunctionTask<(AsyncFunctionMarker, T, $($param),*), F>
        where
            Fut: Future<Output = Result<(), Box<dyn Error + Send>>> + Send,
            T: for<'de> Deserialize<'de> + Serialize + 'static + Send,
            F: Fn(Message<T>, $(&$param),*) -> Fut + Send + Sync + 'static,
            $($param: Send + Sync + 'static),*
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

            async fn perform(&self, payload: SerializableMessage, state: &Container![Send + Sync]) -> Result<(), Box<dyn Error + Send>> {
                let message: Message<T> = from_value::<T>(payload.payload).unwrap().into();
                (self.func)(message, $(state.try_get::<$param>().unwrap()),*).await
            }
        }

        #[async_trait]
        impl<T: Serialize + Send + Into<Message<T>>, F, Fut, $($param),*> PerformableTask<T>
            for FunctionTask<(AsyncFunctionMarker, T, $($param),*), F>
        where
            Fut: Future<Output = Result<(), Box<dyn Error + Send>>> + Send,
            T: for<'de> Deserialize<'de> + Serialize + 'static + Send,
            F: Fn(Message<T>, $(&$param),*) -> Fut + Send + Sync + 'static,
            $($param: Send + Sync + 'static),*
        {
            async fn perform_now<B: Broker + Send + Sync>(
                &self,
                _app: &IronworkerApplication<B>,
                _payload: T,
            ) -> Result<(), Box<dyn Error + Send>> {
                todo!();
            }
        }

        impl<T, F, Fut, $($param),*> ConfigurableTask for FunctionTask<(AsyncFunctionMarker, T, $($param),*), F>
        where
            Fut: Future<Output = Result<(), Box<dyn Error + Send>>> + Send,
            T: for<'de> Deserialize<'de> + Serialize + 'static + Send,
            F: Fn(Message<T>, $(&$param),*) -> Fut + Send + Sync + 'static,
            $($param: Send + Sync + 'static),*
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
    };
}

impl_async_task_function!(T1);
impl_async_task_function!(T1, T2);
impl_async_task_function!(T1, T2, T3);
impl_async_task_function!(T1, T2, T3, T4);
impl_async_task_function!(T1, T2, T3, T4, T5);
impl_async_task_function!(T1, T2, T3, T4, T5, T6);
impl_async_task_function!(T1, T2, T3, T4, T5, T6, T7);
impl_async_task_function!(T1, T2, T3, T4, T5, T6, T7, T8);
impl_async_task_function!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
impl_async_task_function!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
impl_async_task_function!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
impl_async_task_function!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);
impl_async_task_function!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13);
impl_async_task_function!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14);
impl_async_task_function!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15);
impl_async_task_function!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16);
