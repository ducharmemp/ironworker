use std::error::Error;
use std::marker::PhantomData;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::from_value;
use state::Container;

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

    async fn perform(
        &self,
        payload: SerializableMessage,
        _state: &Container![Send + Sync],
    ) -> Result<(), Box<dyn Error + Send>> {
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
        _payload: T,
    ) -> Result<(), Box<dyn Error + Send>> {
        todo!();
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

macro_rules! impl_task_function {
    ($($param: ident),*) => {
        impl<T, F, $($param),*> IntoTask<(IsFunctionSystem, FunctionMarker, T, $($param),*)> for F
        where
            T: for<'de> Deserialize<'de> + Serialize + 'static + Send,
            F: Fn(Message<T>, $(&$param),*) -> Result<(), Box<dyn Error + Send>> + Send + Sync + 'static,
            $($param: Send + Sync + 'static),*
        {
            type Task = FunctionTask<(FunctionMarker, T, $($param),*), F>;
            fn task(self) -> Self::Task {
                FunctionTask {
                    func: self,
                    marker: PhantomData,
                    config: Config::default(),
                }
            }
        }

        #[async_trait]
        impl<T, F, $($param),*> Task for FunctionTask<(FunctionMarker, T, $($param),*), F>
        where
            T: for<'de> Deserialize<'de> + Serialize + 'static + Send,
            F: Fn(Message<T>, $(&$param),*) -> Result<(), Box<dyn Error + Send>> + Send + Sync + 'static,
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
                (self.func)(message, $(state.try_get::<$param>().unwrap()),*)
            }
        }

        #[async_trait]
        impl<T: Serialize + Send + Into<Message<T>>, F, $($param),*> PerformableTask<T>
            for FunctionTask<(FunctionMarker, T, $($param),*), F>
        where
            T: for<'de> Deserialize<'de> + Serialize + 'static + Send,
            F: Fn(Message<T>, $(&$param),*) -> Result<(), Box<dyn Error + Send>> + Send + Sync + 'static,
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

        impl<T, F, $($param),*> ConfigurableTask for FunctionTask<(FunctionMarker, T, $($param),*), F>
        where
            T: for<'de> Deserialize<'de> + Serialize + 'static + Send,
            F: Fn(Message<T>, $(&$param),*) -> Result<(), Box<dyn Error + Send>> + Send + Sync + 'static,
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

impl_task_function!(T1);
impl_task_function!(T1, T2);
impl_task_function!(T1, T2, T3);
impl_task_function!(T1, T2, T3, T4);
impl_task_function!(T1, T2, T3, T4, T5);
impl_task_function!(T1, T2, T3, T4, T5, T6);
impl_task_function!(T1, T2, T3, T4, T5, T6, T7);
impl_task_function!(T1, T2, T3, T4, T5, T6, T7, T8);
impl_task_function!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
impl_task_function!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
impl_task_function!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
impl_task_function!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);
impl_task_function!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13);
impl_task_function!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14);
impl_task_function!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15);
impl_task_function!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16);
