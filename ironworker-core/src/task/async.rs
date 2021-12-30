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

#[cfg(test)]
mod test {
    use crate::{broker::InProcessBroker, IronworkerApplicationBuilder};

    use super::*;

    // TODO: Need to fix this test, I can't figure out a good way of getting a Fn instead of a FnOnce
    // #[tokio::test]
    // async fn perform_runs_the_task() {
    //     let status_mock_called = Arc::new(Mutex::new(false));

    //     let status_mock = {
    //         let inner = status_mock_called.clone();
    //         |_state: Message<u32>| async move {
    //             let mut write_guard = inner.lock().unwrap();
    //             *write_guard = true;
    //             Ok(())
    //         }
    //     };

    //     let payload: Message<u32> = 123.into();
    //     let res = status_mock
    //         .task()
    //         .perform(
    //             SerializableMessage::from_message("status_mock", payload),
    //             &Default::default(),
    //         )
    //         .await;
    //     assert_eq!(res.unwrap(), ());
    //     assert_eq!(*status_mock_called.lock().unwrap(), true);
    // }

    #[tokio::test]
    async fn perform_now_enqueues_the_task() {}

    #[tokio::test]
    async fn perform_later_enqueues_the_task() {
        async fn some_task(_payload: Message<u32>) -> Result<(), Box<dyn Error + Send>> {
            Ok(())
        }

        let app = IronworkerApplicationBuilder::default()
            .broker(InProcessBroker::default())
            .register_task(some_task.task())
            .build();
        some_task.task().perform_later(&app, 123).await;
        assert_eq!(app.broker.queues.lock().await["default"].len(), 1);
    }

    #[tokio::test]
    async fn name_gives_the_name_of_the_task() {
        async fn some_task(_payload: Message<u32>) -> Result<(), Box<dyn Error + Send>> {
            Ok(())
        }

        assert_eq!(some_task.task().name(), "&ironworker_core::task::async::test::name_gives_the_name_of_the_task::{{closure}}::some_task");
    }

    #[tokio::test]
    async fn queue_as_sets_the_queue() {
        async fn some_task(_payload: Message<u32>) -> Result<(), Box<dyn Error + Send>> {
            Ok(())
        }

        assert_eq!(some_task.task().config().queue, "default");
        let tsk = some_task.task().queue_as("low");
        assert_eq!(tsk.config().queue, "low");
    }

    #[tokio::test]
    async fn retry_on_sets_up_a_retry_handler_for_an_error() {}

    #[tokio::test]
    async fn discard_on_sets_up_a_discard_handler_for_an_error() {}

    #[tokio::test]
    async fn retries_sets_up_the_base_retries() {
        async fn some_task(_payload: Message<u32>) -> Result<(), Box<dyn Error + Send>> {
            Ok(())
        }

        assert_eq!(some_task.task().config().retries, 0);
        let tsk = some_task.task().retries(5);
        assert_eq!(tsk.config().retries, 5);
    }
}
