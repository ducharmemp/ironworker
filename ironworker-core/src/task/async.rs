use std::future::Future;
use std::marker::PhantomData;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};

use super::base::{SendSyncStatic, TaskPayload};
use super::{Config, FunctionTask, IntoTask, Task, TaskError};

use crate::from_payload::FromPayload;
use crate::message::{Message, SerializableMessage};

#[allow(missing_debug_implementations)]
#[derive(Clone, Copy)]
pub struct IsAsyncFunctionSystem;

#[allow(missing_debug_implementations)]
#[derive(Clone, Copy)]
pub struct AsyncFunctionMarker;

macro_rules! impl_async_task_function {
    ($($param: ident),*) => {
        impl<T, F, Err, Fut, $($param),*> IntoTask<T, (IsAsyncFunctionSystem, AsyncFunctionMarker, T, $($param),*)> for F
        where
            Err: TaskError,
            Fut: Future<Output = Result<(), Err>> + Send,
            T: TaskPayload,
            F: FnOnce(Message<T>, $($param),*) -> Fut + SendSyncStatic + Clone,
            $($param: SendSyncStatic + FromPayload),*
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
        #[allow(non_snake_case)]
        impl<T, F, Err, Fut, $($param),*> Task<T> for FunctionTask<(AsyncFunctionMarker, T, $($param),*), F>
        where
            Err: TaskError,
            Fut: Future<Output = Result<(), Err>> + Send,
            T: TaskPayload,
            F: FnOnce(Message<T>, $($param),*) -> Fut + SendSyncStatic + Clone,
            $($param: SendSyncStatic + FromPayload),*
        {
            fn name(&self) -> &'static str {
                fn type_name_of<T>(_: T) -> &'static str {
                    std::any::type_name::<T>()
                }
                type_name_of(&self.func)
            }

            fn config(&self) -> Config {
                self.config.clone()
            }

            fn queue_as(mut self, queue_name: &'static str) -> Self {
                self.config.queue.replace(queue_name);
                self
            }

            fn wait_until(mut self, future_time: DateTime<Utc>) -> Self {
                self.config.at.replace(future_time);
                self
            }

            fn wait(mut self, delay: Duration) -> Self {
                self.config.at.replace(Utc::now() + delay);
                self
            }

            fn retries(mut self, count: u64) -> Self {
                self.config.retries.replace(count);
                self
            }

            async fn perform(self, payload: SerializableMessage) -> Result<(), Box<dyn TaskError>> {
                let message: Message<T> = Message::from_payload(&payload).await.unwrap();
                $(
                    let $param = $param::from_payload(&payload).await.unwrap();
                )*
                (self.func)(message, $($param,)*).await.map_err(|e| Box::new(e) as Box<_>)
            }
        }
    };
}

impl_async_task_function!();
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
    use std::convert::Infallible;

    use crate::enqueuer::MockEnqueuer;

    use super::*;

    #[tokio::test]
    async fn perform_later_enqueues_the_task() {
        async fn some_task(_payload: Message<u32>) -> Result<(), Infallible> {
            Ok(())
        }

        let mut mock = MockEnqueuer::new();
        mock.expect_enqueue::<u32>()
            .times(1)
            .return_once(|_, _, _| Ok(()));
        some_task.task().perform_later(&mock, 123).await.unwrap();
    }

    #[tokio::test]
    async fn name_gives_the_name_of_the_task() {
        async fn some_task(_payload: Message<u32>) -> Result<(), Infallible> {
            Ok(())
        }

        assert_eq!(some_task.task().name(), "&ironworker_core::task::async::test::name_gives_the_name_of_the_task::{{closure}}::some_task");
    }

    #[tokio::test]
    async fn queue_as_sets_the_queue() {
        async fn some_task(_payload: Message<u32>) -> Result<(), Infallible> {
            Ok(())
        }

        assert_eq!(some_task.task().config().queue, None);
        let tsk = some_task.task().queue_as("low");
        assert_eq!(tsk.config().queue, Some("low"));
    }

    #[tokio::test]
    async fn retry_on_sets_up_a_retry_handler_for_an_error() {}

    #[tokio::test]
    async fn discard_on_sets_up_a_discard_handler_for_an_error() {}

    #[tokio::test]
    async fn retries_sets_up_the_base_retries() {
        async fn some_task(_payload: Message<u32>) -> Result<(), Infallible> {
            Ok(())
        }

        assert_eq!(some_task.task().config().retries, None);
        let tsk = some_task.task().retries(5);
        assert_eq!(tsk.config().retries, Some(5));
    }
}
