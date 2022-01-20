use std::marker::PhantomData;

use async_trait::async_trait;
use serde::Serialize;

use crate::{SerializableMessage, Task};

use super::{Config, TaskError};

pub struct IntoPerformableTask<Tsk, T> {
    inner: Tsk,
    _marker: PhantomData<fn() -> T>,
}

impl<Tsk, T> IntoPerformableTask<Tsk, T> {
    pub(super) fn new(inner: Tsk) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }
}

impl<Tsk, T> std::fmt::Debug for IntoPerformableTask<Tsk, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("IntoPerformableTask")
            .field(&format_args!("..."))
            .finish()
    }
}

impl<Tsk, T> Clone for IntoPerformableTask<Tsk, T>
where
    Tsk: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _marker: PhantomData,
        }
    }
}

#[async_trait]
pub trait PerformableTask: Send {
    async fn perform(&mut self, payload: SerializableMessage) -> Result<(), Box<dyn TaskError>>;
    fn config(&self) -> Config;
    fn clone_box(&self) -> Box<dyn PerformableTask>;
}

#[async_trait]
impl<Tsk, T> PerformableTask for IntoPerformableTask<Tsk, T>
where
    Tsk: Task<T> + Clone + Send + 'static,
    T: Serialize + Send + 'static,
{
    fn clone_box(&self) -> Box<dyn PerformableTask> {
        Box::new(self.clone())
    }

    async fn perform(&mut self, payload: SerializableMessage) -> Result<(), Box<dyn TaskError>> {
        let inner = self.inner.clone();
        let fut = inner.perform(payload);
        fut.await
    }

    fn config(&self) -> Config {
        let inner = self.inner.clone();
        *inner.config()
    }
}