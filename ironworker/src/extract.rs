use std::{convert::Infallible, ops::Deref};

use async_trait::async_trait;
use ironworker_core::{
    from_payload::FromPayload, message::SerializableMessage, middleware::IronworkerMiddleware,
};

#[derive(Clone, Copy, Debug)]
pub struct AddMessageStateMiddleware<T> {
    value: T,
}

impl<T> AddMessageStateMiddleware<T> {
    pub fn new(value: T) -> Self {
        Self { value }
    }
}

#[async_trait]
impl<T> IronworkerMiddleware for AddMessageStateMiddleware<T>
where
    T: Send + Clone + Sync + 'static,
{
    async fn before_perform(&self, message: &mut SerializableMessage) {
        message.message_state.insert(self.value.clone());
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Extract<T>(pub T);

#[async_trait]
impl<T> FromPayload for Extract<T>
where
    T: Clone + Send + Sync + 'static,
{
    type Error = Infallible;

    async fn from_payload(message: &SerializableMessage) -> Result<Self, Self::Error> {
        #[allow(clippy::expect_used)]
        let value = message.message_state.get::<T>().expect("Could not get value from message state, was AddMessageStateMiddleware added to the Ironworker application?").clone();

        Ok(Extract(value))
    }
}

impl<T> Deref for Extract<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
