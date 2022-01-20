use async_trait::async_trait;

use crate::{task::TaskError, Message};

#[async_trait]
pub trait FromPayload<T>: Sized {
    type Error: TaskError + Send;

    async fn from_payload(message: &Message<T>) -> Result<Self, Self::Error>;
}
