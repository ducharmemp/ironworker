use async_trait::async_trait;

use crate::{task::TaskError, SerializableMessage};

#[async_trait]
pub trait FromPayload: Sized {
    type Error: TaskError + Send;

    async fn from_payload(message: &SerializableMessage) -> Result<Self, Self::Error>;
}
