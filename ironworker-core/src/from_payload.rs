use async_trait::async_trait;

use crate::{task::TaskError, SerializableMessage};

/// A trait describing something that can be constructed from a given payload.
#[async_trait]
pub trait FromPayload: Sized {
    type Error: TaskError + Send;

    async fn from_payload(message: &SerializableMessage) -> Result<Self, Self::Error>;
}
