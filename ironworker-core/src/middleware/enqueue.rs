use std::convert::Infallible;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Serialize;
use serde_json::to_value;
use snafu::ResultExt;

use crate::application::SharedData;

use crate::error::{CouldNotConstructSerializableMessageSnafu, CouldNotSerializePayloadSnafu};
use crate::message::SerializableMessageBuilder;
use crate::task::Config as TaskConfig;
use crate::{Broker, Enqueuer, FromPayload, IronworkerError, Message, SerializableMessage};

#[derive(Debug, Clone)]
pub struct Enqueue<B: Broker> {
    shared_data: Arc<SharedData<B>>,
}

#[async_trait]
impl<B: Broker> FromPayload for Enqueue<B> {
    type Error = Infallible;

    async fn from_payload(message: &SerializableMessage) -> Result<Self, Self::Error> {
        #[allow(clippy::expect_used)]
        let shared = message
            .message_state
            .get::<Arc<SharedData<B>>>()
            .expect("Could not get broker from message state, this is a bug");
        Ok(Enqueue {
            shared_data: shared.clone(),
        })
    }
}

#[async_trait]
impl<B: Broker> Enqueuer for Enqueue<B> {
    async fn enqueue<P: Serialize + Send + Into<Message<P>>>(
        &self,
        task: &str,
        payload: P,
        task_config: TaskConfig,
    ) -> Result<(), IronworkerError> {
        let handler_config = {
            let (_, handler_config) = self.shared_data.tasks.get(task).unwrap();
            *handler_config
        };
        let merged_config = task_config.merge(handler_config);
        let unwrapped_config = merged_config.unwrap();
        let value = to_value(payload).context(CouldNotSerializePayloadSnafu {})?;

        let serializable = SerializableMessageBuilder::default()
            .task(task.to_string())
            .queue(unwrapped_config.queue.to_string())
            .payload(value)
            .at(unwrapped_config.at)
            .retries(0)
            .build()
            .context(CouldNotConstructSerializableMessageSnafu {})?;

        for middleware in self.shared_data.middleware.iter() {
            middleware.before_enqueue(&serializable).await;
        }

        self.shared_data
            .broker
            .enqueue(unwrapped_config.queue, serializable, unwrapped_config.at)
            .await
            .map_err(|_| IronworkerError::CouldNotEnqueue)?;

        // FIXME: This needs the serializable message even though we rightfully hand off ownership
        // for middleware in self.shared_data.middleware.iter() {
        //     middleware.after_enqueue(&serializable).await;
        // }

        Ok(())
    }
}
