use async_trait::async_trait;
use aws_config::Config;
use aws_sdk_sqs::{config::Builder, Client};

use ironworker_core::Broker;
use ironworker_core::SerializableMessage;
use serde_json::from_str;
use serde_json::to_string;
use snafu::prelude::*;

use crate::errors::{
    DeleteMessageFailedSnafu, GetQueueUrlFailedSnafu, SendMessageFailedSnafu, SqsBrokerError,
};

pub struct SqsBroker {
    client: Client,
}

impl SqsBroker {
    pub async fn new(config: &Config) -> SqsBroker {
        let client = Client::from_conf(Builder::from(config).build());
        Self { client }
    }

    pub fn from_client(client: Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl Broker for SqsBroker {
    type Error = SqsBrokerError;

    async fn enqueue(&self, queue: &str, message: SerializableMessage) -> Result<(), Self::Error> {
        let queue = self
            .client
            .get_queue_url()
            .queue_name(queue)
            .send()
            .await
            .context(GetQueueUrlFailedSnafu)?;
        self.client
            .send_message()
            .queue_url(queue.queue_url.unwrap())
            .message_body(to_string(&message).unwrap())
            .send()
            .await
            .context(SendMessageFailedSnafu)?;
        Ok(())
    }

    async fn dequeue(&self, from: &str) -> Option<SerializableMessage> {
        let queue = self
            .client
            .get_queue_url()
            .queue_name(from)
            .send()
            .await
            .unwrap();
        let received = self
            .client
            .receive_message()
            .queue_url(queue.queue_url.unwrap())
            .wait_time_seconds(5)
            .send()
            .await
            .unwrap();
        let mut messages = received.messages.unwrap_or_default();
        let message = messages.pop()?;
        let mut payload = from_str::<SerializableMessage>(&message.body?).unwrap();
        payload.delivery_tag = message.receipt_handle;
        Some(payload)
    }

    async fn acknowledge_processed(
        &self,
        from: &str,
        message: SerializableMessage,
    ) -> Result<(), Self::Error> {
        let queue = self
            .client
            .get_queue_url()
            .queue_name(from)
            .send()
            .await
            .context(GetQueueUrlFailedSnafu)?;
        self.client
            .delete_message()
            .queue_url(queue.queue_url.unwrap())
            .receipt_handle(message.delivery_tag.unwrap())
            .send()
            .await
            .context(DeleteMessageFailedSnafu)?;
        Ok(())
    }
}
