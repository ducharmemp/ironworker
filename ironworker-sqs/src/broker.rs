use async_trait::async_trait;
use aws_config::Config;
use aws_sdk_sqs::{config::Builder, Client, SdkError};

use ironworker_core::Broker;
use ironworker_core::SerializableMessage;
use serde_json::from_str;
use serde_json::to_string;

pub struct SqsBroker {
    client: Client,
}

impl SqsBroker {
    pub async fn new(config: &Config) -> SqsBroker {
        let client = Client::from_conf(Builder::from(config).build());
        Self { client }
    }

    pub async fn from_builder(builder: Builder) -> SqsBroker {
        let client = Client::from_conf(builder.build());
        Self { client }
    }
}

#[async_trait]
impl Broker for SqsBroker {
    type Error = ();

    async fn enqueue(&self, queue: &str, message: SerializableMessage) -> Result<(), Self::Error> {
        let queue = self
            .client
            .get_queue_url()
            .queue_name(queue)
            .send()
            .await
            .unwrap();
        self.client
            .send_message()
            .queue_url(queue.queue_url.unwrap())
            .message_body(to_string(&message).unwrap())
            .send()
            .await
            .unwrap();
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
        dbg!(&message);
        let queue = self
            .client
            .get_queue_url()
            .queue_name(from)
            .send()
            .await
            .unwrap();
        self.client
            .delete_message()
            .queue_url(queue.queue_url.unwrap())
            .receipt_handle(message.delivery_tag.unwrap())
            .send()
            .await
            .unwrap();
        Ok(())
    }
}
