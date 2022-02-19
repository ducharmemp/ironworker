use std::convert::TryInto;

use async_trait::async_trait;
use aws_config::Config;
use aws_sdk_sqs::{config::Builder, Client};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use ironworker_core::Broker;
use ironworker_core::SerializableMessage;
use serde_json::from_str;
use serde_json::to_string;
use snafu::prelude::*;

use crate::errors::{
    DeleteMessageFailedSnafu, DeserializeMessageFailedSnafu, GetQueueUrlFailedSnafu,
    SendMessageFailedSnafu, SqsBrokerError,
};
use crate::queue::Queue;

#[derive(Debug)]
pub struct SqsBroker {
    client: Client,
    queues: DashMap<String, Queue>,
}

impl SqsBroker {
    pub async fn new(config: &Config) -> SqsBroker {
        let client = Client::from_conf(Builder::from(config).build());
        Self {
            client,
            queues: Default::default(),
        }
    }

    pub fn from_client(client: Client) -> Self {
        Self {
            client,
            queues: Default::default(),
        }
    }
}

#[async_trait]
impl Broker for SqsBroker {
    type Error = SqsBrokerError;

    async fn register_worker(&self, _worker_id: &str, queue: &str) -> Result<(), Self::Error> {
        let queue = queue.to_string();

        let queue_url_output = self
            .client
            .get_queue_url()
            .queue_name(&queue)
            .send()
            .await
            .context(GetQueueUrlFailedSnafu)?;

        let queue_url = queue_url_output
            .queue_url
            .ok_or(SqsBrokerError::NoQueueUrl)?;

        self.queues
            .entry(queue)
            .or_insert_with(|| Queue::new(queue_url));

        Ok(())
    }

    async fn enqueue(
        &self,
        queue: &str,
        message: SerializableMessage,
        at: Option<DateTime<Utc>>,
    ) -> Result<(), Self::Error> {
        let queue = self.queues.get(&queue.to_string()).unwrap();
        let mut message_builder = self.client.send_message().queue_url(&queue.url);

        if let Some(at) = at {
            let delay_seconds = (Utc::now() - at).num_seconds();
            debug_assert!(delay_seconds < 15 * 60);
            debug_assert!(delay_seconds > 0);

            message_builder =
                message_builder.set_delay_seconds(Some(delay_seconds.try_into().unwrap()));
        }

        message_builder = message_builder.message_body(to_string(&message).unwrap());

        message_builder
            .send()
            .await
            .context(SendMessageFailedSnafu)?;
        Ok(())
    }

    async fn dequeue(&self, from: &str) -> Result<Option<SerializableMessage>, Self::Error> {
        let queue = self.queues.get(&from.to_string()).unwrap();
        let received = self
            .client
            .receive_message()
            .queue_url(&queue.url)
            .wait_time_seconds(5)
            .send()
            .await
            .unwrap();
        let mut messages = received.messages.unwrap_or_default();
        let message = messages.pop();
        if let Some(message) = message {
            if let Some(body) = message.body {
                let mut payload = from_str::<SerializableMessage>(&body)
                    .context(DeserializeMessageFailedSnafu {})?;
                payload.delivery_tag = message.receipt_handle;
                Ok(Some(payload))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    async fn acknowledge_processed(
        &self,
        from: &str,
        message: SerializableMessage,
    ) -> Result<(), Self::Error> {
        let queue = self.queues.get(&from.to_string()).unwrap();
        self.client
            .delete_message()
            .queue_url(&queue.url)
            .receipt_handle(message.delivery_tag.unwrap())
            .send()
            .await
            .context(DeleteMessageFailedSnafu)?;
        Ok(())
    }
}
