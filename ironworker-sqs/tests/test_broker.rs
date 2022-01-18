use aws_sdk_sqs::{Client, Endpoint};
use chrono::Utc;
use ironworker_core::{Broker, SerializableMessage};
use ironworker_sqs::SqsBroker;
use serde_json::Value;
use uuid::Uuid;

use testcontainers::{clients, Docker};
mod image;

use image::Sqs;

#[tokio::test]
async fn test_enqueue() {
    let docker = clients::Cli::default();
    let node = docker.run(Sqs::default().with_tag("latest"));
    let host_port = node.get_host_port(9324).unwrap();
    let shared_config = aws_config::load_from_env().await;
    let sqs_config_builder = aws_sdk_sqs::config::Builder::from(&shared_config);
    let sqs_config_builder = sqs_config_builder.endpoint_resolver(Endpoint::immutable(
        format!("http://localhost:{}", host_port)
            .try_into()
            .unwrap(),
    ));
    let config = sqs_config_builder.build();
    let queue = "test_enqueue";

    let client = Client::from_conf(config);
    client
        .create_queue()
        .queue_name(queue)
        .send()
        .await
        .unwrap();

    let broker = SqsBroker::from_client(client.clone());

    broker
        .enqueue(
            queue,
            SerializableMessage {
                job_id: Uuid::new_v4().to_string(),
                queue: queue.to_string(),
                task: "test_task".to_string(),
                payload: Value::String("test payload".to_string()),
                enqueued_at: Utc::now(),
                err: None,
                retries: 0,
                delivery_tag: None,
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn test_dequeue() {
    let docker = clients::Cli::default();
    let node = docker.run(Sqs::default().with_tag("latest"));
    let host_port = node.get_host_port(9324).unwrap();
    let shared_config = aws_config::load_from_env().await;
    let sqs_config_builder = aws_sdk_sqs::config::Builder::from(&shared_config);
    let sqs_config_builder = sqs_config_builder.endpoint_resolver(Endpoint::immutable(
        format!("http://localhost:{}", host_port)
            .try_into()
            .unwrap(),
    ));
    let config = sqs_config_builder.build();
    let queue = "test_dequeue";

    let client = Client::from_conf(config);
    client
        .create_queue()
        .queue_name(queue)
        .send()
        .await
        .unwrap();

    let broker = SqsBroker::from_client(client.clone());

    let enqueued_message = SerializableMessage {
        job_id: Uuid::new_v4().to_string(),
        queue: queue.to_string(),
        task: "test_task".to_string(),
        payload: Value::String("test payload".to_string()),
        enqueued_at: Utc::now(),
        err: None,
        retries: 0,
        delivery_tag: None,
    };
    broker
        .enqueue(queue, enqueued_message.clone())
        .await
        .unwrap();

    let message = broker.dequeue(queue).await;
    let dequeued = message.unwrap();
    assert_eq!(enqueued_message.job_id, dequeued.job_id);
    assert_eq!(enqueued_message.task, dequeued.task);
    assert_eq!(enqueued_message.queue, dequeued.queue);
    assert_eq!(enqueued_message.payload, dequeued.payload);
    assert_eq!(enqueued_message.enqueued_at, dequeued.enqueued_at);
    assert_eq!(enqueued_message.retries, 0);
    assert_eq!(enqueued_message.err, None);
    assert_eq!(dequeued.delivery_tag.is_some(), true);
}

#[tokio::test]
async fn test_dequeue_no_message() {
    let docker = clients::Cli::default();
    let node = docker.run(Sqs::default().with_tag("latest"));
    let host_port = node.get_host_port(9324).unwrap();
    let shared_config = aws_config::load_from_env().await;
    let sqs_config_builder = aws_sdk_sqs::config::Builder::from(&shared_config);
    let sqs_config_builder = sqs_config_builder.endpoint_resolver(Endpoint::immutable(
        format!("http://localhost:{}", host_port)
            .try_into()
            .unwrap(),
    ));
    let config = sqs_config_builder.build();
    let queue = "test_dequeue_no_message";

    let client = Client::from_conf(config);
    client
        .create_queue()
        .queue_name(queue)
        .send()
        .await
        .unwrap();

    let broker = SqsBroker::from_client(client.clone());

    let message = broker.dequeue(queue).await;
    assert_eq!(None, message);
}
