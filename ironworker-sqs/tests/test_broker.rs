use aws_sdk_sqs::Endpoint;
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
        format!("http://localhost:{}", host_port).try_into().unwrap(),
    ));
    let broker = SqsBroker::from_builder(sqs_config_builder).await;
    let queue = "test_enqueue";
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
        format!("http://localhost:{}", host_port).try_into().unwrap(),
    ));
    let broker = SqsBroker::from_builder(sqs_config_builder).await;
    let queue = "test_dequeue";
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
    assert_eq!(enqueued_message, message.unwrap());
}

#[tokio::test]
async fn test_dequeue_no_message() {
    let docker = clients::Cli::default();
    let node = docker.run(Sqs::default().with_tag("latest"));
    let host_port = node.get_host_port(9324).unwrap();
    let shared_config = aws_config::load_from_env().await;
    let sqs_config_builder = aws_sdk_sqs::config::Builder::from(&shared_config);
    let sqs_config_builder = sqs_config_builder.endpoint_resolver(Endpoint::immutable(
        format!("http://localhost:{}", host_port).try_into().unwrap(),
    ));
    let broker = SqsBroker::from_builder(sqs_config_builder).await;
    let queue = "test_dequeue_no_message";
    let message = broker.dequeue(queue).await;
    assert_eq!(None, message);
}

#[tokio::test]
async fn test_deadletter() {
    let docker = clients::Cli::default();
    let node = docker.run(Sqs::default().with_tag("latest"));
    let host_port = node.get_host_port(9324).unwrap();
    let shared_config = aws_config::load_from_env().await;
    let sqs_config_builder = aws_sdk_sqs::config::Builder::from(&shared_config);
    let sqs_config_builder = sqs_config_builder.endpoint_resolver(Endpoint::immutable(
        format!("http://localhost:{}", host_port).try_into().unwrap(),
    ));
    let broker = SqsBroker::from_builder(sqs_config_builder).await;
    let queue = "test_deadletter";
    broker
        .deadletter(
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
