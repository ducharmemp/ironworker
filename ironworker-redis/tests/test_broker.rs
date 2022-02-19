use chrono::Utc;
use ironworker_core::{Broker, SerializableMessage};
use ironworker_redis::RedisBroker;
use serde_json::Value;
use uuid::Uuid;

use testcontainers::{clients, images, Docker};

#[tokio::test]
async fn test_enqueue() {
    let docker = clients::Cli::default();
    let node = docker.run(images::redis::Redis::default().with_tag("latest"));
    let host_port = node.get_host_port(6379).unwrap();
    let broker = RedisBroker::new(&format!("redis://localhost:{}", host_port))
        .await
        .unwrap();
    let queue = "test_enqueue";
    broker
        .enqueue(
            queue,
            SerializableMessage {
                job_id: Uuid::new_v4(),
                queue: queue.to_string(),
                task: "test_task".to_string(),
                payload: Value::String("test payload".to_string()),
                enqueued_at: Some(Utc::now()),
                created_at: Utc::now(),
                at: None,
                err: None,
                retries: 0,
                delivery_tag: None,
                message_state: Default::default(),
            },
            None,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn test_dequeue() {
    let docker = clients::Cli::default();
    let node = docker.run(images::redis::Redis::default().with_tag("latest"));
    let host_port = node.get_host_port(6379).unwrap();
    let broker = RedisBroker::new(&format!("redis://localhost:{}", host_port))
        .await
        .unwrap();
    let queue = "test_dequeue";
    let enqueued_message = SerializableMessage {
        job_id: Uuid::new_v4(),
        queue: queue.to_string(),
        task: "test_task".to_string(),
        payload: Value::String("test payload".to_string()),
        enqueued_at: Some(Utc::now()),
        created_at: Utc::now(),
        at: None,
        err: None,
        retries: 0,
        delivery_tag: None,
        message_state: Default::default(),
    };
    broker
        .enqueue(queue, enqueued_message.clone(), None)
        .await
        .unwrap();

    let message = broker.dequeue(queue).await;
    assert_eq!(Some(enqueued_message), message.unwrap());
}

#[tokio::test]
async fn test_dequeue_no_message() {
    let docker = clients::Cli::default();
    let node = docker.run(images::redis::Redis::default().with_tag("latest"));
    let host_port = node.get_host_port(6379).unwrap();
    let broker = RedisBroker::new(&format!("redis://localhost:{}", host_port))
        .await
        .unwrap();
    let queue = "test_dequeue_no_message";
    let message = broker.dequeue(queue).await;
    assert_eq!(Ok(None), message);
}

#[tokio::test]
async fn test_deadletter() {
    let docker = clients::Cli::default();
    let node = docker.run(images::redis::Redis::default().with_tag("latest"));
    let host_port = node.get_host_port(6379).unwrap();
    let broker = RedisBroker::new(&format!("redis://localhost:{}", host_port))
        .await
        .unwrap();
    let queue = "test_deadletter";
    broker
        .deadletter(
            queue,
            SerializableMessage {
                job_id: Uuid::new_v4(),
                queue: queue.to_string(),
                task: "test_task".to_string(),
                payload: Value::String("test payload".to_string()),
                enqueued_at: Some(Utc::now()),
                created_at: Utc::now(),
                at: None,
                err: None,
                retries: 0,
                delivery_tag: None,
                message_state: Default::default(),
            },
        )
        .await
        .unwrap();
}
