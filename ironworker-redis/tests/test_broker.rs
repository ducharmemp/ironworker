use chrono::{Duration, Utc};
use ironworker_core::{broker::Broker, message::SerializableMessage};
use ironworker_redis::RedisBroker;
use redis::{AsyncCommands, Client};
use serde_json::Value;
use uuid::Uuid;

use testcontainers::{clients, images, Docker};

#[tokio::test]
async fn test_enqueue() {
    let docker = clients::Cli::default();
    let node = docker.run(images::redis::Redis::default().with_tag("latest"));
    let host_port = node.get_host_port(6379).unwrap();
    let client = Client::open(format!("redis://localhost:{}", host_port)).unwrap();
    let broker = RedisBroker::from_client(&client).await.unwrap();

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
    let mut connection = client.get_async_connection().await.unwrap();
    assert_eq!(
        connection
            .llen::<_, u64>("queue:test_enqueue")
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn test_dequeue() {
    let docker = clients::Cli::default();
    let node = docker.run(images::redis::Redis::default().with_tag("latest"));
    let host_port = node.get_host_port(6379).unwrap();
    let client = Client::open(format!("redis://localhost:{}", host_port)).unwrap();
    let broker = RedisBroker::from_client(&client).await.unwrap();

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
    let mut connection = client.get_async_connection().await.unwrap();
    assert_eq!(
        connection
            .llen::<_, u64>("queue:test_dequeue")
            .await
            .unwrap(),
        0
    );
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
    let client = Client::open(format!("redis://localhost:{}", host_port)).unwrap();
    let broker = RedisBroker::from_client(&client).await.unwrap();
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

    let mut connection = client.get_async_connection().await.unwrap();
    assert_eq!(
        connection
            .zcard::<_, u64>("failed:test_deadletter")
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn test_scheduled() {
    let docker = clients::Cli::default();
    let node = docker.run(images::redis::Redis::default().with_tag("latest"));
    let host_port = node.get_host_port(6379).unwrap();
    let client = Client::open(format!("redis://localhost:{}", host_port)).unwrap();
    let broker = RedisBroker::from_client(&client).await.unwrap();
    let queue = "test_scheduled";
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
            Some(Utc::now() + Duration::days(5)),
        )
        .await
        .unwrap();

    let mut connection = client.get_async_connection().await.unwrap();
    assert_eq!(
        connection
            .zcard::<_, u64>("scheduled:test_scheduled")
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn test_acknowledge_processed() {
    let docker = clients::Cli::default();
    let node = docker.run(images::redis::Redis::default().with_tag("latest"));
    let host_port = node.get_host_port(6379).unwrap();
    let client = Client::open(format!("redis://localhost:{}", host_port)).unwrap();
    let broker = RedisBroker::from_client(&client).await.unwrap();
    let queue = "test_processed";
    broker
        .acknowledge_processed(
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

    let mut connection = client.get_async_connection().await.unwrap();
    assert_eq!(
        connection
            .hget::<_, _, u64>("ironworker:stats", "processed")
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn test_heartbeat() {
    let docker = clients::Cli::default();
    let node = docker.run(images::redis::Redis::default().with_tag("latest"));
    let host_port = node.get_host_port(6379).unwrap();
    let client = Client::open(format!("redis://localhost:{}", host_port)).unwrap();
    let broker = RedisBroker::from_client(&client).await.unwrap();
    let worker_id = "worker_id";
    broker.heartbeat(worker_id).await.unwrap();

    let mut connection = client.get_async_connection().await.unwrap();
    assert!(connection
        .hget::<_, _, Option<u64>>("worker:worker_id", "last_seen_at")
        .await
        .unwrap()
        .is_some());
}
