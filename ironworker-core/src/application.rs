use std::collections::HashMap;

use serde::Serialize;
use tokio::time::{sleep, Duration};
use uuid::Uuid;

use crate::{message::DeadLetterMessage, Broker, Message, SerializableMessage, Task, WorkerState};

pub struct IronworkerApplication<B: Broker> {
    id: String,
    broker: B,
    tasks: HashMap<&'static str, Box<dyn Task>>,
}

impl<B: Broker> IronworkerApplication<B> {
    pub fn new(broker: B) -> IronworkerApplication<B> {
        Self {
            id: format!("worker:{}", Uuid::new_v4()),
            tasks: HashMap::new(),
            broker,
        }
    }

    pub async fn register_task<T: Task + Send>(&mut self, task: T) {
        self.tasks
            .entry(task.name())
            .or_insert_with(|| Box::new(task));
    }

    pub async fn list_workers(&self) -> Vec<WorkerState> {
        self.broker.list_workers().await
    }

    pub async fn list_queues(&self) -> Vec<String> {
        self.broker.list_queues().await
    }

    pub async fn enqueue<P: Serialize + Send + Into<Message<P>>>(
        &self,
        queue: &str,
        task: &str,
        payload: P,
    ) {
        let message: Message<P> = payload.into();
        let serializable = SerializableMessage::from_message(task, message);
        self.broker.enqueue(queue, serializable).await;
    }

    async fn run_task(
        &self,
        task: SerializableMessage,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let task_name = task.task.clone();
        let handler = self.tasks.get(&task_name.as_str());
        if let Some(handler) = handler {
            handler.perform(task).await?;
        }

        Ok(())
    }

    pub async fn run(&self) {
        loop {
            self.broker.heartbeat(&self.id).await;
            let task = self.broker.dequeue("default").await;
            match task {
                Some(task) => {
                    if let Err(e) = self.run_task(task.clone()).await {
                        self.broker
                            .deadletter(DeadLetterMessage::new(task, "default", e))
                            .await;
                    }
                }
                None => {
                    sleep(Duration::from_millis(5000)).await;
                }
            }
        }
    }
}
