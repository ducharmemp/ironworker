use std::collections::HashMap;
use std::sync::Arc;

use flume::{bounded, Receiver};
use serde::Serialize;
use tokio::time::{sleep, Duration};

use crate::{message::DeadLetterMessage, Broker, Message, SerializableMessage, Task, WorkerState};

pub struct IronworkerApplication<B: Broker> {
    pub(crate) id: String,
    pub(crate) broker: Arc<B>,
    pub(crate) tasks: Arc<HashMap<&'static str, Box<dyn Task>>>,
}

impl<B: Broker + Sync + Send + 'static> IronworkerApplication<B> {
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

    fn spawn_consumer_task(&self, rx: Receiver<SerializableMessage>) {
        let broker = self.broker.clone();
        let tasks = self.tasks.clone();

        tokio::task::spawn(async move {
            while let Ok(message) = rx.recv_async().await {
                let task_name = message.task.clone();

                let handler = tasks.get(&task_name.as_str());
                if let Some(handler) = handler {
                    if let Err(e) = handler.perform(message.clone()).await {
                        broker
                            .deadletter(DeadLetterMessage::new(message, "default", e))
                            .await;
                    }
                }
            }
        });
    }

    pub async fn run(&self) {
        let (tx, rx) = bounded::<SerializableMessage>(50);
        (0..25).for_each(|_| self.spawn_consumer_task(rx.clone()));

        loop {
            self.broker.heartbeat(&self.id).await;
            let task = self.broker.dequeue("default").await;

            match task {
                Some(task) => {
                    if let Err(_) = tx.try_send(task.clone()) {
                        self.broker.put_back(task).await;
                    }
                }
                None => {
                    dbg!("Sleeping for 5 seconds...");
                    sleep(Duration::from_millis(5000)).await;
                }
            }
        }
    }
}
