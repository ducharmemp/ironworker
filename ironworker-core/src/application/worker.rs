use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::sleep;

use crate::{Broker, DeadLetterMessage, Task};

pub struct IronWorker<B: Broker> {
    id: String,
    pub(crate) broker: Arc<B>,
    pub(crate) tasks: Arc<HashMap<&'static str, Box<dyn Task>>>,
    pub(crate) queue: &'static str,
}

impl<B: Broker + Sync + Send + 'static> IronWorker<B> {
    pub fn new(
        id: String,
        broker: Arc<B>,
        tasks: Arc<HashMap<&'static str, Box<dyn Task>>>,
        queue: &'static str,
    ) -> Self {
        Self {
            id,
            broker,
            tasks,
            queue,
        }
    }

    pub async fn work(self) {
        loop {
            self.broker.heartbeat(&self.id).await;
            let message = self.broker.dequeue(&self.id, self.queue).await;

            match message {
                Some(message) => {
                    let task_name = message.task.clone();

                    let handler = self.tasks.get(&task_name.as_str());
                    if let Some(handler) = handler {
                        if let Err(e) = handler.perform(message.clone()).await {
                            self.broker
                                .deadletter(DeadLetterMessage::new(message, "default", e))
                                .await;
                        } else {
                            self.broker.mark_done(&self.id).await;
                        }
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
