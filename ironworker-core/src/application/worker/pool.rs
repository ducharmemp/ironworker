use std::sync::atomic::Ordering;
use std::sync::{atomic::AtomicUsize, Arc};

use futures::future::select_all;

use tokio::select;
use tokio::sync::broadcast::{channel, Receiver, Sender};
use tokio::task::JoinHandle;
use tracing::info;

use super::WorkerStateMachine;
use crate::application::shared::SharedData;
use crate::Broker;

pub struct IronWorkerPool<B: Broker> {
    id: String,
    queue: &'static str,
    worker_count: usize,
    shutdown_channel: Receiver<()>,
    current_worker_index: AtomicUsize,
    worker_shutdown_channel: Sender<()>,
    shared_data: Arc<SharedData<B>>,
}

impl<B: Broker + Sync + Send + 'static> IronWorkerPool<B> {
    pub fn new(
        id: String,
        queue: &'static str,
        worker_count: usize,
        shutdown_channel: Receiver<()>,
        shared_data: Arc<SharedData<B>>,
    ) -> Self {
        Self {
            id,
            queue,
            worker_count,
            shutdown_channel,
            current_worker_index: AtomicUsize::new(0),
            worker_shutdown_channel: channel(1).0,
            shared_data,
        }
    }

    fn spawn_worker(&self) -> JoinHandle<()> {
        let index = self.current_worker_index.fetch_add(1, Ordering::SeqCst);
        let id = format!("{}-{}-{}", self.id.clone(), self.queue, index);
        let queue = self.queue;
        let rx = self.worker_shutdown_channel.subscribe();
        info!(id=?id, "Booting worker {}", &id);
        let worker = WorkerStateMachine::new(id, queue, self.shared_data.clone());
        tokio::task::spawn(async move { worker.run(rx).await })
    }

    pub async fn work(mut self) {
        info!(id=?self.id, queue=?self.queue, "Booting worker pool with {} workers", self.worker_count);
        let mut worker_handles: Vec<_> = (0..self.worker_count)
            .map(|_| self.spawn_worker())
            .collect();

        loop {
            select!(
                _ = self.shutdown_channel.recv() => {
                    self.worker_shutdown_channel.send(()).expect("All workers were dropped");
                    return;
                },
                (_res, _, rest) = select_all(worker_handles) => {
                    worker_handles = rest;
                    worker_handles.push(self.spawn_worker());
                }
            );
        }
    }
}
