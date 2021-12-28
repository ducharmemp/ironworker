#![deny(clippy::all, clippy::cargo)]

mod application;
mod broker;
mod config;
mod message;
mod state;
mod task;
mod error;

pub use application::{IronworkerApplication, IronworkerApplicationBuilder};
pub use broker::Broker;
pub use message::{Message, SerializableMessage};
pub use state::{QueueState, WorkerState};
pub use task::{ConfigurableTask, IntoTask, PerformableTask, Task};
pub use error::{IronworkerError, IronworkerResult};

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
