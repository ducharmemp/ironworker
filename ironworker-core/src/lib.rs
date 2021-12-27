#![deny(clippy::all, clippy::cargo)]

mod application;
mod broker;
mod message;
mod task;
mod worker_state;

pub use application::{IronworkerApplication, IronworkerApplicationBuilder};
pub use broker::Broker;
pub use message::{DeadLetterMessage, Message, SerializableMessage};
pub use task::{ConfigurableTask, IntoTask, PerformableTask, Task};
pub use worker_state::WorkerState;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
