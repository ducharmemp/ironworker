mod application;
mod broker;
mod message;
mod task;
mod worker_state;

pub use application::IronworkerApplication;
pub use broker::Broker;
pub use message::{Message, SerializableMessage};
pub use task::{IntoTask, PerformableTask, Task};
pub use worker_state::WorkerState;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
