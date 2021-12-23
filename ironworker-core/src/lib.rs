mod broker;
mod message;
mod task;
mod worker;

pub use broker::Broker;
pub use message::{Message, SerializableMessage};
pub use task::{IntoTask, Task};
pub use worker::Worker;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
