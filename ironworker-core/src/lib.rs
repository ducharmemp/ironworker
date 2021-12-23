mod worker;
mod message;
mod broker;
mod task;

pub use worker::Worker;
pub use message::{Message, SerializableMessage};
pub use broker::Broker;
pub use task::{Task, IntoTask};


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
