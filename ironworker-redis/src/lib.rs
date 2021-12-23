mod error;
mod worker;
mod broker;

pub use error::Result;
pub use worker::RedisWorker;
pub use broker::RedisBroker;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
