mod broker;
mod error;
mod message;

pub use broker::RedisBroker;
pub use error::Result;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
