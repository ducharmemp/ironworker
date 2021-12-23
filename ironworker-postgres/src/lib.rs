mod broker;
mod error;
mod worker;

pub use broker::PostgresBroker;
pub use error::Result;
pub use worker::PostgresWorker;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
