#![deny(clippy::all, clippy::cargo)]

mod broker;
mod error;

pub use broker::PostgresBroker;
pub use error::Result;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
