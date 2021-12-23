use thiserror::Error;
use redis::RedisError;

#[derive(Error, Debug)]
pub enum IronworkerRedisError {
    #[error("unknown data store error")]
    Unknown,
    #[error(transparent)]
    SqlxError(#[from] RedisError)
}

pub type Result<T> = std::result::Result<T, IronworkerRedisError>;