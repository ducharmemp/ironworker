use thiserror::Error;
use sqlx::Error;

#[derive(Error, Debug)]
pub enum IronworkerPostgresError {
    #[error("unknown data store error")]
    Unknown,
    #[error(transparent)]
    SqlxError(#[from] Error)
}

pub type Result<T> = std::result::Result<T, IronworkerPostgresError>;