use thiserror::Error;

#[derive(Error, Debug)]
pub enum IronworkerError {
    #[error("Could not enqueue a task")]
    CouldNotEnqueue,
    #[error("Could not dequeue a task")]
    CouldNotDequeue,
}

pub type IronworkerResult<T> = Result<T, IronworkerError>;
