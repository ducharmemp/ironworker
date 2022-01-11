use snafu::Snafu;

#[derive(Snafu, Debug, Copy, Clone)]
pub enum IronworkerError {
    #[snafu(display("Could not enqueue a task"))]
    CouldNotEnqueue,
    #[snafu(display("Could not dequeue a task"))]
    CouldNotDequeue,
}

pub type IronworkerResult<T> = Result<T, IronworkerError>;
