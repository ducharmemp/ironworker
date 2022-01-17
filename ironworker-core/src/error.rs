use snafu::Snafu;

use crate::task::TaskError;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum IronworkerError {
    #[snafu(display("Could not enqueue a task"))]
    CouldNotEnqueue,
    #[snafu(display("Could not dequeue a task"))]
    CouldNotDequeue,
    #[snafu(display("Job execution failed"))]
    PerformNowError { source: Box<dyn TaskError> },
}

pub type IronworkerResult<T> = Result<T, IronworkerError>;
