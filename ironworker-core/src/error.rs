use serde_json::Error as SerdeError;
use snafu::Snafu;
use tokio::time::error::Elapsed;

use crate::{message::SerializableMessageBuilderError, task::TaskError};

#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum IronworkerError {
    #[snafu(display("Could not enqueue a task"))]
    CouldNotEnqueue,
    #[snafu(display("Could not dequeue a task"))]
    CouldNotDequeue,
    #[snafu(display("Job execution failed"))]
    PerformNowError { source: Box<dyn TaskError> },
    #[snafu(display("Job execution failed"))]
    PerformLaterError { source: Box<dyn TaskError> },
    #[snafu(display("Job execution timed out"))]
    TimeoutError { source: Elapsed },
    #[snafu(display("Could not extract state"))]
    CouldNotExtractState,
    #[snafu(display("Could not construct serializable message"))]
    CouldNotConstructSerializableMessage {
        source: SerializableMessageBuilderError,
    },
    #[snafu(display("Could not serialize payload"))]
    CouldNotSerializePayload { source: SerdeError },
}

pub type IronworkerResult<T> = Result<T, IronworkerError>;
