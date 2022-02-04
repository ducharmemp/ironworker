use aws_sdk_sqs::{
    error::{DeleteMessageError, GetQueueUrlError, ReceiveMessageError, SendMessageError},
    SdkError,
};
use snafu::prelude::*;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum SqsBrokerError {
    #[snafu(display("Could not get the url for the queue"))]
    GetQueueUrlFailed { source: SdkError<GetQueueUrlError> },
    #[snafu(display("There was no URL available for the queue"))]
    NoQueueUrl,
    #[snafu(display("Could not enqueue message"))]
    SendMessageFailed { source: SdkError<SendMessageError> },
    #[snafu(display("Could not dequeue message"))]
    ReceiveMessagesFailed {
        source: SdkError<ReceiveMessageError>,
    },
    #[snafu(display("Could not ack message"))]
    DeleteMessageFailed {
        source: SdkError<DeleteMessageError>,
    },
}
