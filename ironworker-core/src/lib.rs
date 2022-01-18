#![forbid(unsafe_code)]
#![deny(
    clippy::all,
    clippy::cargo,
    nonstandard_style,
    rust_2018_idioms,
    clippy::dbg_macro,
    clippy::todo,
    clippy::empty_enum,
    clippy::enum_glob_use,
    clippy::inefficient_to_string,
    clippy::option_option,
    clippy::unnested_or_patterns,
    clippy::needless_continue,
    clippy::needless_borrow,
    private_in_public,
    unreachable_code,
    unreachable_patterns,
)]
#![forbid(non_ascii_idents, unsafe_code, unused_crate_dependencies)]
#![warn(
    deprecated_in_future,
    missing_copy_implementations,
    missing_debug_implementations,
    // missing_docs,
    // unreachable_pub,
    unused_import_braces,
    unused_labels,
    unused_lifetimes,
    unused_qualifications,
    future_incompatible,
    nonstandard_style,
)]

mod application;
mod broker;
mod config;
mod error;
pub mod info;
mod message;
mod middleware;
mod task;

pub use application::{IronworkerApplication, IronworkerApplicationBuilder};
pub use broker::{Broker, BrokerConfig, HeartbeatStrategy, InProcessBroker, RetryStrategy};
pub use error::{IronworkerError, IronworkerResult};
pub use message::{Message, SerializableMessage};
pub use middleware::IronworkerMiddleware;
pub use task::{ConfigurableTask, IntoTask, PerformableTask, Task};

#[cfg(test)]
pub(crate) mod test {
    use chrono::{SubsecRound, Utc};
    use snafu::prelude::*;

    use crate::{message::SerializableError, IntoTask, Message, SerializableMessage, Task};

    #[derive(Snafu, Debug)]
    pub(crate) enum TestEnum {
        #[snafu(display("The task failed"))]
        Failed,
    }

    pub(crate) fn boxed_task<T: Task>(t: T) -> Box<dyn Task> {
        Box::new(t)
    }

    pub(crate) async fn successful(_message: Message<u32>) -> Result<(), TestEnum> {
        Ok(())
    }

    pub(crate) async fn failed(_message: Message<u32>) -> Result<(), TestEnum> {
        Err(TestEnum::Failed)
    }

    pub(crate) fn message(task: Box<dyn Task>) -> SerializableMessage {
        SerializableMessage {
            enqueued_at: Utc::now().trunc_subsecs(0), // Force the resolution to be lower for testing so equality checks will "work"
            queue: "default".to_string(),
            job_id: "test-id".to_string(),
            task: task.name().to_string(),
            payload: 123.into(),
            err: None,
            retries: 0,
            delivery_tag: None,
        }
    }

    pub(crate) fn enqueued_successful_message() -> SerializableMessage {
        message(boxed_task(successful.task()))
    }

    pub(crate) fn successful_message() -> SerializableMessage {
        message(boxed_task(successful.task()))
    }

    pub(crate) fn failed_message() -> SerializableMessage {
        let mut message = message(boxed_task(failed.task()));
        message
            .err
            .replace(SerializableError::new(Box::new(TestEnum::Failed) as Box<_>));
        message
    }
}
