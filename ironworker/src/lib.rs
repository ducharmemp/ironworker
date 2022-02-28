//! Async Job Queues for Rust Applications
//!
//! This crate provides the core Ironworker application, the main driver behind enqueueing and running jobs in an asynchronous manner. This
//! library is completely backend agnostic, and instead delegates the communication strategies to auxiliary crates. Ironworker is also focused
//! on tight integration with existing web servers to provide seamless integration and ease of use.
//!
//! # Overview
//!
//! At its heart, an Ironworker application acts on messages provided by Producers, through a backing datastore, and onto Consumers. This approach
//! to structuring applications has a long history, but at a high level this structure means that work can be offloaded from user-facing servers
//! onto dedicated machines, providing scalability, reducing coupling, and greater durability of business level operations.
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
    noop_method_call,
    clippy::unwrap_used,
    clippy::expect_used
)]
#![forbid(non_ascii_idents, unsafe_code, unused_crate_dependencies)]
#![warn(
    deprecated_in_future,
    missing_copy_implementations,
    missing_debug_implementations,
    // missing_docs,
    unused_import_braces,
    unused_labels,
    unused_lifetimes,
    unused_qualifications,
    future_incompatible,
    nonstandard_style,
)]
#![allow(clippy::multiple_crate_versions)]

pub use ironworker_core::*;

mod extras;

#[cfg(feature = "redis")]
pub mod redis {
    pub use ironworker_redis::*;
}
#[cfg(feature = "sqs")]
pub mod sqs {
    pub use ironworker_sqs::*;
}
#[cfg(feature = "axum-integration")]
pub mod axum {
    pub use crate::extras::axum_integration::*;
}

pub mod application;
pub mod enqueue;
pub mod extract;
pub mod process;

#[cfg(test)]
pub(crate) mod test {
    use async_trait::async_trait;
    use chrono::{DateTime, SubsecRound, Utc};
    use ironworker_core::broker::Broker;
    use ironworker_core::error::PerformLaterSnafu;
    use ironworker_core::message::{Message, SerializableError, SerializableMessage};
    use ironworker_core::middleware::IronworkerMiddleware;
    use ironworker_core::task::{IntoTask, PerformableTask, Task};
    use mockall::predicate::*;
    use mockall::*;
    use serde::Serialize;
    use snafu::{prelude::*, IntoError};
    use uuid::Uuid;

    pub(crate) fn assert_send<T: Send>() {}
    pub(crate) fn assert_sync<T: Sync>() {}

    #[derive(Snafu, Debug)]
    pub(crate) enum TestEnum {
        #[snafu(display("The task failed"))]
        Failed,
    }

    pub(crate) fn boxed_task<T: Serialize + Send + 'static>(
        t: impl Task<T>,
    ) -> Box<dyn PerformableTask> {
        Box::new(t.into_performable_task())
    }

    pub(crate) async fn successful(_message: Message<u32>) -> Result<(), TestEnum> {
        Ok(())
    }

    pub(crate) async fn failed(_message: Message<u32>) -> Result<(), TestEnum> {
        Err(TestEnum::Failed)
    }

    pub(crate) fn message(name: &str, _task: Box<dyn PerformableTask>) -> SerializableMessage {
        SerializableMessage {
            enqueued_at: None,
            created_at: Utc::now().trunc_subsecs(0), // Force the resolution to be lower for testing so equality checks will "work"
            queue: "default".to_string(),
            job_id: Uuid::parse_str("8aa34936-1f60-404f-8cbb-973123e6744e").unwrap(), // Always have the same ID for jobs when testing the crate
            task: name.to_string(),
            payload: 123.into(),
            at: None,
            err: None,
            retries: 0,
            delivery_tag: None,
            message_state: Default::default(),
        }
    }

    pub(crate) fn enqueued_successful_message() -> SerializableMessage {
        message(
            "&ironworker::test::successful",
            boxed_task(successful.task()),
        )
    }

    pub(crate) fn successful_message() -> SerializableMessage {
        message(
            "&ironworker::test::successful",
            boxed_task(successful.task()),
        )
    }

    pub(crate) fn failed_message() -> SerializableMessage {
        let mut message = message("&ironworker::test::failed", boxed_task(failed.task()));
        message.err.replace(SerializableError::new(
            PerformLaterSnafu.into_error(Box::new(TestEnum::Failed)),
        ));
        message
    }

    mock! {
        pub TestBroker {}

        #[async_trait]
        impl Broker for TestBroker {
            type Error = ();

            async fn register_worker(&self, _worker_id: &str, _queue: &str) -> Result<(), <Self as Broker>::Error>;

            async fn enqueue(
                &self,
                queue: &str,
                message: SerializableMessage,
                _at: Option<DateTime<Utc>>,
            ) -> Result<(), <Self as Broker>::Error>;

            async fn deadletter(
                &self,
                _queue: &str,
                _message: SerializableMessage,
            ) -> Result<(), <Self as Broker>::Error>;

            async fn dequeue(&self, from: &str) -> Result<Option<SerializableMessage>, <Self as Broker>::Error>;

            async fn heartbeat(&self, _worker_id: &str) -> Result<(), <Self as Broker>::Error>;

            async fn deregister_worker(&self, _worker_id: &str) -> Result<(), <Self as Broker>::Error>;

            async fn acknowledge_processed(
                &self,
                _from: &str,
                _message: SerializableMessage,
            ) -> Result<(), <Self as Broker>::Error>;
        }
    }

    mock! {
        pub TestMiddleware {}

        #[async_trait]
        impl IronworkerMiddleware for TestMiddleware {
            async fn before_enqueue(&self, _message: &SerializableMessage);
            async fn after_enqueue(&self, _message: &SerializableMessage);
            async fn before_perform(&self, _message: &mut SerializableMessage);
            async fn after_perform(&self, _message: &SerializableMessage);
        }
    }
}
