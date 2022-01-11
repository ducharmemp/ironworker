#![forbid(unsafe_code)]
#![deny(clippy::all, clippy::cargo, nonstandard_style, rust_2018_idioms)]
#![forbid(non_ascii_idents, unsafe_code, unused_crate_dependencies)]
#![warn(
    deprecated_in_future,
    missing_copy_implementations,
    // missing_debug_implementations,
    // missing_docs,
    // unreachable_pub,
    unused_import_braces,
    unused_labels,
    unused_lifetimes,
    unused_qualifications,
    // unused_results,
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
pub use task::{ConfigurableTask, ErrorRetryConfiguration, IntoTask, PerformableTask, Task};
