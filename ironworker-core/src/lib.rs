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

pub mod broker;
pub mod enqueuer;
pub mod error;
pub mod from_payload;
pub mod info;
pub mod message;
pub mod middleware;
pub mod task;
