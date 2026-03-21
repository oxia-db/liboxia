#![allow(clippy::result_large_err)]
mod address;
mod batch;
mod batch_manager;
pub mod client;
pub mod client_builder;
pub mod client_options;
pub mod errors;
mod key;
mod notification_manager;
mod operations;
mod provider_manager;
mod sequence_updates_manager;
mod session_manager;
mod shard_manager;
mod status;
mod write_stream;
mod write_stream_manager;

#[allow(clippy::derive_partial_eq_without_eq)]
pub mod oxia {
    include!(concat!(env!("OUT_DIR"), "/io.streamnative.oxia.proto.rs"));
}

// Convenience re-exports for common types
pub use client::OxiaClient;
pub use client_builder::OxiaClientBuilder;
pub use errors::OxiaError;
