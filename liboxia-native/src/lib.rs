pub mod client;
pub mod client_builder;
pub mod client_options;
pub mod errors;
mod shard_manager;
mod batch;
mod operations;
mod provider_manager;
mod write_stream_manager;
mod write_stream;
mod batch_manager;
mod address;
mod session_manager;
mod status;
mod key;

#[allow(clippy::derive_partial_eq_without_eq)]
pub mod oxia {
    include!(concat!(env!("OUT_DIR"), "/io.streamnative.oxia.proto.rs"));
}
