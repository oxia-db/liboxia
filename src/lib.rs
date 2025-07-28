mod client;
pub mod client_builder;
mod client_options;
mod errors;
mod shard_manager;
#[allow(clippy::derive_partial_eq_without_eq)]
pub mod oxia {
    include!(concat!(env!("OUT_DIR"), "/io.streamnative.oxia.proto.rs"));
}
