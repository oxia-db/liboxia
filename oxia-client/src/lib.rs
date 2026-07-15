//! Rust client SDK for [Oxia](https://github.com/oxia-db/oxia), a scalable
//! metadata store and coordination system.
//!
//! The entry point is [`OxiaClient`]. Every operation returns a request builder:
//! chain options onto it and `.await` it directly.
//!
//! ```no_run
//! use oxia::{ComparisonType, OxiaClient};
//!
//! # async fn example() -> Result<(), oxia::OxiaError> {
//! let client = OxiaClient::connect("localhost:6648").await?;
//!
//! // Plain put/get.
//! let res = client.put("greeting", "hello").await?;
//! let rec = client.get("greeting").await?;
//! assert_eq!(rec.value.as_deref(), Some(b"hello".as_ref()));
//!
//! // Options chain onto the builder.
//! client
//!     .put("config/a", "1")
//!     .expected_version_id(res.version.version_id)
//!     .await?;
//! client.put("locks/w1", "").ephemeral().await?;
//! let floor = client.get("config/z").comparison(ComparisonType::Floor).await?;
//!
//! // Range operations, as a whole result or as an ordered stream.
//! let keys = client.list("config/", "config/~").await?;
//! let mut scan = client.range_scan("config/", "config/~").stream().await?;
//! # let _ = (floor, keys, scan);
//! client.close().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Concepts
//!
//! - **Records** are key/value pairs with a [`Version`] carrying the record's
//!   version id and timestamps; conditional operations
//!   ([`expected_version_id`](PutBuilder::expected_version_id)) implement
//!   compare-and-swap.
//! - **Ephemeral records** ([`PutBuilder::ephemeral`]) live as long as the
//!   client's session and are removed automatically when it closes or expires.
//! - **Partition keys** ([`PutBuilder::partition_key`]) co-locate related
//!   records on one shard, enabling atomic multi-record batches and
//!   server-generated **sequential keys**
//!   ([`PutBuilder::sequence_key_deltas`], [`OxiaClient::sequence_updates`]).
//! - **Secondary indexes** ([`PutBuilder::secondary_index`]) provide alternate
//!   query paths for gets, lists and scans
//!   ([`GetBuilder::use_index`], [`ListBuilder::use_index`]).
//! - **Notifications** ([`OxiaClient::notifications`]) stream every change
//!   applied to the database.
//!
//! Keys are sorted with Oxia's slash-aware order (`/`-separated path segments),
//! not plain lexicographic order; range boundaries follow it.
//!
//! # Error handling
//!
//! Every operation returns [`OxiaError`]. Semantic outcomes callers commonly
//! match on are dedicated variants ([`OxiaError::KeyNotFound`],
//! [`OxiaError::UnexpectedVersionId`], …); transient infrastructure failures
//! answer `true` to [`OxiaError::is_retryable`] and are safe to retry —
//! idempotent operations unconditionally, non-idempotent ones (puts without a
//! version condition) with application-level judgment. Each operation on
//! [`OxiaClient`] documents the errors it can produce.
//!
//! # Cancellation
//!
//! Dropping an operation's future stops waiting but does not recall an
//! operation already submitted to a batch; it may still execute on the
//! server. The `recv` methods on [`Notifications`] and [`SequenceUpdates`]
//! are cancel-safe.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![allow(clippy::result_large_err)]

mod address;
mod batcher;
mod client;
mod client_builder;
mod client_options;
mod errors;
mod hash;
mod key;
mod notification_manager;
mod operations;
mod provider_manager;
mod requests;
mod retry;
mod sequence_updates_manager;
mod session_manager;
mod shard_manager;
mod status;
mod streams;
mod types;

#[allow(
    clippy::derive_partial_eq_without_eq,
    clippy::enum_variant_names,
    missing_docs
)]
pub(crate) mod proto {
    include!(concat!(env!("OUT_DIR"), "/io.oxia.proto.v1.rs"));
}

pub use bytes::Bytes;

pub use client::OxiaClient;
pub use client_builder::OxiaClientBuilder;
pub use errors::OxiaError;
pub use requests::{
    DeleteBuilder, DeleteRangeBuilder, GetBuilder, ListBuilder, NotificationsBuilder, PutBuilder,
    RangeScanBuilder, SequenceUpdatesBuilder,
};
pub use streams::{ListStream, Notifications, RangeScanStream, SequenceUpdates};
pub use types::{ComparisonType, GetResult, Notification, PutResult, Version};
