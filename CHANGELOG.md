# Changelog

All notable changes to the Oxia Rust client are documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the crate
adheres to [Semantic Versioning](https://semver.org).

## [0.1.0] — 2026-07-16

First release under the `oxia-client` name (previously published as
[`liboxia`](https://crates.io/crates/liboxia), now deprecated). The public API
was redesigned in this release and is the surface we intend to stabilize
towards 1.0.

### Migrating from `liboxia` 0.0.2

```toml
[dependencies]
# old: liboxia = "0.0.2"
oxia-client = "0.1.0"
```

The import root is unchanged (`use oxia::…`). The API moved from
`*_with_options(…, vec![Option])` pairs to one fluent request builder per
operation — chain options and `.await` directly:

| liboxia 0.0.2 | oxia-client 0.1.0 |
|---|---|
| `client.put(k, v).await` | `client.put(k, v).await` (unchanged; params now `impl Into<String>` / `impl Into<Bytes>`) |
| `client.put_with_options(k, v, vec![PutOption::Ephemeral()])` | `client.put(k, v).ephemeral().await` |
| `client.put_with_options(k, v, vec![PutOption::ExpectVersionId(n)])` | `client.put(k, v).expected_version_id(n).await` |
| `client.get_with_options(k, vec![GetOption::ComparisonType(Floor)])` | `client.get(k).comparison(ComparisonType::Floor).await` |
| `client.list(min, max).await?.keys` | `client.list(min, max).await?` (returns `Vec<String>`) |
| `client.range_scan(min, max).await?.records` | `client.range_scan(min, max).await?` (returns `Vec<GetResult>`) — or `.stream().await?` |
| `client.get_notifications()` | `client.notifications().await?` (returns a `Notifications` handle) |
| `client.get_sequence_updates_with_options(k, vec![PartitionKey(pk)])` | `client.sequence_updates(k, pk).await?` (partition key is a required argument) |
| `client.shutdown()` (consumed the client) | `client.close()` (idempotent, works from any clone, flushes pending batches) |
| `GetResult.value: Option<Vec<u8>>` | `GetResult.value: Option<Bytes>` |
| `Notification::KeyCreated(KeyCreated { .. })` | `Notification::KeyCreated { .. }` (struct variants) |
| `oxia::client::…` / `oxia::errors::…` module paths | everything re-exported at the crate root (`oxia::…`) |

### Added

- Fluent request builders implementing `IntoFuture` for every operation.
- Ordered streaming for `list` / `range_scan` (`.stream()`): per-shard results
  merged in the server's global key order with O(shards) memory.
- `GetResult.secondary_index_key`, and `include_internal_keys` on list/scan.
- Graceful `close(&self)`: flushes pending batches, closes sessions (removing
  ephemeral records), idempotent across clones.
- Typed, `#[non_exhaustive]` error enum with `OxiaError::is_retryable()`.
- Connection robustness: fail-fast client construction, connect timeout,
  HTTP/2 keep-alive, per-request deadlines, unbounded cancellation-aware
  retries for all background streams, transparent session re-creation after
  expiry, atomic shard-assignment snapshots with O(log n) routing.
- Go/Java interoperability guarantees, enforced by cross-client tests in CI:
  XXH3-32 shard routing and slash-aware key ordering identical to the server.

### Changed

- **Breaking:** the entire options API, result types, module layout, and
  shutdown flow (see the migration table above).
- Record values are `bytes::Bytes` end-to-end: puts move the value to the wire
  without copying; get/scan values are zero-copy slices of the receive buffer.
- The client speaks the `io.oxia.proto.v1` gRPC service and requires a server
  that provides it.
- Logging goes through `tracing` (was `log`).
- Edition 2024; MSRV 1.85.

[0.1.0]: https://github.com/oxia-db/oxia-client-rust/releases/tag/v0.1.0
