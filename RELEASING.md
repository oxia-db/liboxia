# Release process

`oxia-client` is published to [crates.io](https://crates.io/crates/oxia-client) and announced as
[GitHub Releases](https://github.com/oxia-db/oxia-client-rust/releases).

**Release notes live in GitHub Releases** — this repository intentionally keeps no in-repo
`CHANGELOG.md`.

## Steps

1. **Bump the version.** Update `version` in `oxia-client/Cargo.toml` and
   `oxia-client-ffi/Cargo.toml` (keep the two crates in lockstep) and refresh `Cargo.lock`
   (`cargo build`). Open a PR and merge it to `main`.

2. **Tag the release** on the merge commit and push the tag:

   ```shell
   git tag vX.Y.Z
   git push origin vX.Y.Z
   ```

   Pushing a `v*` tag triggers the
   [`release-cargo.yml`](./.github/workflows/release-cargo.yml) workflow, which runs the full
   test suite and then `cargo publish`es `oxia-client`.

3. **Publish the GitHub Release.** Draft a release for the tag (GitHub can auto-generate notes
   from the merged PRs) and curate it into highlights, notable changes, and any migration notes,
   then publish it.

## Versioning

The crate follows [Semantic Versioning](https://semver.org). CI runs
[`cargo-semver-checks`](https://github.com/obi1kenobi/cargo-semver-checks) against the latest
published release to catch accidental breaking changes; the check self-arms once the first
version is on crates.io.

## crates.io token

The publish step authenticates with the `CRATE_TOKEN` repository secret. It must be a crates.io
API token with the **publish-new** scope (required to publish a crate name for the first time)
and **publish-update** scope. If the token is crate-scoped, it must include `oxia-client`. A token
lacking these scopes fails the publish with `403 Forbidden`.
