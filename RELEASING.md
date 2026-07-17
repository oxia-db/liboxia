# Release process

`oxia-client` is released to [crates.io](https://crates.io/crates/oxia-client) and announced as
[GitHub Releases](https://github.com/oxia-db/oxia-client-rust/releases). Releases are automated
with [release-plz](https://release-plz.dev).

**Release notes live in GitHub Releases** — this repository intentionally keeps no in-repo
`CHANGELOG.md`. release-plz generates the release notes from the commit history, so land changes
with [Conventional Commits](https://www.conventionalcommits.org) messages (`feat:` for a minor
bump, `fix:` for a patch, `feat!:` / `BREAKING CHANGE:` for a major).

## How it works

The [`Release-plz`](./.github/workflows/release-plz.yml) workflow runs on every push to `main`:

1. **Release PR.** release-plz keeps a PR open that bumps `oxia-client`'s version according to the
   conventional commits merged since the last release. Review it like any other PR.
2. **Publish.** Merging the release PR makes the workflow `cargo publish` `oxia-client`, push a
   `vX.Y.Z` tag, and create a GitHub Release with generated notes.
3. **(Optional) Curate.** Edit the generated GitHub Release to add highlights or migration notes.

Only `oxia-client` is released. The `oxia-client-ffi` shim and the `oxia-perf` tool are internal
(`publish = false`, and `release = false` in [`release-plz.toml`](./release-plz.toml)), so
release-plz never publishes, tags, or creates a GitHub Release for them — though it may bump the
FFI's version to keep it in step with `oxia-client`.

## Required setup (one-time)

- **`CRATE_TOKEN` repository secret** — a crates.io API token with the **publish-update** scope
  (plus **publish-new** for a brand-new crate name), scoped to include `oxia-client`. It is passed
  to release-plz as `CARGO_REGISTRY_TOKEN`; a token lacking these scopes fails publishing with
  `403 Forbidden`.
- **Repository setting** — under *Settings → Actions → General → Workflow permissions*, enable
  *"Allow GitHub Actions to create and approve pull requests"* so the release PR can be opened.
- The workflow uses the built-in `GITHUB_TOKEN`. Because PRs opened by `GITHUB_TOKEN` do not
  trigger other workflows, the release PR will not run CI on its own; to get CI on it, supply a
  [Personal Access Token or GitHub App token](https://release-plz.dev/docs/github/token) as the
  `release-pr` job's `GITHUB_TOKEN`.

## Versioning

The crate follows [Semantic Versioning](https://semver.org). `ci.yml` runs
[`cargo-semver-checks`](https://github.com/obi1kenobi/cargo-semver-checks) against the latest
published release to catch accidental breaking changes; release-plz's own semver check is disabled
in `release-plz.toml` to avoid duplicating it.
