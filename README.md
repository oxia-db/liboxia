# oxia-client-rust


[![Crates.io][crates-badge]][crates-url]

[crates-badge]: https://img.shields.io/crates/v/oxia-client.svg
[crates-url]: https://crates.io/crates/oxia-client

The native Rust client SDK for [Oxia](https://github.com/oxia-db/oxia), a distributed key-value store. Built on the
Tokio asynchronous runtime, it offers an idiomatic and comprehensive API for managing data in Oxia, plus an optional C
Foreign Function Interface (FFI) for integration with other languages.

## Crates

- **oxia-client**: The core, high-performance Rust client library (published to crates.io as `oxia-client`, imported as
  `use oxia::...`). Built on the Tokio asynchronous runtime, it offers an idiomatic and comprehensive API for managing
  data in Oxia.
- **oxia-client-ffi**: A C Foreign Function Interface over `oxia-client`, exposing its functionality to C/C++
  applications. The FFI layer manages data conversions and memory handling to bridge the Rust and C codebases.

## Getting Started

Please check the examples directory for usage examples.

- oxia-client: [oxia-client/examples](./oxia-client/examples)
- oxia-client-ffi: [oxia-client-ffi/examples](./oxia-client-ffi/examples)

## Contributing

We welcome contributions from the community! To contribute to this project, please:

1. Fork the repository and clone it locally.
2. Create a new branch for your feature or bug fix.
3. Submit a Pull Request (PR) with a clear description of your changes.

All contributions submitted to this project will be licensed under the Apache License 2.0.

## License

This project is licensed under the Apache License 2.0. A copy of the license is included in the repository.