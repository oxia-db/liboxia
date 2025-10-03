# Liboxia

Liboxia is a Rust library designed for both native Rust applications and for integration with other languages via its C
Foreign Function Interface (FFI).
It serves as a client SDK for [Oxia](https://github.com/oxia-db/oxia), a distributed key-value store, enabling robust,
asynchronous data operations.

## Modules

- **liboxia-native**: This is the core, high-performance Rust library of the client. Built on the Tokio asynchronous
  runtime, it offers an idiomatic and comprehensive API for managing data in Oxia.
- **liboxia-ffi**: This module provides a C Foreign Function Interface for liboxia-native, allowing its functionality to
  be exposed to C/C++ applications. The FFI layer manages complex data conversions and memory handling, ensuring a safe
  and efficient bridge between the Rust and C codebases.

## Contributing

We welcome contributions from the community! To contribute to this project, please:

1. Fork the repository and clone it locally.
2. Create a new branch for your feature or bug fix.
3. Submit a Pull Request (PR) with a clear description of your changes.

All contributions submitted to this project will be licensed under the Apache License 2.0.

## License

This project is licensed under the Apache License 2.0. A copy of the license is included in the repository.