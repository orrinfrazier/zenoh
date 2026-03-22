//
// Copyright (c) 2023 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//

//! # zenoh-rpc
//!
//! Typed RPC service layer for [Zenoh](https://zenoh.io).
//!
//! This crate provides typed request-response service definitions layered on top
//! of Zenoh's query/reply primitive. Services are Rust traits with typed request
//! and response types, deadlines, and structured errors.
//!
//! ## Features
//!
//! - **Service trait pattern**: Define services as Rust traits with typed methods
//! - **Deadline propagation**: Automatic timeout budget tracking across calls
//! - **Structured errors**: Typed error variants with fast-path status codes
//! - **Service discovery**: Automatic instance discovery via liveliness tokens
//!
//! ## Feature flags
//!
//! - `default`: Core RPC functionality
//! - `unstable`: Gated experimental APIs

#[cfg(test)]
mod tests {
    #[test]
    fn crate_compiles() {
        // Verify the crate links and compiles successfully.
        // Actual functionality tests live in integration tests.
    }
}
