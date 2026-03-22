<p align="center">
    <a href="http://zenoh.io">
        <img height="100" src="https://raw.githubusercontent.com/eclipse-zenoh/zenoh/main/zenoh-dragon.png">
    </a>
</p>

# zenoh-rpc

Typed RPC service layer for [Zenoh](https://zenoh.io).

[![License](https://img.shields.io/badge/License-EPL%202.0%20%2F%20Apache%202.0-blue.svg)](https://choosealicense.com/licenses/epl-2.0/)
[![Rust](https://img.shields.io/badge/rust-1.75.0%2B-orange.svg)](https://www.rust-lang.org)

## Overview

`zenoh-rpc` provides typed request-response service definitions on top of Zenoh's query/reply primitive. Define services as Rust traits with typed request and response types, deadlines, and structured errors.

## Features

- **Service trait pattern** — Define services as Rust traits with typed methods. No proc macros — users implement the trait manually, helper functions handle the wiring.
- **ServiceServer** — Wraps a trait implementation + TypedQueryable. Dispatches incoming queries to the correct method based on attachments.
- **ServiceClient** — Wraps a TypedQuerier. Provides typed method calls with serialization, deadline propagation, and error handling.
- **Deadline propagation** — Client sets a deadline on the call, transmitted as an attachment. Server checks remaining budget before processing.
- **Structured errors** — `ServiceError` enum with variants (NotFound, InvalidRequest, Internal, DeadlineExceeded, custom).
- **Service discovery** — Automatic instance discovery via Zenoh liveliness tokens.

## Usage

```rust
use async_trait::async_trait;

// Define a service trait
#[async_trait]
trait DeviceConfigService {
    async fn get_config(
        &self,
        req: GetConfigRequest,
    ) -> Result<DeviceConfig, ServiceError>;

    async fn push_firmware(
        &self,
        req: PushFirmwareRequest,
    ) -> Result<FirmwareResult, ServiceError>;
}
```

## Feature flags

| Flag | Description |
|------|-------------|
| `default` | Core RPC functionality |
| `unstable` | Experimental APIs, gated behind the `unstable` feature |

## License

This project is dual-licensed under the [Eclipse Public License 2.0](http://www.eclipse.org/legal/epl-2.0)
and [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0).

SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
