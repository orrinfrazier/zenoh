//
// Copyright (c) 2024 ZettaScale Technology
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

/// RPC server example.
///
/// Demonstrates how to create a multi-method RPC server using `ServiceServer`.
/// The server registers two methods:
///
/// - **"greet"** — accepts a name (String) and returns a greeting.
/// - **"add"** — accepts a pair of i64 values and returns their sum.
///
/// The server also validates inputs, returning `ServiceError::InvalidRequest`
/// when the name is empty. Run this alongside `rpc_client` to see the full
/// request-response cycle.
///
/// Usage:
///   cargo run --example rpc_server -p zenoh-rpc
use zenoh_ext::{z_deserialize, z_serialize};
use zenoh_rpc::{ServiceError, ServiceServer};

const SERVICE_KEY: &str = "demo/rpc";

#[tokio::main]
async fn main() {
    zenoh::init_log_from_env_or("error");

    println!("Opening session...");
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    println!("Starting RPC server on '{SERVICE_KEY}'...");
    let _server = ServiceServer::builder(&session, SERVICE_KEY)
        // "greet" method: takes a name, returns a greeting string.
        .method_fn("greet", |query| {
            Box::pin(async move {
                // Deserialize the name from the query payload
                let payload = query
                    .payload()
                    .ok_or_else(|| ServiceError::InvalidRequest {
                        message: "missing payload".to_string(),
                    })?;
                let name: String =
                    z_deserialize(payload).map_err(|_| ServiceError::InvalidRequest {
                        message: "payload is not a valid string".to_string(),
                    })?;

                // Validate input — demonstrate returning an error
                if name.is_empty() {
                    return Err(ServiceError::InvalidRequest {
                        message: "name must not be empty".to_string(),
                    });
                }

                let greeting = format!("Hello, {name}! Welcome to zenoh-rpc.");
                println!("  greet({name:?}) -> {greeting:?}");
                Ok(z_serialize(&greeting))
            })
        })
        // "add" method: takes (i64, i64), returns i64.
        .method_fn("add", |query| {
            Box::pin(async move {
                let payload = query
                    .payload()
                    .ok_or_else(|| ServiceError::InvalidRequest {
                        message: "missing payload".to_string(),
                    })?;
                let (a, b): (i64, i64) =
                    z_deserialize(payload).map_err(|_| ServiceError::InvalidRequest {
                        message: "payload is not a valid (i64, i64) tuple".to_string(),
                    })?;

                let result = a + b;
                println!("  add({a}, {b}) -> {result}");
                Ok(z_serialize(&result))
            })
        })
        .await
        .unwrap();

    println!("RPC server running. Press CTRL-C to quit.");

    // Keep the server alive until interrupted.
    // Use a pending future — the process terminates via SIGINT.
    std::future::pending::<()>().await;
}
