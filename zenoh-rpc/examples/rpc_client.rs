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

/// RPC client example.
///
/// Demonstrates how to call methods on a remote `ServiceServer` using
/// `ServiceClient`. Shows both typed calls (via `call`) and raw/untyped
/// calls (via `call_raw`), plus error handling for:
///
/// - Invalid input (empty name)
/// - Non-existent methods
///
/// Start `rpc_server` first, then run this example.
///
/// Usage:
///   cargo run --example rpc_client -p zenoh-rpc
use std::time::Duration;

use zenoh_ext::{z_deserialize, z_serialize};
use zenoh_rpc::{ServiceClient, ServiceError};

const SERVICE_KEY: &str = "demo/rpc";
const TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() {
    zenoh::init_log_from_env_or("error");

    println!("Opening session...");
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let client = ServiceClient::new(&session, SERVICE_KEY, TIMEOUT)
        .expect("failed to create ServiceClient");

    // --- Typed call: greet ---
    println!("\n--- call(\"greet\", \"Alice\") ---");
    match client
        .call::<String, String>("greet", &"Alice".to_string(), None)
        .await
    {
        Ok(greeting) => println!("  Response: {greeting}"),
        Err(e) => eprintln!("  Error: {e}"),
    }

    // --- Typed call: add ---
    println!("\n--- call(\"add\", (17, 25)) ---");
    match client
        .call::<(i64, i64), i64>("add", &(17, 25), None)
        .await
    {
        Ok(sum) => println!("  Response: {sum}"),
        Err(e) => eprintln!("  Error: {e}"),
    }

    // --- Raw/untyped call: add ---
    // Use call_raw when you want manual control over serialization.
    println!("\n--- call_raw(\"add\", serialize((100, 200))) ---");
    let raw_payload = z_serialize(&(100_i64, 200_i64));
    match client.call_raw("add", raw_payload, None).await {
        Ok(resp) => {
            let sum: i64 = z_deserialize(&resp).expect("deserialization failed");
            println!("  Response (raw): {sum}");
        }
        Err(e) => eprintln!("  Error: {e}"),
    }

    // --- Error case: empty name ---
    println!("\n--- call(\"greet\", \"\") — expects InvalidRequest ---");
    match client
        .call::<String, String>("greet", &String::new(), None)
        .await
    {
        Ok(resp) => println!("  Unexpected success: {resp}"),
        Err(ServiceError::InvalidRequest { message }) => {
            println!("  InvalidRequest (expected): {message}");
        }
        Err(e) => eprintln!("  Unexpected error variant: {e}"),
    }

    // --- Error case: non-existent method ---
    println!("\n--- call(\"multiply\", (2, 3)) — expects MethodNotFound ---");
    match client
        .call::<(i64, i64), i64>("multiply", &(2, 3), None)
        .await
    {
        Ok(resp) => println!("  Unexpected success: {resp}"),
        Err(ServiceError::MethodNotFound { method }) => {
            println!("  MethodNotFound (expected): {method}");
        }
        Err(e) => eprintln!("  Unexpected error variant: {e}"),
    }

    println!("\nDone.");
}
