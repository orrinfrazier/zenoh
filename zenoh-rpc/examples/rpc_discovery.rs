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

/// Service discovery example.
///
/// Demonstrates how to discover running `ServiceServer` instances using the
/// liveliness-based discovery built into `ServiceClient`. The example:
///
/// 1. Checks availability before any server is started (expects 0 instances).
/// 2. Starts a server, then re-checks (expects 1 instance).
/// 3. Starts a second server on the same key expression (expects 2 instances).
/// 4. Drops a server and verifies the count decreases.
///
/// All servers and clients share a single zenoh session in this example for
/// simplicity. In production, servers typically run in separate processes.
///
/// Usage:
///   cargo run --example rpc_discovery -p zenoh-rpc
use std::time::Duration;

use zenoh::bytes::ZBytes;
use zenoh_rpc::{ServiceClient, ServiceServer};

const SERVICE_KEY: &str = "demo/discovery";

#[tokio::main]
async fn main() {
    zenoh::init_log_from_env_or("error");

    println!("Opening session...");
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let client = ServiceClient::new(&session, SERVICE_KEY, Duration::from_secs(5))
        .expect("failed to create ServiceClient");

    // --- Step 1: No servers running ---
    println!("\n--- Before starting any server ---");
    let available = client.is_available().await.expect("is_available failed");
    let count = client.instance_count().await.expect("instance_count failed");
    println!("  is_available: {available}");
    println!("  instance_count: {count}");

    // --- Step 2: Start one server ---
    println!("\n--- Starting server #1 ---");
    let server1 = ServiceServer::builder(&session, SERVICE_KEY)
        .method_fn("ping", |_query| {
            Box::pin(async { Ok(ZBytes::from(b"pong".as_ref())) })
        })
        .await
        .expect("failed to start server 1");

    // Small delay so liveliness propagates
    tokio::time::sleep(Duration::from_millis(100)).await;

    let available = client.is_available().await.expect("is_available failed");
    let count = client.instance_count().await.expect("instance_count failed");
    println!("  is_available: {available}");
    println!("  instance_count: {count}");

    // --- Step 3: Start a second server (same key expression, different session) ---
    println!("\n--- Starting server #2 (new session) ---");
    let session2 = zenoh::open(zenoh::Config::default()).await.unwrap();
    let server2 = ServiceServer::builder(&session2, SERVICE_KEY)
        .method_fn("ping", |_query| {
            Box::pin(async { Ok(ZBytes::from(b"pong".as_ref())) })
        })
        .await
        .expect("failed to start server 2");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let count = client.instance_count().await.expect("instance_count failed");
    println!("  instance_count: {count}");

    // --- Step 4: Drop server #1, count should decrease ---
    println!("\n--- Dropping server #1 ---");
    drop(server1);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let count = client.instance_count().await.expect("instance_count failed");
    println!("  instance_count: {count}");

    // --- Cleanup ---
    println!("\n--- Dropping server #2 ---");
    drop(server2);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let available = client.is_available().await.expect("is_available failed");
    println!("  is_available: {available}");

    println!("\nDone.");
}
