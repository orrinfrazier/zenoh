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

/// Deadline propagation example.
///
/// Demonstrates how deadline budgets flow from client to server:
///
/// 1. A server method inspects the remaining deadline budget via
///    `DeadlineContext::from_query` and includes it in the response.
/// 2. A client calls the method with a generous timeout (5 s) and the
///    server reports the remaining budget.
/// 3. A client calls the method with a very short timeout (10 ms). The
///    server's automatic deadline check rejects the request before the
///    handler runs, returning `ServiceError::DeadlineExceeded`.
///
/// Usage:
///   cargo run --example rpc_deadline -p zenoh-rpc
use std::time::Duration;

use zenoh_ext::z_serialize;
use zenoh_rpc::{DeadlineContext, ServiceClient, ServiceError, ServiceServer};

const SERVICE_KEY: &str = "demo/deadline";

#[tokio::main]
async fn main() {
    zenoh::init_log_from_env_or("error");

    println!("Opening session...");
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    // --- Server: "slow_work" reports remaining budget ---
    println!("Starting RPC server on '{SERVICE_KEY}'...");
    let _server = ServiceServer::builder(&session, SERVICE_KEY)
        .method_fn("slow_work", |query| {
            Box::pin(async move {
                // Extract deadline context from the query attachment.
                // The server framework already checks for expiration before
                // calling the handler, but we can also inspect the budget
                // for logging or sub-task scheduling.
                let remaining = match DeadlineContext::from_query(query) {
                    Some(ctx) => {
                        let r = ctx.remaining();
                        println!("  [server] remaining budget: {r:?}");
                        r
                    }
                    None => {
                        println!("  [server] no deadline attached");
                        Duration::ZERO
                    }
                };

                // Simulate work that takes 50 ms
                tokio::time::sleep(Duration::from_millis(50)).await;

                // Return the remaining budget (in ms) as the response so the
                // client can see what the server observed.
                let remaining_ms = remaining.as_millis() as u64;
                Ok(z_serialize(&remaining_ms))
            })
        })
        .await
        .unwrap();

    let client = ServiceClient::new(&session, SERVICE_KEY, Duration::from_secs(5))
        .expect("failed to create ServiceClient");

    // --- Call 1: generous timeout (5 s default) ---
    println!("\n--- call(\"slow_work\") with 5 s timeout ---");
    match client
        .call::<String, u64>("slow_work", &String::new(), None)
        .await
    {
        Ok(remaining_ms) => {
            println!("  Server reported {remaining_ms} ms remaining budget");
        }
        Err(e) => eprintln!("  Error: {e}"),
    }

    // --- Call 2: explicit short timeout (10 ms) ---
    // The client sends a deadline that is only 10 ms from now. By the time
    // the query reaches the server, the deadline is likely already expired.
    // The server framework rejects it with DeadlineExceeded before the
    // handler runs.
    println!("\n--- call(\"slow_work\") with 10 ms timeout (expect DeadlineExceeded) ---");
    // Small sleep to ensure the query has time to expire in transit
    tokio::time::sleep(Duration::from_millis(20)).await;
    match client
        .call::<String, u64>("slow_work", &String::new(), Some(Duration::from_millis(10)))
        .await
    {
        Ok(remaining_ms) => {
            println!("  Unexpected success: server reported {remaining_ms} ms remaining");
        }
        Err(ServiceError::DeadlineExceeded { budget_ms }) => {
            println!("  DeadlineExceeded (expected): budget was {budget_ms} ms");
        }
        Err(e) => {
            // On a fast local machine the query might arrive before expiry,
            // or the zenoh timeout might fire first. Both are valid outcomes.
            eprintln!("  Other error (acceptable on fast machines): {e}");
        }
    }

    // --- Call 3: demonstrate reading the budget in the handler ---
    println!("\n--- call(\"slow_work\") with 2 s explicit timeout ---");
    match client
        .call::<String, u64>("slow_work", &String::new(), Some(Duration::from_secs(2)))
        .await
    {
        Ok(remaining_ms) => {
            println!("  Server reported {remaining_ms} ms remaining budget");
            println!("  (expected close to 2000 ms, minus transit time)");
        }
        Err(e) => eprintln!("  Error: {e}"),
    }

    println!("\nDone.");
}
