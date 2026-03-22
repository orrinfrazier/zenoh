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

#![cfg(feature = "unstable")]

use std::time::Duration;

use zenoh::bytes::ZBytes;
use zenoh::config::{EndPoint, WhatAmI};

use zenoh_ext::{ServiceClient, ServiceServer};

/// Create a peer session with multicast scouting disabled and listening on
/// the given endpoint.
async fn listening_session(endpoint: &str) -> zenoh::Session {
    let mut config = zenoh::Config::default();
    config
        .listen
        .endpoints
        .set(vec![endpoint.parse::<EndPoint>().unwrap()])
        .unwrap();
    config
        .scouting
        .multicast
        .set_enabled(Some(false))
        .unwrap();
    let _ = config.set_mode(Some(WhatAmI::Peer));
    zenoh::open(config).await.unwrap()
}

/// Create a peer session with multicast scouting disabled that connects to
/// the given endpoint.
async fn connecting_session(endpoint: &str) -> zenoh::Session {
    let mut config = zenoh::Config::default();
    config
        .connect
        .endpoints
        .set(vec![endpoint.parse::<EndPoint>().unwrap()])
        .unwrap();
    config
        .scouting
        .multicast
        .set_enabled(Some(false))
        .unwrap();
    let _ = config.set_mode(Some(WhatAmI::Peer));
    zenoh::open(config).await.unwrap()
}

/// Unique key expression generator to avoid cross-test interference.
fn unique_ke(suffix: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("test/rpc/discovery/{id}/{suffix}")
}

// -- Test: server is discoverable after start --

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_is_available_after_start() {
    zenoh_util::init_log_from_env_or("error");

    const ENDPOINT: &str = "tcp/localhost:17501";
    let ke = unique_ke("available");

    let session1 = listening_session(ENDPOINT).await;
    let session2 = connecting_session(ENDPOINT).await;

    // Start the server
    let _server = ServiceServer::builder(&session1, ke.as_str())
        .method_fn("ping", |_query| {
            Box::pin(async { Ok(ZBytes::from(b"pong".as_ref())) })
        })
        .await
        .expect("server should start");

    // Allow liveliness token to propagate
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Client on a different session should see the server
    let client =
        ServiceClient::new(&session2, &ke, Duration::from_secs(5)).expect("client should build");

    let available = client.is_available().await.expect("is_available should work");
    assert!(available, "server should be available");

    session1.close().await.unwrap();
    session2.close().await.unwrap();
}

// -- Test: no server means not available --

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_server_is_not_available() {
    zenoh_util::init_log_from_env_or("error");

    const ENDPOINT: &str = "tcp/localhost:17502";
    let ke = unique_ke("not_available");

    let session1 = listening_session(ENDPOINT).await;
    let session2 = connecting_session(ENDPOINT).await;

    // Allow sessions to establish connectivity
    tokio::time::sleep(Duration::from_secs(1)).await;

    // No server is started — client should see nothing
    let client =
        ServiceClient::new(&session2, &ke, Duration::from_secs(5)).expect("client should build");

    let available = client.is_available().await.expect("is_available should work");
    assert!(!available, "no server should mean not available");

    session1.close().await.unwrap();
    session2.close().await.unwrap();
}

// -- Test: two servers on the same key report instance_count == 2 --

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_servers_report_instance_count_two() {
    zenoh_util::init_log_from_env_or("error");

    const ENDPOINT: &str = "tcp/localhost:17503";
    let ke = unique_ke("two_instances");

    let session1 = listening_session(ENDPOINT).await;
    let session2 = connecting_session(ENDPOINT).await;
    let session3 = connecting_session(ENDPOINT).await;

    // Start two servers on the same key expression but different sessions
    let _server1 = ServiceServer::builder(&session1, ke.as_str())
        .method_fn("ping", |_query| {
            Box::pin(async { Ok(ZBytes::from(b"pong1".as_ref())) })
        })
        .await
        .expect("server1 should start");

    let _server2 = ServiceServer::builder(&session2, ke.as_str())
        .method_fn("ping", |_query| {
            Box::pin(async { Ok(ZBytes::from(b"pong2".as_ref())) })
        })
        .await
        .expect("server2 should start");

    // Allow liveliness tokens to propagate
    tokio::time::sleep(Duration::from_secs(1)).await;

    let client =
        ServiceClient::new(&session3, &ke, Duration::from_secs(5)).expect("client should build");

    let count = client
        .instance_count()
        .await
        .expect("instance_count should work");
    assert_eq!(count, 2, "should see 2 server instances");

    session1.close().await.unwrap();
    session2.close().await.unwrap();
    session3.close().await.unwrap();
}

// -- Test: dropping a server decreases the count --

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_server_decreases_count() {
    zenoh_util::init_log_from_env_or("error");

    const ENDPOINT: &str = "tcp/localhost:17504";
    let ke = unique_ke("drop_count");

    let session1 = listening_session(ENDPOINT).await;
    let session2 = connecting_session(ENDPOINT).await;
    let session3 = connecting_session(ENDPOINT).await;

    let _server1 = ServiceServer::builder(&session1, ke.as_str())
        .method_fn("ping", |_query| {
            Box::pin(async { Ok(ZBytes::from(b"pong1".as_ref())) })
        })
        .await
        .expect("server1 should start");

    let server2 = ServiceServer::builder(&session2, ke.as_str())
        .method_fn("ping", |_query| {
            Box::pin(async { Ok(ZBytes::from(b"pong2".as_ref())) })
        })
        .await
        .expect("server2 should start");

    // Allow liveliness tokens to propagate
    tokio::time::sleep(Duration::from_secs(1)).await;

    let client =
        ServiceClient::new(&session3, &ke, Duration::from_secs(5)).expect("client should build");

    let count = client
        .instance_count()
        .await
        .expect("instance_count should work");
    assert_eq!(count, 2, "should see 2 server instances initially");

    // Drop server2 — its liveliness token should be undeclared
    drop(server2);
    tokio::time::sleep(Duration::from_secs(1)).await;

    let count = client
        .instance_count()
        .await
        .expect("instance_count should work");
    assert_eq!(count, 1, "should see 1 server instance after dropping one");

    session1.close().await.unwrap();
    session2.close().await.unwrap();
    session3.close().await.unwrap();
}
