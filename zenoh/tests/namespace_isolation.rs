//
// Copyright (c) 2025 ZettaScale Technology
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

//! Integration tests for namespace isolation hardening (Milestone 5).
//!
//! Validates auto-deny ACL rules (#43), connection limits (#44),
//! and cross-namespace blocking behavior.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use zenoh::sample::SampleKind;
use zenoh_config::{ModeDependentValue, WhatAmI};
use zenoh_core::{zlock, ztimeout};

const TIMEOUT: Duration = Duration::from_secs(60);
const SLEEP: Duration = Duration::from_secs(1);
const VALUE: &str = "namespace-isolation-test";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn router_config(port: u16) -> zenoh_config::Config {
    let mut config = zenoh_config::Config::default();
    config.set_mode(Some(WhatAmI::Router)).unwrap();
    config
        .listen
        .endpoints
        .set(vec![format!("tcp/127.0.0.1:{port}").parse().unwrap()])
        .unwrap();
    config.scouting.multicast.set_enabled(Some(false)).unwrap();
    config
}

fn client_config(port: u16) -> zenoh_config::Config {
    let mut config = zenoh_config::Config::default();
    config.set_mode(Some(WhatAmI::Client)).unwrap();
    config
        .connect
        .set_endpoints(ModeDependentValue::Unique(vec![
            format!("tcp/127.0.0.1:{port}").parse().unwrap(),
        ]))
        .unwrap();
    config.scouting.multicast.set_enabled(Some(false)).unwrap();
    config
}

fn ns(name: &str) -> Option<zenoh_keyexpr::OwnedNonWildKeyExpr> {
    Some(zenoh_keyexpr::OwnedNonWildKeyExpr::try_from(name.to_string()).unwrap())
}

// ---------------------------------------------------------------------------
// Test 1: Cross-namespace blocked
// ---------------------------------------------------------------------------

/// Two clients with different namespaces through a router.
/// Client A (tenant-a) publishes. Client B (tenant-b) subscribes to the same
/// key expression. B should NOT receive the message because the router's
/// namespace isolation (ENamespace + ACL auto-deny) blocks cross-namespace flow.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cross_namespace_blocked() {
    zenoh::init_log_from_env_or("error");

    let port = 19301;
    let mut rconfig = router_config(port);
    rconfig.namespace = ns("tenant-a");
    rconfig
        .insert_json5(
            "access_control",
            r#"{
                "enabled": true,
                "default_permission": "allow",
                "rules": [],
                "subjects": [],
                "policies": []
            }"#,
        )
        .unwrap();

    let router = ztimeout!(zenoh::open(rconfig)).unwrap();

    // Client A: namespace tenant-a (matches router)
    let mut ca = client_config(port);
    ca.namespace = ns("tenant-a");
    let session_a = ztimeout!(zenoh::open(ca)).unwrap();

    // Client B: namespace tenant-b (different from router)
    let mut cb = client_config(port);
    cb.namespace = ns("tenant-b");
    let session_b = ztimeout!(zenoh::open(cb)).unwrap();

    let received = Arc::new(Mutex::new(false));
    let recv_clone = received.clone();
    let _sub = session_b
        .declare_subscriber("data")
        .callback(move |_sample| {
            *zlock!(recv_clone) = true;
        })
        .await
        .unwrap();

    tokio::time::sleep(SLEEP).await;

    // Client A publishes to "data" (becomes "tenant-a/data" on wire)
    session_a.put("data", VALUE).await.unwrap();
    tokio::time::sleep(SLEEP).await;

    // Client B should NOT receive — cross-namespace blocked
    assert!(
        !*zlock!(received),
        "Cross-namespace message should be blocked"
    );

    ztimeout!(session_a.close()).unwrap();
    ztimeout!(session_b.close()).unwrap();
    ztimeout!(router.close()).unwrap();
}

// ---------------------------------------------------------------------------
// Test 2: Same-namespace allowed
// ---------------------------------------------------------------------------

/// Two clients with the SAME namespace through a router.
/// Pub/sub should work normally within the namespace.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_same_namespace_allowed() {
    zenoh::init_log_from_env_or("error");

    let port = 19302;
    let mut rconfig = router_config(port);
    rconfig.namespace = ns("tenant-a");
    rconfig
        .insert_json5(
            "access_control",
            r#"{
                "enabled": true,
                "default_permission": "allow",
                "rules": [],
                "subjects": [],
                "policies": []
            }"#,
        )
        .unwrap();

    let router = ztimeout!(zenoh::open(rconfig)).unwrap();

    // Both clients: namespace tenant-a
    let mut ca = client_config(port);
    ca.namespace = ns("tenant-a");
    let session_a = ztimeout!(zenoh::open(ca)).unwrap();

    let mut cb = client_config(port);
    cb.namespace = ns("tenant-a");
    let session_b = ztimeout!(zenoh::open(cb)).unwrap();

    let received_value = Arc::new(Mutex::new(String::new()));
    let recv_clone = received_value.clone();
    let _sub = session_b
        .declare_subscriber("data")
        .callback(move |sample| {
            if sample.kind() == SampleKind::Put {
                *zlock!(recv_clone) = sample
                    .payload()
                    .try_to_string()
                    .unwrap_or_default()
                    .to_string();
            }
        })
        .await
        .unwrap();

    tokio::time::sleep(SLEEP).await;

    session_a.put("data", VALUE).await.unwrap();
    tokio::time::sleep(SLEEP).await;

    assert_eq!(
        *zlock!(received_value),
        VALUE,
        "Same-namespace message should be delivered"
    );

    ztimeout!(session_a.close()).unwrap();
    ztimeout!(session_b.close()).unwrap();
    ztimeout!(router.close()).unwrap();
}

// ---------------------------------------------------------------------------
// Test 3: Connection limit
// ---------------------------------------------------------------------------

/// Router with max_connections=2. Two clients connect successfully.
/// Third connection is rejected.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_connection_limit() {
    zenoh::init_log_from_env_or("error");

    let port = 19303;
    let mut rconfig = router_config(port);
    rconfig.namespace = ns("tenant-a");
    rconfig
        .insert_json5("max_connections", "2")
        .unwrap();

    let _router = ztimeout!(zenoh::open(rconfig)).unwrap();

    let c1 = ztimeout!(zenoh::open(client_config(port)));
    assert!(c1.is_ok(), "First client should connect");

    let c2 = ztimeout!(zenoh::open(client_config(port)));
    assert!(c2.is_ok(), "Second client should connect");

    let c3 = ztimeout!(zenoh::open(client_config(port)));
    assert!(
        c3.is_err(),
        "Third client should be rejected (max_connections=2)"
    );

    ztimeout!(c1.unwrap().close()).unwrap();
    ztimeout!(c2.unwrap().close()).unwrap();
    ztimeout!(_router.close()).unwrap();
}

// ---------------------------------------------------------------------------
// Test 4: No namespace — no change
// ---------------------------------------------------------------------------

/// Without namespaces, normal pub/sub works. No auto-deny rules generated.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_no_namespace_no_change() {
    zenoh::init_log_from_env_or("error");

    let port = 19304;
    let mut rconfig = router_config(port);
    // ACL enabled but no namespace — should not affect behavior
    rconfig
        .insert_json5(
            "access_control",
            r#"{
                "enabled": true,
                "default_permission": "allow",
                "rules": [],
                "subjects": [],
                "policies": []
            }"#,
        )
        .unwrap();

    let router = ztimeout!(zenoh::open(rconfig)).unwrap();

    let session_a = ztimeout!(zenoh::open(client_config(port))).unwrap();
    let session_b = ztimeout!(zenoh::open(client_config(port))).unwrap();

    let received_value = Arc::new(Mutex::new(String::new()));
    let recv_clone = received_value.clone();
    let _sub = session_b
        .declare_subscriber("test/demo")
        .callback(move |sample| {
            if sample.kind() == SampleKind::Put {
                *zlock!(recv_clone) = sample
                    .payload()
                    .try_to_string()
                    .unwrap_or_default()
                    .to_string();
            }
        })
        .await
        .unwrap();

    tokio::time::sleep(SLEEP).await;

    session_a.put("test/demo", VALUE).await.unwrap();
    tokio::time::sleep(SLEEP).await;

    assert_eq!(
        *zlock!(received_value),
        VALUE,
        "Without namespace, pub/sub should work normally"
    );

    ztimeout!(session_a.close()).unwrap();
    ztimeout!(session_b.close()).unwrap();
    ztimeout!(router.close()).unwrap();
}

// ---------------------------------------------------------------------------
// Test 5: ACL override allows cross-namespace
// ---------------------------------------------------------------------------

/// Explicit ACL allow rule overrides namespace auto-deny.
/// Router has namespace "tenant-a" with ACL auto-deny, but an explicit allow
/// rule for "**" on all interfaces lets everything through.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_acl_override_allows_cross_namespace() {
    zenoh::init_log_from_env_or("error");

    let port = 19305;
    let mut rconfig = router_config(port);
    rconfig.namespace = ns("tenant-a");
    // ACL with explicit allow rule for all keys — should override namespace auto-deny
    rconfig
        .insert_json5(
            "access_control",
            r#"{
                "enabled": true,
                "default_permission": "deny",
                "rules": [
                    {
                        "id": "allow-all",
                        "permission": "allow",
                        "flows": ["egress", "ingress"],
                        "messages": [
                            "put", "delete", "declare_subscriber",
                            "declare_queryable", "query", "reply"
                        ],
                        "key_exprs": ["**"]
                    }
                ],
                "subjects": [
                    {
                        "id": "everyone",
                        "interfaces": ["lo", "lo0"]
                    }
                ],
                "policies": [
                    {
                        "rules": ["allow-all"],
                        "subjects": ["everyone"]
                    }
                ]
            }"#,
        )
        .unwrap();

    let router = ztimeout!(zenoh::open(rconfig)).unwrap();

    // Client with same namespace
    let mut ca = client_config(port);
    ca.namespace = ns("tenant-a");
    let session_a = ztimeout!(zenoh::open(ca)).unwrap();

    let received_value = Arc::new(Mutex::new(String::new()));
    let recv_clone = received_value.clone();
    let _sub = session_a
        .declare_subscriber("data")
        .callback(move |sample| {
            if sample.kind() == SampleKind::Put {
                *zlock!(recv_clone) = sample
                    .payload()
                    .try_to_string()
                    .unwrap_or_default()
                    .to_string();
            }
        })
        .await
        .unwrap();

    tokio::time::sleep(SLEEP).await;

    // Publish from same session — the explicit allow rule should let it through
    // even though we have default_permission: deny
    session_a.put("data", VALUE).await.unwrap();
    tokio::time::sleep(SLEEP).await;

    assert_eq!(
        *zlock!(received_value),
        VALUE,
        "Explicit ACL allow should override default deny"
    );

    ztimeout!(session_a.close()).unwrap();
    ztimeout!(router.close()).unwrap();
}
