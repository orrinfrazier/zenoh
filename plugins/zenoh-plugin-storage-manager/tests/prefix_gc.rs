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

// Integration tests for prefix-scoped GC.
//
// Validates that GC with `prefix_lifespans` correctly expires event keys
// (matching `**/events/**`) while preserving entity state keys in the same
// storage.

use std::thread::sleep;
use std::time::Duration;

use tokio::runtime::Runtime;
use zenoh::{internal::zasync_executor_init, query::Reply, sample::Sample, Config, Session};
use zenoh_plugin_trait::Plugin;

async fn put_data(session: &Session, key_expr: &str, value: &str) {
    session.put(key_expr, value).await.unwrap();
}

async fn get_data(session: &Session, key_expr: &str) -> Vec<Sample> {
    let replies: Vec<Reply> = session.get(key_expr).await.unwrap().into_iter().collect();
    let mut samples = Vec::new();
    for reply in replies {
        if let Ok(sample) = reply.into_result() {
            samples.push(sample);
        }
    }
    samples
}

/// Build an in-process Zenoh runtime + storage plugin with the given storage config JSON5.
async fn setup_zenoh(
    storage_config_json5: &str,
) -> (
    Session,
    Box<dyn std::any::Any + Send + Sync>, // storage plugin handle (kept alive via ownership)
) {
    zasync_executor_init!();
    let mut config = Config::default();
    config
        .insert_json5("plugins/storage-manager", storage_config_json5)
        .unwrap();
    config
        .insert_json5(
            "timestamping",
            r#"{
                enabled: {
                    router: true,
                    peer: true,
                    client: true
                }
            }"#,
        )
        .unwrap();

    let runtime = zenoh::internal::runtime::RuntimeBuilder::new(config)
        .build()
        .await
        .unwrap()
        .into();
    let storage =
        zenoh_plugin_storage_manager::StoragesPlugin::start("storage-manager", &runtime).unwrap();
    let session = zenoh::session::init(runtime).await.unwrap();

    // Wait for storage plugin to initialize
    sleep(Duration::from_secs(1));

    (session, Box::new(storage))
}

// ---------------------------------------------------------------------------
// Test 1: Event keys expire
// ---------------------------------------------------------------------------
// Put 10 event keys at `pfx1/events/{n}` with a 1-second prefix lifespan.
// After GC runs, all 10 must be deleted from storage.
async fn test_event_keys_expire_impl() {
    let (session, _storage) = setup_zenoh(
        r#"{
            storages: {
                pfx_gc_1: {
                    key_expr: "pfx1/**",
                    volume: { id: "memory" },
                    garbage_collection: {
                        period: 1,
                        lifespan: 86400,
                        prefix_lifespans: [
                            { key_expr: "**/events/**", lifespan: 1, delete_data: true }
                        ]
                    }
                }
            }
        }"#,
    )
    .await;

    // Put 10 event keys
    for i in 0..10 {
        put_data(&session, &format!("pfx1/events/ts_{i}"), &format!("ev{i}")).await;
        sleep(Duration::from_millis(10));
    }

    // Confirm all 10 are present before GC kicks in
    let data = get_data(&session, "pfx1/events/*").await;
    assert_eq!(data.len(), 10, "Expected 10 event keys before GC");

    // Wait for keys to age past 1s lifespan + GC to trigger
    sleep(Duration::from_secs(4));

    // All event keys should be deleted
    let data = get_data(&session, "pfx1/events/*").await;
    assert_eq!(
        data.len(),
        0,
        "Expected all event keys deleted after prefix GC"
    );
}

#[test]
fn event_keys_expire() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async { test_event_keys_expire_impl().await });
}

// ---------------------------------------------------------------------------
// Test 2: Entity keys preserved
// ---------------------------------------------------------------------------
// Put 10 entity state keys at `pfx2/devices/{n}/status`. Same GC config with
// `**/events/**` prefix. Entity keys do NOT match the prefix and must survive.
async fn test_entity_keys_preserved_impl() {
    let (session, _storage) = setup_zenoh(
        r#"{
            storages: {
                pfx_gc_2: {
                    key_expr: "pfx2/**",
                    volume: { id: "memory" },
                    garbage_collection: {
                        period: 1,
                        lifespan: 86400,
                        prefix_lifespans: [
                            { key_expr: "**/events/**", lifespan: 1, delete_data: true }
                        ]
                    }
                }
            }
        }"#,
    )
    .await;

    // Put 10 entity state keys
    for i in 0..10 {
        put_data(
            &session,
            &format!("pfx2/devices/{i}/status"),
            &format!("online_{i}"),
        )
        .await;
        sleep(Duration::from_millis(10));
    }

    // Confirm all 10 are present
    let data = get_data(&session, "pfx2/devices/*/status").await;
    assert_eq!(data.len(), 10, "Expected 10 entity keys before GC");

    // Wait for GC to run multiple times
    sleep(Duration::from_secs(4));

    // All entity keys must still be present (not matched by **/events/**)
    let data = get_data(&session, "pfx2/devices/*/status").await;
    assert_eq!(
        data.len(),
        10,
        "Expected all entity keys preserved after prefix GC"
    );
}

#[test]
fn entity_keys_preserved() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async { test_entity_keys_preserved_impl().await });
}

// ---------------------------------------------------------------------------
// Test 3: Mixed workload
// ---------------------------------------------------------------------------
// Interleave event and entity puts. GC deletes only event keys past lifespan.
async fn test_mixed_workload_impl() {
    let (session, _storage) = setup_zenoh(
        r#"{
            storages: {
                pfx_gc_3: {
                    key_expr: "pfx3/**",
                    volume: { id: "memory" },
                    garbage_collection: {
                        period: 1,
                        lifespan: 86400,
                        prefix_lifespans: [
                            { key_expr: "**/events/**", lifespan: 1, delete_data: true }
                        ]
                    }
                }
            }
        }"#,
    )
    .await;

    // Interleave event and entity puts
    for i in 0..10 {
        if i % 2 == 0 {
            put_data(
                &session,
                &format!("pfx3/events/ts_{i}"),
                &format!("ev{i}"),
            )
            .await;
        } else {
            put_data(
                &session,
                &format!("pfx3/devices/{i}/status"),
                &format!("on_{i}"),
            )
            .await;
        }
        sleep(Duration::from_millis(10));
    }

    // Verify: 5 event keys + 5 entity keys present
    let events = get_data(&session, "pfx3/events/*").await;
    let entities = get_data(&session, "pfx3/devices/*/status").await;
    assert_eq!(events.len(), 5, "Expected 5 event keys before GC");
    assert_eq!(entities.len(), 5, "Expected 5 entity keys before GC");

    // Wait for GC
    sleep(Duration::from_secs(4));

    // Event keys deleted, entity keys preserved
    let events = get_data(&session, "pfx3/events/*").await;
    let entities = get_data(&session, "pfx3/devices/*/status").await;
    assert_eq!(
        events.len(),
        0,
        "Expected all event keys deleted after prefix GC"
    );
    assert_eq!(
        entities.len(),
        5,
        "Expected all entity keys preserved after prefix GC"
    );
}

#[test]
fn mixed_workload() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async { test_mixed_workload_impl().await });
}

// ---------------------------------------------------------------------------
// Test 4: Recent events preserved
// ---------------------------------------------------------------------------
// Put event keys with timestamps younger than the prefix lifespan. GC runs but
// should NOT delete them — they haven't aged past the threshold.
async fn test_recent_events_preserved_impl() {
    let (session, _storage) = setup_zenoh(
        r#"{
            storages: {
                pfx_gc_4: {
                    key_expr: "pfx4/**",
                    volume: { id: "memory" },
                    garbage_collection: {
                        period: 1,
                        lifespan: 86400,
                        prefix_lifespans: [
                            { key_expr: "**/events/**", lifespan: 3600, delete_data: true }
                        ]
                    }
                }
            }
        }"#,
    )
    .await;

    // Put 10 event keys (timestamps are "now" — well within the 1-hour lifespan)
    for i in 0..10 {
        put_data(&session, &format!("pfx4/events/ts_{i}"), &format!("ev{i}")).await;
        sleep(Duration::from_millis(10));
    }

    // Confirm all present
    let data = get_data(&session, "pfx4/events/*").await;
    assert_eq!(data.len(), 10, "Expected 10 event keys before GC");

    // Wait for several GC ticks
    sleep(Duration::from_secs(4));

    // All event keys must still be present (only ~4s old vs 3600s lifespan)
    let data = get_data(&session, "pfx4/events/*").await;
    assert_eq!(
        data.len(),
        10,
        "Expected recent event keys preserved (within lifespan)"
    );
}

#[test]
fn recent_events_preserved() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async { test_recent_events_preserved_impl().await });
}

// ---------------------------------------------------------------------------
// Test 5: No prefix config — default GC, no regression
// ---------------------------------------------------------------------------
// Default GC config (no prefix_lifespans). Even with a short default lifespan,
// only metadata (cache) is cleaned — storage data persists. No regression.
async fn test_no_prefix_config_impl() {
    let (session, _storage) = setup_zenoh(
        r#"{
            storages: {
                pfx_gc_5: {
                    key_expr: "pfx5/**",
                    volume: { id: "memory" },
                    garbage_collection: {
                        period: 1,
                        lifespan: 1
                    }
                }
            }
        }"#,
    )
    .await;

    // Put a mix of event-like and entity-like keys
    for i in 0..5 {
        put_data(
            &session,
            &format!("pfx5/events/ts_{i}"),
            &format!("ev{i}"),
        )
        .await;
        put_data(
            &session,
            &format!("pfx5/devices/{i}/status"),
            &format!("on_{i}"),
        )
        .await;
        sleep(Duration::from_millis(10));
    }

    // Verify all 10 present
    let events = get_data(&session, "pfx5/events/*").await;
    let entities = get_data(&session, "pfx5/devices/*/status").await;
    assert_eq!(events.len(), 5, "Expected 5 event keys before GC");
    assert_eq!(entities.len(), 5, "Expected 5 entity keys before GC");

    // Wait for GC to run (default behavior: only metadata cleanup, NOT data deletion)
    sleep(Duration::from_secs(4));

    // All data must still be present — default GC never calls storage.delete()
    let events = get_data(&session, "pfx5/events/*").await;
    let entities = get_data(&session, "pfx5/devices/*/status").await;
    assert_eq!(
        events.len(),
        5,
        "Default GC must NOT delete event data from storage"
    );
    assert_eq!(
        entities.len(),
        5,
        "Default GC must NOT delete entity data from storage"
    );
}

#[test]
fn no_prefix_config() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async { test_no_prefix_config_impl().await });
}
