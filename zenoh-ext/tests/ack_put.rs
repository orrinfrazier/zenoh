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

//! Integration tests for `ack_put()` and `ack_delete()` zenoh-ext helpers.
//!
//! Validates the full round-trip: zenoh-ext helper -> router -> storage queryable
//! -> Storage::put() -> StorageInsertionResult -> reply -> helper returns result.

use std::time::Duration;

use zenoh::{
    bytes::Encoding,
    query::Reply,
    Config,
};
use zenoh_ext::{ack_delete, ack_put, StorageInsertionResult};
use zenoh_plugin_trait::Plugin;

const ACK_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ack_put_integration() {
    // Setup in-process router with memory storage
    let mut config = Config::default();
    config
        .insert_json5(
            "plugins/storage-manager",
            r#"{
                storages: {
                    ack_test: {
                        key_expr: "ack/ext/**",
                        volume: {
                            id: "memory"
                        }
                    }
                }
            }"#,
        )
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
    let _storage =
        zenoh_plugin_storage_manager::StoragesPlugin::start("storage-manager", &runtime).unwrap();
    let session = zenoh::session::init(runtime).await.unwrap();

    tokio::time::sleep(Duration::from_secs(1)).await;

    // --- Scenario 1: Basic ack'd put returns Inserted ---
    let key_a = zenoh::key_expr::KeyExpr::try_from("ack/ext/a").unwrap();
    let result = ack_put(
        &session,
        &key_a,
        "hello",
        Encoding::TEXT_PLAIN,
        ACK_TIMEOUT,
    )
    .await
    .unwrap();
    assert_eq!(
        result,
        StorageInsertionResult::Inserted,
        "ack_put on new key should return Inserted"
    );

    // --- Scenario 2: Ack'd delete on existing key returns Deleted ---
    let result = ack_delete(&session, &key_a, ACK_TIMEOUT).await.unwrap();
    assert_eq!(
        result,
        StorageInsertionResult::Deleted,
        "ack_delete on existing key should return Deleted"
    );

    // --- Scenario 3: Timeout when no storage configured ---
    let no_storage_key = zenoh::key_expr::KeyExpr::try_from("no/storage/here").unwrap();
    let result = ack_put(
        &session,
        &no_storage_key,
        "orphan",
        Encoding::TEXT_PLAIN,
        Duration::from_millis(500),
    )
    .await;
    assert!(
        result.is_err(),
        "ack_put with no matching storage should return timeout error"
    );

    // --- Scenario 4: Round-trip verification ---
    let key_b = zenoh::key_expr::KeyExpr::try_from("ack/ext/b").unwrap();
    let result = ack_put(
        &session,
        &key_b,
        "round-trip",
        Encoding::TEXT_PLAIN,
        ACK_TIMEOUT,
    )
    .await
    .unwrap();
    assert_eq!(result, StorageInsertionResult::Inserted);

    tokio::time::sleep(Duration::from_millis(100)).await;

    let replies: Vec<Reply> = session
        .get(&key_b)
        .timeout(ACK_TIMEOUT)
        .await
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(
        replies.len(),
        1,
        "Stored value should be retrievable via session.get()"
    );
    let sample = replies[0].result().expect("Expected Ok reply");
    assert_eq!(
        sample.payload().try_to_string().unwrap(),
        "round-trip",
        "Retrieved value should match what was sent via ack_put"
    );

    // --- Scenario 5: Non-interference with regular put/get ---
    let key_c = zenoh::key_expr::KeyExpr::try_from("ack/ext/c").unwrap();
    session.put(&key_c, "normal-value").await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let replies: Vec<Reply> = session
        .get(&key_c)
        .timeout(ACK_TIMEOUT)
        .await
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(replies.len(), 1, "Regular put should be retrievable via get");
    let sample = replies[0].result().expect("Expected Ok reply");
    assert_eq!(sample.payload().try_to_string().unwrap(), "normal-value");

    // Ack'd operations on same key expression still work
    let result = ack_put(
        &session,
        &key_c,
        "ack-overwrite",
        Encoding::TEXT_PLAIN,
        ACK_TIMEOUT,
    )
    .await
    .unwrap();
    assert_eq!(
        result,
        StorageInsertionResult::Replaced,
        "ack_put over existing value should return Replaced"
    );
}

/// Ensures the duplicated `StorageInsertionResult` in `zenoh-ext` stays in sync
/// with the source-of-truth in `zenoh-backend-traits`.
#[test]
fn discriminant_parity() {
    use zenoh_backend_traits::StorageInsertionResult as BackendResult;
    use zenoh_ext::StorageInsertionResult as ExtResult;

    assert_eq!(BackendResult::Outdated as u8, ExtResult::Outdated as u8);
    assert_eq!(BackendResult::Inserted as u8, ExtResult::Inserted as u8);
    assert_eq!(BackendResult::Replaced as u8, ExtResult::Replaced as u8);
    assert_eq!(BackendResult::Deleted as u8, ExtResult::Deleted as u8);
}
