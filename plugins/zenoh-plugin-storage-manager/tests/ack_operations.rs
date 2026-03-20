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

use std::thread::sleep;

use tokio::runtime::Runtime;
use zenoh::{
    internal::zasync_executor_init,
    query::Reply,
    Config,
};
use zenoh_backend_traits::StorageInsertionResult;
use zenoh_plugin_trait::Plugin;

async fn test_ack_operations() {
    async {
        zasync_executor_init!();
    }
    .await;

    let mut config = Config::default();
    config
        .insert_json5(
            "plugins/storage-manager",
            r#"{
                    storages: {
                        ack_test: {
                            key_expr: "ack/test/**",
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
    let storage =
        zenoh_plugin_storage_manager::StoragesPlugin::start("storage-manager", &runtime).unwrap();

    let session = zenoh::session::init(runtime).await.unwrap();

    sleep(std::time::Duration::from_secs(1));

    // --- Test 1: _ack_put triggers Storage::put() and returns StorageInsertionResult ---
    println!("--- Test: _ack_put returns insertion result ---");

    let replies: Vec<Reply> = session
        .get("ack/test/a?_ack_put=true")
        .payload("hello")
        .await
        .unwrap()
        .into_iter()
        .collect();

    assert_eq!(replies.len(), 1, "Expected exactly one reply from _ack_put");
    let sample = replies[0].result().expect("Expected Ok reply, got error");
    let bytes: Vec<u8> = sample.payload().to_bytes().into_owned();
    assert_eq!(bytes.len(), 1, "Expected single byte payload for result");
    assert_eq!(
        bytes[0],
        StorageInsertionResult::Inserted as u8,
        "Expected Inserted result from _ack_put"
    );

    // Verify the data was actually written by doing a normal GET
    sleep(std::time::Duration::from_millis(10));
    let get_replies: Vec<Reply> = session
        .get("ack/test/a")
        .await
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(
        get_replies.len(),
        1,
        "Expected data to be retrievable after _ack_put"
    );
    let get_sample = get_replies[0].result().expect("Expected Ok reply");
    assert_eq!(
        get_sample.payload().try_to_string().unwrap(),
        "hello",
        "Stored value should match what was sent via _ack_put"
    );

    // --- Test 2: _ack_delete triggers Storage::delete() and returns result ---
    println!("--- Test: _ack_delete returns deletion result ---");

    // Data 'ack/test/a' already exists from test 1
    let replies: Vec<Reply> = session
        .get("ack/test/a?_ack_delete=true")
        .await
        .unwrap()
        .into_iter()
        .collect();

    assert_eq!(
        replies.len(),
        1,
        "Expected exactly one reply from _ack_delete"
    );
    let sample = replies[0].result().expect("Expected Ok reply, got error");
    let bytes: Vec<u8> = sample.payload().to_bytes().into_owned();
    assert_eq!(bytes.len(), 1, "Expected single byte payload for result");
    assert_eq!(
        bytes[0],
        StorageInsertionResult::Deleted as u8,
        "Expected Deleted result from _ack_delete"
    );

    // Verify the data was actually deleted
    sleep(std::time::Duration::from_millis(100));
    let get_replies: Vec<Reply> = session
        .get("ack/test/a")
        .await
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(
        get_replies.len(),
        0,
        "Data should be gone after _ack_delete"
    );

    // --- Test 3: Regular GET queries are unaffected ---
    println!("--- Test: regular GET unaffected ---");

    session.put("ack/test/b", "normal_value").await.unwrap();
    sleep(std::time::Duration::from_millis(100));

    // Regular GET should work as before
    let replies: Vec<Reply> = session
        .get("ack/test/b")
        .await
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(replies.len(), 1);
    let sample = replies[0].result().expect("Expected Ok reply");
    assert_eq!(sample.payload().try_to_string().unwrap(), "normal_value");

    // GET with unrelated parameters should also work normally
    let replies: Vec<Reply> = session
        .get("ack/test/b?some_other_param=foo")
        .await
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(replies.len(), 1);
    let sample = replies[0].result().expect("Expected Ok reply");
    assert_eq!(sample.payload().try_to_string().unwrap(), "normal_value");

    // --- Test 4: _ack_put without payload returns error reply ---
    println!("--- Test: _ack_put without payload returns error ---");

    let replies: Vec<Reply> = session
        .get("ack/test/c?_ack_put=true")
        .await
        .unwrap()
        .into_iter()
        .collect();

    assert_eq!(
        replies.len(),
        1,
        "Expected exactly one reply (error) from _ack_put without payload"
    );
    assert!(
        replies[0].result().is_err(),
        "Expected error reply when _ack_put is called without a payload"
    );

    drop(storage);
}

#[test]
fn ack_operations_test() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async { test_ack_operations().await });
}
