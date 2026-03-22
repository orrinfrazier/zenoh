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

use zenoh_ext::{
    z_deserialize, z_serialize, DeadlineContext, ServiceClient, ServiceError, ServiceServer,
    DEADLINE_ATTACHMENT_KEY, METHOD_ATTACHMENT_KEY,
};

/// Unique key expression generator to avoid cross-test interference.
fn unique_ke(suffix: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("test/rpc/client/{id}/{suffix}")
}

// -- Test: typed call with ServiceClient and ServiceServer --

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_call_returns_typed_response() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let ke = unique_ke("typed");

    let _server = ServiceServer::builder(&session, ke.as_str())
        .method_fn("greet", |query| {
            Box::pin(async move {
                let name: String = query
                    .payload()
                    .map(|p| z_deserialize(p).unwrap())
                    .unwrap_or_default();
                let greeting = format!("Hello, {name}!");
                Ok(z_serialize(&greeting))
            })
        })
        .await
        .expect("server should start");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let client =
        ServiceClient::new(&session, &ke, Duration::from_secs(5)).expect("client should build");

    let response: String = client
        .call("greet", &"World".to_string(), None)
        .await
        .expect("call should succeed");
    assert_eq!(response, "Hello, World!");
}

// -- Test: raw call with ServiceClient and ServiceServer --

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_call_returns_raw_response() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let ke = unique_ke("raw");

    let _server = ServiceServer::builder(&session, ke.as_str())
        .method_fn("echo", |query| {
            let payload = query
                .payload()
                .map(|p| p.to_bytes().to_vec())
                .unwrap_or_default();
            Box::pin(async move { Ok(ZBytes::from(payload)) })
        })
        .await
        .expect("server should start");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let client =
        ServiceClient::new(&session, &ke, Duration::from_secs(5)).expect("client should build");

    let request = ZBytes::from(b"raw bytes here".as_ref());
    let response = client
        .call_raw("echo", request, None)
        .await
        .expect("call_raw should succeed");
    assert_eq!(response.to_bytes().as_ref(), b"raw bytes here");
}

// -- Test: call non-existent method returns MethodNotFound --

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn call_nonexistent_method_returns_method_not_found() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let ke = unique_ke("not_found");

    let _server = ServiceServer::builder(&session, ke.as_str())
        .method_fn("exists", |_query| {
            Box::pin(async { Ok(ZBytes::default()) })
        })
        .await
        .expect("server should start");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let client =
        ServiceClient::new(&session, &ke, Duration::from_secs(5)).expect("client should build");

    let result = client
        .call_raw("nonexistent", ZBytes::default(), None)
        .await;

    match result {
        Err(ServiceError::MethodNotFound { method }) => {
            assert_eq!(method, "nonexistent");
        }
        other => panic!("expected MethodNotFound, got {other:?}"),
    }
}

// -- Test: deadline attachment is set on the query (server-side check) --

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deadline_attachment_is_set_on_query() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let ke = unique_ke("deadline");

    let _server = ServiceServer::builder(&session, ke.as_str())
        .method_fn("check_deadline", |query| {
            Box::pin(async move {
                // Extract the deadline from the query's attachment
                let ctx = DeadlineContext::from_query(query)
                    .ok_or_else(|| ServiceError::Internal {
                        message: "no deadline attachment found".to_string(),
                    })?;

                // The deadline should not be expired (we just sent the query)
                if ctx.is_expired() {
                    return Err(ServiceError::Internal {
                        message: "deadline already expired".to_string(),
                    });
                }

                // Return the remaining budget in ms
                let remaining_ms = ctx.remaining().as_millis() as u64;
                Ok(z_serialize(&remaining_ms))
            })
        })
        .await
        .expect("server should start");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let client =
        ServiceClient::new(&session, &ke, Duration::from_secs(10)).expect("client should build");

    let remaining_ms: u64 = client
        .call("check_deadline", &(), None)
        .await
        .expect("call should succeed");

    // The remaining budget should be close to 10 seconds (allow margin)
    assert!(
        remaining_ms > 8_000,
        "remaining_ms {remaining_ms} should be > 8000"
    );
    assert!(
        remaining_ms <= 10_000,
        "remaining_ms {remaining_ms} should be <= 10000"
    );
}

// -- Test: client with custom timeout overrides default --

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn custom_timeout_overrides_default() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let ke = unique_ke("custom_timeout");

    let _server = ServiceServer::builder(&session, ke.as_str())
        .method_fn("check_deadline", |query| {
            Box::pin(async move {
                let ctx = DeadlineContext::from_query(query)
                    .ok_or_else(|| ServiceError::Internal {
                        message: "no deadline attachment".to_string(),
                    })?;
                let remaining_ms = ctx.remaining().as_millis() as u64;
                Ok(z_serialize(&remaining_ms))
            })
        })
        .await
        .expect("server should start");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Default timeout is 1 second, but we override with 20 seconds
    let client =
        ServiceClient::new(&session, &ke, Duration::from_secs(1)).expect("client should build");

    let remaining_ms: u64 = client
        .call("check_deadline", &(), Some(Duration::from_secs(20)))
        .await
        .expect("call should succeed");

    // The remaining budget should be close to 20 seconds (not 1 second)
    assert!(
        remaining_ms > 18_000,
        "remaining_ms {remaining_ms} should be > 18000 (override should use 20s, not default 1s)"
    );
}

// -- Test: handler error is propagated through client --

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handler_error_is_propagated_through_client() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let ke = unique_ke("handler_error");

    let _server = ServiceServer::builder(&session, ke.as_str())
        .method_fn("fail", |_query| {
            Box::pin(async {
                Err(ServiceError::Application {
                    code: 503,
                    message: "service unavailable".to_string(),
                })
            })
        })
        .await
        .expect("server should start");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let client =
        ServiceClient::new(&session, &ke, Duration::from_secs(5)).expect("client should build");

    let result = client.call_raw("fail", ZBytes::default(), None).await;

    match result {
        Err(ServiceError::Application { code, message }) => {
            assert_eq!(code, 503);
            assert_eq!(message, "service unavailable");
        }
        other => panic!("expected Application error, got {other:?}"),
    }
}

// -- Test: method attachment key is present in client queries --

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn method_attachment_is_present() {
    use std::collections::HashMap;

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let ke = unique_ke("method_attachment");

    let _server = ServiceServer::builder(&session, ke.as_str())
        .method_fn("inspect", |query| {
            Box::pin(async move {
                // Extract the full attachment map and return the method value
                let attachment = query.attachment().ok_or_else(|| ServiceError::Internal {
                    message: "no attachment".to_string(),
                })?;
                let map: HashMap<String, String> =
                    z_deserialize(attachment).map_err(|_| ServiceError::Internal {
                        message: "failed to deserialize attachment".to_string(),
                    })?;
                let method = map
                    .get(METHOD_ATTACHMENT_KEY)
                    .cloned()
                    .unwrap_or_default();
                let has_deadline = map.contains_key(DEADLINE_ATTACHMENT_KEY);
                let result = format!("{method}:{has_deadline}");
                Ok(z_serialize(&result))
            })
        })
        .await
        .expect("server should start");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let client =
        ServiceClient::new(&session, &ke, Duration::from_secs(5)).expect("client should build");

    let result: String = client
        .call("inspect", &(), None)
        .await
        .expect("call should succeed");

    // Method name should be "inspect" and deadline should be present
    assert_eq!(result, "inspect:true");
}

// -- Test: multiple sequential calls work correctly --

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multiple_sequential_calls() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let ke = unique_ke("sequential");

    let _server = ServiceServer::builder(&session, ke.as_str())
        .method_fn("add_one", |query| {
            Box::pin(async move {
                let n: u64 = query
                    .payload()
                    .map(|p| z_deserialize(p).unwrap())
                    .unwrap_or(0);
                Ok(z_serialize(&(n + 1)))
            })
        })
        .await
        .expect("server should start");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let client =
        ServiceClient::new(&session, &ke, Duration::from_secs(5)).expect("client should build");

    for i in 0u64..5 {
        let result: u64 = client
            .call("add_one", &i, None)
            .await
            .expect("call should succeed");
        assert_eq!(result, i + 1, "call {i} returned wrong value");
    }
}
