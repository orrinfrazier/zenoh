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

use std::collections::HashMap;
use std::time::Duration;

use zenoh::bytes::ZBytes;

use zenoh_ext::{
    z_deserialize, z_serialize, MethodHandler, ServiceError, ServiceServer, StatusCode,
    METHOD_ATTACHMENT_KEY,
};

/// Build a method attachment containing the given method name.
fn method_attachment(method: &str) -> ZBytes {
    let map: HashMap<String, String> =
        HashMap::from([(METHOD_ATTACHMENT_KEY.to_string(), method.to_string())]);
    z_serialize(&map)
}

/// Unique key expression generator to avoid cross-test interference.
fn unique_ke(suffix: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("test/rpc/server/{id}/{suffix}")
}

// -- Test: build a ServiceServer with 2 methods, verify it starts without error --

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_server_starts_with_two_methods() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let ke = unique_ke("starts");

    let _server = ServiceServer::builder(&session, ke.as_str())
        .method_fn("ping", |_query| {
            Box::pin(async { Ok(ZBytes::from(b"pong".as_ref())) })
        })
        .method_fn("echo", |query| {
            let payload = query
                .payload()
                .map(|p| p.to_bytes().to_vec())
                .unwrap_or_default();
            Box::pin(async move { Ok(ZBytes::from(payload)) })
        })
        .await
        .expect("server should start without error");
}

// -- Test: query with correct method name dispatches to the right handler --

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dispatch_to_correct_handler() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let ke = unique_ke("dispatch");

    let _server = ServiceServer::builder(&session, ke.as_str())
        .method_fn("get_config", |_query| {
            Box::pin(async { Ok(z_serialize(&"config_value".to_string())) })
        })
        .method_fn("get_status", |_query| {
            Box::pin(async { Ok(z_serialize(&"running".to_string())) })
        })
        .await
        .expect("server should start");

    // Give the queryable time to register
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Query get_config
    let replies = session
        .get(&ke)
        .attachment(method_attachment("get_config"))
        .await
        .unwrap();

    let reply = tokio::time::timeout(Duration::from_secs(5), replies.recv_async())
        .await
        .expect("timeout waiting for reply")
        .expect("channel closed");

    let sample = reply.into_result().expect("reply should be Ok");
    let value: String = z_deserialize(sample.payload()).expect("deserialize response");
    assert_eq!(value, "config_value");

    // Query get_status
    let replies = session
        .get(&ke)
        .attachment(method_attachment("get_status"))
        .await
        .unwrap();

    let reply = tokio::time::timeout(Duration::from_secs(5), replies.recv_async())
        .await
        .expect("timeout waiting for reply")
        .expect("channel closed");

    let sample = reply.into_result().expect("reply should be Ok");
    let value: String = z_deserialize(sample.payload()).expect("deserialize response");
    assert_eq!(value, "running");
}

// -- Test: query with missing method attachment yields ServiceError::InvalidRequest --

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_method_attachment_returns_invalid_request() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let ke = unique_ke("missing_method");

    let _server = ServiceServer::builder(&session, ke.as_str())
        .method_fn("noop", |_query| Box::pin(async { Ok(ZBytes::default()) }))
        .await
        .expect("server should start");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Query without any attachment
    let replies = session.get(&ke).await.unwrap();

    let reply = tokio::time::timeout(Duration::from_secs(5), replies.recv_async())
        .await
        .expect("timeout waiting for reply")
        .expect("channel closed");

    let reply_err = reply.into_result().expect_err("should be an error reply");
    let err: ServiceError =
        z_deserialize(reply_err.payload()).expect("deserialize ServiceError");
    assert_eq!(
        err,
        ServiceError::InvalidRequest {
            message: "missing rpc:method attachment".to_string()
        }
    );
    assert_eq!(err.status_code(), StatusCode::InvalidRequest);
}

// -- Test: query with unknown method yields ServiceError::MethodNotFound --

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_method_returns_method_not_found() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let ke = unique_ke("unknown_method");

    let _server = ServiceServer::builder(&session, ke.as_str())
        .method_fn("known", |_query| Box::pin(async { Ok(ZBytes::default()) }))
        .await
        .expect("server should start");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let replies = session
        .get(&ke)
        .attachment(method_attachment("nonexistent"))
        .await
        .unwrap();

    let reply = tokio::time::timeout(Duration::from_secs(5), replies.recv_async())
        .await
        .expect("timeout waiting for reply")
        .expect("channel closed");

    let reply_err = reply.into_result().expect_err("should be an error reply");
    let err: ServiceError =
        z_deserialize(reply_err.payload()).expect("deserialize ServiceError");
    assert_eq!(
        err,
        ServiceError::MethodNotFound {
            method: "nonexistent".to_string()
        }
    );
    assert_eq!(err.status_code(), StatusCode::MethodNotFound);
}

// -- Test: handler that returns an error produces the correct error reply --

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handler_error_is_propagated() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let ke = unique_ke("handler_error");

    let _server = ServiceServer::builder(&session, ke.as_str())
        .method_fn("fail", |_query| {
            Box::pin(async {
                Err(ServiceError::Internal {
                    message: "something broke".to_string(),
                })
            })
        })
        .await
        .expect("server should start");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let replies = session
        .get(&ke)
        .attachment(method_attachment("fail"))
        .await
        .unwrap();

    let reply = tokio::time::timeout(Duration::from_secs(5), replies.recv_async())
        .await
        .expect("timeout waiting for reply")
        .expect("channel closed");

    let reply_err = reply.into_result().expect_err("should be an error reply");
    let err: ServiceError =
        z_deserialize(reply_err.payload()).expect("deserialize ServiceError");
    assert_eq!(
        err,
        ServiceError::Internal {
            message: "something broke".to_string()
        }
    );
}

// -- Test: success reply carries status code Ok in attachment --

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn success_reply_has_ok_status_attachment() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let ke = unique_ke("status_ok");

    let _server = ServiceServer::builder(&session, ke.as_str())
        .method_fn("ping", |_query| {
            Box::pin(async { Ok(ZBytes::from(b"pong".as_ref())) })
        })
        .await
        .expect("server should start");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let replies = session
        .get(&ke)
        .attachment(method_attachment("ping"))
        .await
        .unwrap();

    let reply = tokio::time::timeout(Duration::from_secs(5), replies.recv_async())
        .await
        .expect("timeout waiting for reply")
        .expect("channel closed");

    let sample = reply.into_result().expect("reply should be Ok");

    // Check the status code attachment
    let attachment = sample.attachment().expect("reply should have an attachment");
    let map: HashMap<String, String> =
        z_deserialize(attachment).expect("deserialize attachment map");
    let status_str = map
        .get("rpc:status")
        .expect("should have rpc:status key");
    assert_eq!(status_str, "0", "status code should be Ok (0)");
}

// -- Test: struct implementing MethodHandler works --

struct EchoHandler;

#[async_trait::async_trait]
impl MethodHandler for EchoHandler {
    async fn handle(&self, query: &zenoh::query::Query) -> Result<ZBytes, ServiceError> {
        let payload = query
            .payload()
            .map(|p| p.to_bytes().to_vec())
            .unwrap_or_default();
        Ok(ZBytes::from(payload))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn struct_method_handler_works() {
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let ke = unique_ke("struct_handler");

    let _server = ServiceServer::builder(&session, ke.as_str())
        .method("echo", EchoHandler)
        .await
        .expect("server should start");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let request_data = b"hello world";
    let replies = session
        .get(&ke)
        .payload(ZBytes::from(request_data.as_ref()))
        .attachment(method_attachment("echo"))
        .await
        .unwrap();

    let reply = tokio::time::timeout(Duration::from_secs(5), replies.recv_async())
        .await
        .expect("timeout waiting for reply")
        .expect("channel closed");

    let sample = reply.into_result().expect("reply should be Ok");
    assert_eq!(sample.payload().to_bytes().as_ref(), request_data);
}
