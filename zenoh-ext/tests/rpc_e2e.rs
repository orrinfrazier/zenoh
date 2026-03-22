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

use zenoh_ext::{
    z_deserialize, z_serialize, Deserialize, Serialize, ServiceClient, ServiceError,
    ServiceServer, ZDeserializeError, ZDeserializer, ZSerializer,
};

const TIMEOUT: Duration = Duration::from_secs(10);

macro_rules! ztimeout {
    ($f:expr) => {
        tokio::time::timeout(TIMEOUT, ::core::future::IntoFuture::into_future($f))
            .await
            .unwrap()
    };
}

// ---------------------------------------------------------------------------
// Shared test types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
struct GreetRequest {
    name: String,
}

impl Serialize for GreetRequest {
    fn serialize(&self, serializer: &mut ZSerializer) {
        serializer.serialize(&self.name);
    }
}

impl Deserialize for GreetRequest {
    fn deserialize(deserializer: &mut ZDeserializer) -> Result<Self, ZDeserializeError> {
        Ok(Self {
            name: deserializer.deserialize()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GreetResponse {
    message: String,
}

impl Serialize for GreetResponse {
    fn serialize(&self, serializer: &mut ZSerializer) {
        serializer.serialize(&self.message);
    }
}

impl Deserialize for GreetResponse {
    fn deserialize(deserializer: &mut ZDeserializer) -> Result<Self, ZDeserializeError> {
        Ok(Self {
            message: deserializer.deserialize()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AddRequest {
    a: i32,
    b: i32,
}

impl Serialize for AddRequest {
    fn serialize(&self, serializer: &mut ZSerializer) {
        serializer.serialize(self.a);
        serializer.serialize(self.b);
    }
}

impl Deserialize for AddRequest {
    fn deserialize(deserializer: &mut ZDeserializer) -> Result<Self, ZDeserializeError> {
        Ok(Self {
            a: deserializer.deserialize()?,
            b: deserializer.deserialize()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AddResponse {
    sum: i32,
}

impl Serialize for AddResponse {
    fn serialize(&self, serializer: &mut ZSerializer) {
        serializer.serialize(self.sum);
    }
}

impl Deserialize for AddResponse {
    fn deserialize(deserializer: &mut ZDeserializer) -> Result<Self, ZDeserializeError> {
        Ok(Self {
            sum: deserializer.deserialize()?,
        })
    }
}

// ---------------------------------------------------------------------------
// Session helpers
// ---------------------------------------------------------------------------

/// Create a peer session that listens on the given TCP endpoint with multicast
/// scouting disabled.
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
    ztimeout!(zenoh::open(config)).unwrap()
}

/// Create a peer session that connects to the given TCP endpoint with multicast
/// scouting disabled.
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
    ztimeout!(zenoh::open(config)).unwrap()
}

/// Generate a unique key expression per test to avoid cross-test interference.
fn unique_ke(suffix: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("test/rpc/e2e/{id}/{suffix}")
}

// ---------------------------------------------------------------------------
// 1. Happy path: client -> server -> typed response
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn happy_path_greet_and_add() {
    zenoh_util::init_log_from_env_or("error");

    const ENDPOINT: &str = "tcp/localhost:27500";
    let ke = unique_ke("happy_path");

    let server_session = listening_session(ENDPOINT).await;
    let client_session = connecting_session(ENDPOINT).await;

    // Register a server with two methods: "greet" and "add"
    let _server = ztimeout!(ServiceServer::builder(&server_session, ke.as_str())
        .method_fn("greet", |query| {
            Box::pin(async move {
                let req: GreetRequest = query
                    .payload()
                    .map(z_deserialize)
                    .transpose()
                    .map_err(|_| ServiceError::InvalidRequest {
                        message: "failed to deserialize GreetRequest".to_string(),
                    })?
                    .ok_or_else(|| ServiceError::InvalidRequest {
                        message: "missing request payload".to_string(),
                    })?;
                let resp = GreetResponse {
                    message: format!("Hello, {}!", req.name),
                };
                Ok(z_serialize(&resp))
            })
        })
        .method_fn("add", |query| {
            Box::pin(async move {
                let req: AddRequest = query
                    .payload()
                    .map(z_deserialize)
                    .transpose()
                    .map_err(|_| ServiceError::InvalidRequest {
                        message: "failed to deserialize AddRequest".to_string(),
                    })?
                    .ok_or_else(|| ServiceError::InvalidRequest {
                        message: "missing request payload".to_string(),
                    })?;
                let resp = AddResponse { sum: req.a + req.b };
                Ok(z_serialize(&resp))
            })
        }))
    .expect("server should start");

    // Allow the queryable to propagate
    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = ServiceClient::new(&client_session, &ke, Duration::from_secs(5))
        .expect("client should build");

    // Call "greet"
    let greet_resp: GreetResponse = client
        .call(
            "greet",
            &GreetRequest {
                name: "Zenoh".to_string(),
            },
            None,
        )
        .await
        .expect("greet call should succeed");
    assert_eq!(greet_resp.message, "Hello, Zenoh!");

    // Call "add"
    let add_resp: AddResponse = client
        .call("add", &AddRequest { a: 17, b: 25 }, None)
        .await
        .expect("add call should succeed");
    assert_eq!(add_resp.sum, 42);

    ztimeout!(server_session.close()).unwrap();
    ztimeout!(client_session.close()).unwrap();
}

// ---------------------------------------------------------------------------
// 2. Structured error: server returns ServiceError
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn structured_error_not_found() {
    zenoh_util::init_log_from_env_or("error");

    const ENDPOINT: &str = "tcp/localhost:27501";
    let ke = unique_ke("structured_error");

    let server_session = listening_session(ENDPOINT).await;
    let client_session = connecting_session(ENDPOINT).await;

    let _server = ztimeout!(ServiceServer::builder(&server_session, ke.as_str()).method_fn(
        "find_user",
        |_query| {
            Box::pin(async {
                Err(ServiceError::NotFound {
                    message: "user not found".to_string(),
                })
            })
        },
    ))
    .expect("server should start");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = ServiceClient::new(&client_session, &ke, Duration::from_secs(5))
        .expect("client should build");

    let result = client
        .call_raw("find_user", ZBytes::default(), None)
        .await;

    match result {
        Err(ServiceError::NotFound { message }) => {
            assert_eq!(message, "user not found");
        }
        other => panic!("expected NotFound error, got {other:?}"),
    }

    ztimeout!(server_session.close()).unwrap();
    ztimeout!(client_session.close()).unwrap();
}

// ---------------------------------------------------------------------------
// 3. Method not found
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn method_not_found() {
    zenoh_util::init_log_from_env_or("error");

    const ENDPOINT: &str = "tcp/localhost:27502";
    let ke = unique_ke("method_not_found");

    let server_session = listening_session(ENDPOINT).await;
    let client_session = connecting_session(ENDPOINT).await;

    let _server = ztimeout!(ServiceServer::builder(&server_session, ke.as_str()).method_fn(
        "exists",
        |_query| { Box::pin(async { Ok(ZBytes::default()) }) },
    ))
    .expect("server should start");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = ServiceClient::new(&client_session, &ke, Duration::from_secs(5))
        .expect("client should build");

    let result = client
        .call_raw("nonexistent", ZBytes::default(), None)
        .await;

    match result {
        Err(ServiceError::MethodNotFound { method }) => {
            assert_eq!(method, "nonexistent");
        }
        other => panic!("expected MethodNotFound error, got {other:?}"),
    }

    ztimeout!(server_session.close()).unwrap();
    ztimeout!(client_session.close()).unwrap();
}

// ---------------------------------------------------------------------------
// 4. Invalid request payload — server continues serving after bad request
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_request_no_poison() {
    zenoh_util::init_log_from_env_or("error");

    const ENDPOINT: &str = "tcp/localhost:27503";
    let ke = unique_ke("invalid_request");

    let server_session = listening_session(ENDPOINT).await;
    let client_session = connecting_session(ENDPOINT).await;

    let _server = ztimeout!(ServiceServer::builder(&server_session, ke.as_str()).method_fn(
        "greet",
        |query| {
            Box::pin(async move {
                let req: GreetRequest = query
                    .payload()
                    .map(z_deserialize)
                    .transpose()
                    .map_err(|_| ServiceError::InvalidRequest {
                        message: "failed to deserialize GreetRequest".to_string(),
                    })?
                    .ok_or_else(|| ServiceError::InvalidRequest {
                        message: "missing request payload".to_string(),
                    })?;
                let resp = GreetResponse {
                    message: format!("Hello, {}!", req.name),
                };
                Ok(z_serialize(&resp))
            })
        },
    ))
    .expect("server should start");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = ServiceClient::new(&client_session, &ke, Duration::from_secs(5))
        .expect("client should build");

    // Send garbage bytes — handler should fail with InvalidRequest
    let garbage = ZBytes::from(vec![0xDE, 0xAD, 0xBE, 0xEF]);
    let result = client.call_raw("greet", garbage, None).await;

    match result {
        Err(ServiceError::InvalidRequest { message }) => {
            assert!(
                message.contains("deserialize"),
                "expected deserialization error message, got: {message}"
            );
        }
        other => panic!("expected InvalidRequest error, got {other:?}"),
    }

    // Now send a valid request — server must NOT be poisoned
    let valid_resp: GreetResponse = client
        .call(
            "greet",
            &GreetRequest {
                name: "After-Error".to_string(),
            },
            None,
        )
        .await
        .expect("valid call after bad request should succeed");
    assert_eq!(valid_resp.message, "Hello, After-Error!");

    ztimeout!(server_session.close()).unwrap();
    ztimeout!(client_session.close()).unwrap();
}

// ---------------------------------------------------------------------------
// 5. Deadline exceeded
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deadline_exceeded() {
    zenoh_util::init_log_from_env_or("error");

    const ENDPOINT: &str = "tcp/localhost:27504";
    let ke = unique_ke("deadline_exceeded");

    let server_session = listening_session(ENDPOINT).await;
    let client_session = connecting_session(ENDPOINT).await;

    let _server = ztimeout!(ServiceServer::builder(&server_session, ke.as_str()).method_fn(
        "slow",
        |_query| {
            Box::pin(async {
                // Sleep for 2 seconds — longer than the client timeout
                tokio::time::sleep(Duration::from_secs(2)).await;
                let resp = GreetResponse {
                    message: "too late".to_string(),
                };
                Ok(z_serialize(&resp))
            })
        },
    ))
    .expect("server should start");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = ServiceClient::new(&client_session, &ke, Duration::from_secs(5))
        .expect("client should build");

    // Call with 500ms timeout — server sleeps 2s, so this should fail
    let result = client
        .call_raw(
            "slow",
            ZBytes::default(),
            Some(Duration::from_millis(500)),
        )
        .await;

    // The client timeout fires at 500ms. The server handler sleeps for 2s.
    // The client should receive an error — either:
    // - ServiceError::Internal (from the flume recv timeout / channel close)
    // - ServiceError::DeadlineExceeded (if the server checks deadline before replying)
    // Either is acceptable — the key is that we get an error, not a success.
    assert!(
        result.is_err(),
        "expected error from deadline exceeded, got Ok({:?})",
        result.unwrap().to_bytes()
    );

    ztimeout!(server_session.close()).unwrap();
    ztimeout!(client_session.close()).unwrap();
}

// ---------------------------------------------------------------------------
// 6. Service discovery: available after start, unavailable after drop
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn discovery_available_then_unavailable() {
    zenoh_util::init_log_from_env_or("error");

    const ENDPOINT: &str = "tcp/localhost:27505";
    let ke = unique_ke("discovery");

    let server_session = listening_session(ENDPOINT).await;
    let client_session = connecting_session(ENDPOINT).await;

    let client = ServiceClient::new(&client_session, &ke, Duration::from_secs(5))
        .expect("client should build");

    // Before server starts, should not be available
    tokio::time::sleep(Duration::from_secs(1)).await;
    let available_before = ztimeout!(client.is_available()).expect("is_available should work");
    assert!(
        !available_before,
        "service should NOT be available before server starts"
    );

    // Start the server
    let server = ztimeout!(ServiceServer::builder(&server_session, ke.as_str()).method_fn(
        "ping",
        |_query| { Box::pin(async { Ok(ZBytes::from(b"pong".as_ref())) }) },
    ))
    .expect("server should start");

    // Allow liveliness token to propagate
    tokio::time::sleep(Duration::from_secs(1)).await;

    let available_after_start =
        ztimeout!(client.is_available()).expect("is_available should work");
    assert!(
        available_after_start,
        "service should be available after server starts"
    );

    // Drop server — liveliness token is undeclared
    drop(server);
    tokio::time::sleep(Duration::from_secs(1)).await;

    let available_after_drop =
        ztimeout!(client.is_available()).expect("is_available should work");
    assert!(
        !available_after_drop,
        "service should NOT be available after server is dropped"
    );

    ztimeout!(server_session.close()).unwrap();
    ztimeout!(client_session.close()).unwrap();
}
