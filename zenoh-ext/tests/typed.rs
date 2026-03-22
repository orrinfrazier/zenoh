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

use std::time::Duration;

use zenoh_ext::{Deserialize, Serialize, ZDeserializeError, ZDeserializer, ZSerializer};

// -- Test payload types --

#[derive(Debug, Clone, PartialEq)]
struct TelemetryPayload {
    device_id: u32,
    temperature: f64,
    label: String,
}

impl Serialize for TelemetryPayload {
    fn serialize(&self, serializer: &mut ZSerializer) {
        serializer.serialize(self.device_id);
        serializer.serialize(self.temperature);
        serializer.serialize(&self.label);
    }
}

impl Deserialize for TelemetryPayload {
    fn deserialize(deserializer: &mut ZDeserializer) -> Result<Self, ZDeserializeError> {
        Ok(Self {
            device_id: deserializer.deserialize()?,
            temperature: deserializer.deserialize()?,
            label: deserializer.deserialize()?,
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_pub_sub_round_trip() {
    use zenoh_ext::TypedSessionExt;

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let subscriber = session
        .declare_typed_subscriber::<TelemetryPayload, _>("test/typed/roundtrip")
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let publisher = session
        .declare_typed_publisher::<TelemetryPayload, _>("test/typed/roundtrip")
        .await
        .unwrap();

    let payload = TelemetryPayload {
        device_id: 42,
        temperature: 23.5,
        label: "sensor-a".to_string(),
    };

    publisher.put(&payload).await.unwrap();

    let received = tokio::time::timeout(Duration::from_secs(5), subscriber.recv_async())
        .await
        .expect("timeout waiting for message")
        .expect("channel closed")
        .expect("deserialization failed");

    assert_eq!(received, payload);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_subscriber_malformed_payload_yields_err() {
    use zenoh_ext::TypedSessionExt;

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let subscriber = session
        .declare_typed_subscriber::<TelemetryPayload, _>("test/typed/malformed")
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish raw garbage bytes using a normal publisher
    let raw_publisher = session
        .declare_publisher("test/typed/malformed")
        .await
        .unwrap();
    raw_publisher.put(vec![0u8, 1, 2]).await.unwrap();

    let received = tokio::time::timeout(Duration::from_secs(5), subscriber.recv_async())
        .await
        .expect("timeout waiting for message")
        .expect("channel closed");

    // Should be an Err, not a panic
    assert!(received.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_publisher_sets_encoding() {
    use zenoh_ext::TypedSessionExt;

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let publisher = session
        .declare_typed_publisher::<TelemetryPayload, _>("test/typed/encoding")
        .await
        .unwrap();

    let encoding = publisher.encoding();
    let encoding_str = format!("{encoding}");
    assert!(
        encoding_str.contains("typed"),
        "Encoding should contain 'typed' marker, got: {encoding_str}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_pub_sub_multiple_messages() {
    use zenoh_ext::TypedSessionExt;

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let subscriber = session
        .declare_typed_subscriber::<TelemetryPayload, _>("test/typed/multi")
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let publisher = session
        .declare_typed_publisher::<TelemetryPayload, _>("test/typed/multi")
        .await
        .unwrap();

    for i in 0..5u32 {
        let payload = TelemetryPayload {
            device_id: i,
            temperature: i as f64 * 1.5,
            label: format!("sensor-{i}"),
        };
        publisher.put(&payload).await.unwrap();
    }

    for i in 0..5u32 {
        let received = tokio::time::timeout(Duration::from_secs(5), subscriber.recv_async())
            .await
            .expect("timeout")
            .expect("channel closed")
            .expect("deserialization failed");

        assert_eq!(received.device_id, i);
        assert_eq!(received.label, format!("sensor-{i}"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_subscriber_blocking_recv() {
    use zenoh_ext::TypedSessionExt;

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let subscriber = session
        .declare_typed_subscriber::<TelemetryPayload, _>("test/typed/blocking")
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let publisher = session
        .declare_typed_publisher::<TelemetryPayload, _>("test/typed/blocking")
        .await
        .unwrap();

    let payload = TelemetryPayload {
        device_id: 99,
        temperature: 36.6,
        label: "blocking-test".to_string(),
    };

    publisher.put(&payload).await.unwrap();

    // Use blocking recv from a spawn_blocking context
    let received = tokio::task::spawn_blocking(move || {
        subscriber
            .recv()
            .expect("channel closed")
            .expect("deserialization failed")
    })
    .await
    .expect("task panicked");

    assert_eq!(received, payload);
}

// -- Query/Reply test types --

#[derive(Debug, Clone, PartialEq)]
struct GetConfigRequest {
    device_id: u32,
}

impl Serialize for GetConfigRequest {
    fn serialize(&self, serializer: &mut ZSerializer) {
        serializer.serialize(self.device_id);
    }
}

impl Deserialize for GetConfigRequest {
    fn deserialize(deserializer: &mut ZDeserializer) -> Result<Self, ZDeserializeError> {
        Ok(Self {
            device_id: deserializer.deserialize()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
struct DeviceConfig {
    device_id: u32,
    hostname: String,
    enabled: bool,
}

impl Serialize for DeviceConfig {
    fn serialize(&self, serializer: &mut ZSerializer) {
        serializer.serialize(self.device_id);
        serializer.serialize(&self.hostname);
        serializer.serialize(self.enabled);
    }
}

impl Deserialize for DeviceConfig {
    fn deserialize(deserializer: &mut ZDeserializer) -> Result<Self, ZDeserializeError> {
        Ok(Self {
            device_id: deserializer.deserialize()?,
            hostname: deserializer.deserialize()?,
            enabled: deserializer.deserialize()?,
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_query_reply_round_trip() {
    use zenoh_ext::TypedSessionExt;

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let queryable = session
        .declare_typed_queryable::<GetConfigRequest, DeviceConfig, _>("test/typed/query/config")
        .await
        .unwrap();

    // Spawn handler
    let handle = tokio::spawn(async move {
        let typed_query = queryable.recv_async().await.unwrap();
        let req = typed_query.request().unwrap();
        assert_eq!(req.device_id, 42);

        let resp = DeviceConfig {
            device_id: req.device_id,
            hostname: "switch-42".to_string(),
            enabled: true,
        };
        typed_query.reply(&resp).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let replies = session
        .typed_get::<GetConfigRequest, DeviceConfig, _>(
            "test/typed/query/config",
            &GetConfigRequest { device_id: 42 },
        )
        .await
        .unwrap();

    assert_eq!(replies.len(), 1);
    let config = replies[0].as_ref().unwrap();
    assert_eq!(config.device_id, 42);
    assert_eq!(config.hostname, "switch-42");
    assert!(config.enabled);

    handle.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_queryable_malformed_request_yields_err() {
    use zenoh_ext::TypedSessionExt;

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let queryable = session
        .declare_typed_queryable::<GetConfigRequest, DeviceConfig, _>(
            "test/typed/query/malformed",
        )
        .await
        .unwrap();

    let handle = tokio::spawn(async move {
        let typed_query = queryable.recv_async().await.unwrap();
        // Request should fail to deserialize
        assert!(typed_query.request().is_err());
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send raw garbage payload via session.get()
    let replies: Vec<_> = session
        .get("test/typed/query/malformed")
        .payload(vec![0u8, 1, 2])
        .await
        .unwrap()
        .into_iter()
        .collect();

    // Queryable won't reply since request parsing failed — replies may be empty
    drop(replies);
    handle.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_get_surfaces_reply_errors() {
    use zenoh_ext::{TypedGetError, TypedSessionExt};

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    // Declare a raw queryable that replies with an error
    let queryable = session
        .declare_queryable("test/typed/query/reply_err")
        .await
        .unwrap();

    let handle = tokio::spawn(async move {
        let query = queryable.recv_async().await.unwrap();
        // Reply with an error payload instead of a success
        query
            .reply_err(zenoh::bytes::ZBytes::from(b"service unavailable".as_ref()))
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let replies = session
        .typed_get::<GetConfigRequest, DeviceConfig, _>(
            "test/typed/query/reply_err",
            &GetConfigRequest { device_id: 1 },
        )
        .await
        .unwrap();

    // The reply error should be surfaced, not silently dropped
    assert_eq!(replies.len(), 1);
    match &replies[0] {
        Err(TypedGetError::ReplyError(payload)) => {
            assert!(!payload.is_empty(), "error payload should not be empty");
        }
        other => panic!("expected TypedGetError::ReplyError, got {other:?}"),
    }

    handle.await.unwrap();
}
