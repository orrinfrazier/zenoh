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

// -- SchemaVersion tests --

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_pub_sub_with_schema_version_match() {
    use zenoh_ext::TypedSessionExt;

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let subscriber = session
        .declare_typed_subscriber::<TelemetryPayload, _>("test/typed/version/match")
        .schema_version(3)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let publisher = session
        .declare_typed_publisher::<TelemetryPayload, _>("test/typed/version/match")
        .schema_version(3)
        .await
        .unwrap();

    let payload = TelemetryPayload {
        device_id: 1,
        temperature: 20.0,
        label: "versioned".to_string(),
    };
    publisher.put(&payload).await.unwrap();

    let received = tokio::time::timeout(Duration::from_secs(5), subscriber.recv_async())
        .await
        .expect("timeout")
        .expect("channel closed");

    // Version matches — should get Ok
    assert!(received.is_ok());
    assert_eq!(received.unwrap(), payload);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_pub_sub_version_mismatch_yields_err() {
    use zenoh_ext::TypedSessionExt;

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let subscriber = session
        .declare_typed_subscriber::<TelemetryPayload, _>("test/typed/version/mismatch")
        .schema_version(3)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let publisher = session
        .declare_typed_publisher::<TelemetryPayload, _>("test/typed/version/mismatch")
        .schema_version(2) // Different version!
        .await
        .unwrap();

    let payload = TelemetryPayload {
        device_id: 1,
        temperature: 20.0,
        label: "wrong-version".to_string(),
    };
    publisher.put(&payload).await.unwrap();

    let received = tokio::time::timeout(Duration::from_secs(5), subscriber.recv_async())
        .await
        .expect("timeout")
        .expect("channel closed");

    // Version mismatch — should get Err with VersionMismatch
    assert!(received.is_err());
    let err = received.unwrap_err();
    match err {
        zenoh_ext::TypedReceiveError::VersionMismatch { expected, received, .. } => {
            assert_eq!(expected, 3);
            assert_eq!(received, 2);
        }
        other => panic!("Expected VersionMismatch, got: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_sub_no_version_attachment_degrades_gracefully() {
    use zenoh_ext::TypedSessionExt;

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    // Subscriber expects version 3
    let subscriber = session
        .declare_typed_subscriber::<TelemetryPayload, _>("test/typed/version/noattach")
        .schema_version(3)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publisher WITHOUT schema_version (no version attachment)
    let publisher = session
        .declare_typed_publisher::<TelemetryPayload, _>("test/typed/version/noattach")
        .await
        .unwrap();

    let payload = TelemetryPayload {
        device_id: 5,
        temperature: 15.0,
        label: "no-version".to_string(),
    };
    publisher.put(&payload).await.unwrap();

    let received = tokio::time::timeout(Duration::from_secs(5), subscriber.recv_async())
        .await
        .expect("timeout")
        .expect("channel closed");

    // No version attachment — should attempt deserialization and succeed
    assert!(received.is_ok());
    assert_eq!(received.unwrap(), payload);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_publisher_to_untyped_subscriber_backward_compat() {
    use zenoh_ext::TypedSessionExt;

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    // Untyped subscriber receives raw samples
    let subscriber = session
        .declare_subscriber("test/typed/backward/typed2untyped")
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Typed publisher with schema version
    let publisher = session
        .declare_typed_publisher::<TelemetryPayload, _>("test/typed/backward/typed2untyped")
        .schema_version(3)
        .await
        .unwrap();

    let payload = TelemetryPayload {
        device_id: 77,
        temperature: 18.5,
        label: "backward-compat".to_string(),
    };
    publisher.put(&payload).await.unwrap();

    let sample = tokio::time::timeout(Duration::from_secs(5), subscriber.recv_async())
        .await
        .expect("timeout")
        .expect("channel closed");

    // Untyped subscriber gets valid ZBytes — version attachment is ignorable metadata
    assert!(!sample.payload().is_empty());

    // Can manually deserialize
    let deserialized: TelemetryPayload =
        zenoh_ext::z_deserialize(sample.payload()).expect("manual deserialization should work");
    assert_eq!(deserialized, payload);

    // Attachment is present (contains the version)
    assert!(sample.attachment().is_some());
}
