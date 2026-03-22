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

use zenoh_ext::{
    Deserialize, Serialize, TypedSchema, ZDeserializeError, ZDeserializer, ZSerializer,
};

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

impl TypedSchema for TelemetryPayload {
    const SCHEMA_NAME: &'static str = "test.telemetry.v1";
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
async fn typed_publisher_encoding_uses_schema_name() {
    use zenoh_ext::TypedSessionExt;

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let publisher = session
        .declare_typed_publisher::<TelemetryPayload, _>("test/typed/encoding")
        .await
        .unwrap();

    let encoding = publisher.encoding();
    let encoding_str = format!("{encoding}");
    // Encoding must use the user-provided SCHEMA_NAME, not std::any::type_name
    assert_eq!(
        encoding_str, "zenoh-ext/typed:test.telemetry.v1",
        "Encoding should use TypedSchema::SCHEMA_NAME, got: {encoding_str}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_encoding_is_stable_across_builds() {
    use zenoh_ext::TypedSessionExt;

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    // Create two publishers for the same type — encoding must be identical and deterministic
    let pub1 = session
        .declare_typed_publisher::<TelemetryPayload, _>("test/typed/stable1")
        .await
        .unwrap();
    let pub2 = session
        .declare_typed_publisher::<TelemetryPayload, _>("test/typed/stable2")
        .await
        .unwrap();

    let enc1 = format!("{}", pub1.encoding());
    let enc2 = format!("{}", pub2.encoding());

    assert_eq!(enc1, enc2, "Same type must produce identical encoding");

    // Encoding must NOT contain Rust-specific module paths (the old type_name behavior)
    assert!(
        !enc1.contains("::"),
        "Encoding must not contain Rust module paths (::), got: {enc1}"
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
