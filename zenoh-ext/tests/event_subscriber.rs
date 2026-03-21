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

use zenoh::internal::ztimeout;
use zenoh_config::{ModeDependentValue, WhatAmI};
use zenoh_ext::{z_deserialize, z_serialize, CursorBookmark, EventSubscriber};

const TIMEOUT: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------
// CursorBookmark — unit-level tests
// ---------------------------------------------------------------------------

#[test]
fn cursor_bookmark_new_has_no_last_processed() {
    let bookmark = CursorBookmark::new("my-consumer", "demo/sensor/**");
    assert_eq!(bookmark.cursor_position(), None);
}

#[test]
fn cursor_bookmark_persistence_key_format() {
    let bookmark = CursorBookmark::new("my-consumer", "demo/sensor/temp");
    let key = bookmark.persistence_key();

    // Must start with the cursor namespace prefix and include the consumer name
    assert!(
        key.starts_with("@cursors/my-consumer/"),
        "persistence_key should start with '@cursors/my-consumer/', got: {key}"
    );

    // The suffix should be a hex-encoded hash of the key_expr string
    let hash_part = key.strip_prefix("@cursors/my-consumer/").unwrap();
    assert!(
        !hash_part.is_empty(),
        "hash portion of persistence_key must not be empty"
    );
    // Verify the hash is valid hex
    assert!(
        hash_part.chars().all(|c| c.is_ascii_hexdigit()),
        "hash portion should be hex-encoded, got: {hash_part}"
    );
}

#[test]
fn cursor_bookmark_persistence_key_deterministic() {
    let b1 = CursorBookmark::new("consumer-a", "demo/test");
    let b2 = CursorBookmark::new("consumer-a", "demo/test");
    assert_eq!(
        b1.persistence_key(),
        b2.persistence_key(),
        "same consumer + key_expr should produce the same persistence_key"
    );
}

#[test]
fn cursor_bookmark_persistence_key_differs_for_different_key_exprs() {
    let b1 = CursorBookmark::new("consumer-a", "demo/sensor/temp");
    let b2 = CursorBookmark::new("consumer-a", "demo/sensor/humidity");
    assert_ne!(
        b1.persistence_key(),
        b2.persistence_key(),
        "different key_exprs should produce different persistence_keys"
    );
}

#[test]
fn cursor_bookmark_persistence_key_differs_for_different_consumers() {
    let b1 = CursorBookmark::new("consumer-a", "demo/test");
    let b2 = CursorBookmark::new("consumer-b", "demo/test");
    assert_ne!(
        b1.persistence_key(),
        b2.persistence_key(),
        "different consumer names should produce different persistence_keys"
    );
}

#[test]
fn cursor_bookmark_advance_sets_timestamp() {
    let hlc = uhlc::HLC::default();
    let ts1 = hlc.new_timestamp();

    let mut bookmark = CursorBookmark::new("consumer", "key/expr");
    assert_eq!(bookmark.cursor_position(), None);

    bookmark.advance(ts1);
    assert_eq!(bookmark.cursor_position(), Some(ts1));
}

#[test]
fn cursor_bookmark_advance_only_moves_forward() {
    let hlc = uhlc::HLC::default();
    let ts1 = hlc.new_timestamp();
    let ts2 = hlc.new_timestamp();

    // ts2 > ts1 because HLC is monotonically increasing
    assert!(ts2 > ts1);

    let mut bookmark = CursorBookmark::new("consumer", "key/expr");
    bookmark.advance(ts2);
    assert_eq!(bookmark.cursor_position(), Some(ts2));

    // Advancing with an older timestamp should be a no-op
    bookmark.advance(ts1);
    assert_eq!(
        bookmark.cursor_position(),
        Some(ts2),
        "advance with older timestamp should not regress the cursor"
    );
}

#[test]
fn cursor_bookmark_advance_with_equal_timestamp_is_noop() {
    let hlc = uhlc::HLC::default();
    let ts = hlc.new_timestamp();

    let mut bookmark = CursorBookmark::new("consumer", "key/expr");
    bookmark.advance(ts);
    bookmark.advance(ts);
    assert_eq!(bookmark.cursor_position(), Some(ts));
}

#[test]
fn cursor_bookmark_advance_successive_timestamps() {
    let hlc = uhlc::HLC::default();

    let mut bookmark = CursorBookmark::new("consumer", "key/expr");
    let mut last_ts = None;

    for _ in 0..10 {
        let ts = hlc.new_timestamp();
        bookmark.advance(ts);
        assert_eq!(bookmark.cursor_position(), Some(ts));
        if let Some(prev) = last_ts {
            assert!(ts > prev, "HLC timestamps should be strictly increasing");
        }
        last_ts = Some(ts);
    }
}

#[test]
fn cursor_bookmark_serialization_roundtrip() {
    let hlc = uhlc::HLC::default();
    let ts = hlc.new_timestamp();

    let mut bookmark = CursorBookmark::new("my-consumer", "demo/sensor/**");
    bookmark.advance(ts);

    let serialized = z_serialize(&bookmark);
    let deserialized: CursorBookmark = z_deserialize(&serialized).unwrap();

    assert_eq!(deserialized.cursor_position(), bookmark.cursor_position());
    assert_eq!(deserialized.persistence_key(), bookmark.persistence_key());
}

#[test]
fn cursor_bookmark_serialization_roundtrip_no_timestamp() {
    let bookmark = CursorBookmark::new("consumer", "key/expr");

    let serialized = z_serialize(&bookmark);
    let deserialized: CursorBookmark = z_deserialize(&serialized).unwrap();

    assert_eq!(deserialized.cursor_position(), None);
    assert_eq!(deserialized.persistence_key(), bookmark.persistence_key());
}

// ---------------------------------------------------------------------------
// EventSubscriberBuilder
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_subscriber_builder_with_consumer_name() {
    zenoh_util::init_log_from_env_or("error");

    let session = {
        let mut c = zenoh::Config::default();
        c.scouting.multicast.set_enabled(Some(false)).unwrap();
        c.timestamping
            .set_enabled(Some(ModeDependentValue::Unique(true)))
            .unwrap();
        let _ = c.set_mode(Some(WhatAmI::Peer));
        ztimeout!(zenoh::open(c)).unwrap()
    };

    // Builder should accept consumer_name configuration
    let _sub: EventSubscriber = ztimeout!(session
        .declare_subscriber("test/event_sub/builder")
        .event()
        .consumer_name("test-consumer"))
    .unwrap();

    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_subscriber_builder_default_flush_interval() {
    zenoh_util::init_log_from_env_or("error");

    let session = {
        let mut c = zenoh::Config::default();
        c.scouting.multicast.set_enabled(Some(false)).unwrap();
        c.timestamping
            .set_enabled(Some(ModeDependentValue::Unique(true)))
            .unwrap();
        let _ = c.set_mode(Some(WhatAmI::Peer));
        ztimeout!(zenoh::open(c)).unwrap()
    };

    // Should work with default flush interval (5 seconds) when only consumer_name is set
    let sub: EventSubscriber = ztimeout!(session
        .declare_subscriber("test/event_sub/default_flush")
        .event()
        .consumer_name("test-consumer"))
    .unwrap();

    // The default flush interval should be 5 seconds
    assert_eq!(sub.flush_interval(), Duration::from_secs(5));

    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_subscriber_builder_custom_flush_interval() {
    zenoh_util::init_log_from_env_or("error");

    let session = {
        let mut c = zenoh::Config::default();
        c.scouting.multicast.set_enabled(Some(false)).unwrap();
        c.timestamping
            .set_enabled(Some(ModeDependentValue::Unique(true)))
            .unwrap();
        let _ = c.set_mode(Some(WhatAmI::Peer));
        ztimeout!(zenoh::open(c)).unwrap()
    };

    let custom_interval = Duration::from_secs(30);
    let sub: EventSubscriber = ztimeout!(session
        .declare_subscriber("test/event_sub/custom_flush")
        .event()
        .consumer_name("test-consumer")
        .flush_interval(custom_interval))
    .unwrap();

    assert_eq!(sub.flush_interval(), custom_interval);

    session.close().await.unwrap();
}

// Integration tests for catch-up, dedup, and auto-flush are in issue #42.
