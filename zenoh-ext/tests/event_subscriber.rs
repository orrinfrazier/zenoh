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

// ---------------------------------------------------------------------------
// Integration tests — issue #42
// ---------------------------------------------------------------------------

/// Scenario 1: Fresh start — no persisted cursor, live events only.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_subscriber_fresh_start_live_only() {
    const SLEEP: Duration = Duration::from_secs(1);

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

    let sub: EventSubscriber = ztimeout!(session
        .declare_subscriber("test/fresh-start/**")
        .event()
        .consumer_name("fresh-consumer"))
    .unwrap();

    // No cursor on first run
    assert_eq!(sub.cursor_position(), None);

    // Publish after subscriber is up
    ztimeout!(session.put("test/fresh-start/a", "live-1")).unwrap();
    tokio::time::sleep(SLEEP).await;

    let sample = ztimeout!(sub.recv_async()).unwrap();
    assert_eq!(
        sample.payload().try_to_string().unwrap().as_ref(),
        "live-1"
    );

    // Cursor should have advanced
    assert!(sub.cursor_position().is_some());

    // No extra samples
    assert!(sub.try_recv().unwrap().is_none());

    session.close().await.unwrap();
}

/// Scenario 5: Cursor persistence round-trip — flush, then verify the
/// queryable serves the cursor via session.get().
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_subscriber_cursor_persistence_roundtrip() {
    const SLEEP: Duration = Duration::from_secs(1);

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

    let sub: EventSubscriber = ztimeout!(session
        .declare_subscriber("test/persist-rt/**")
        .event()
        .consumer_name("persist-consumer"))
    .unwrap();

    // Publish and consume an event so cursor advances
    ztimeout!(session.put("test/persist-rt/x", "data-1")).unwrap();
    tokio::time::sleep(SLEEP).await;

    let sample = ztimeout!(sub.recv_async()).unwrap();
    assert_eq!(
        sample.payload().try_to_string().unwrap().as_ref(),
        "data-1"
    );

    let cursor_after_recv = sub.cursor_position();
    assert!(cursor_after_recv.is_some());

    // Flush the cursor
    ztimeout!(sub.flush_cursor()).unwrap();

    // Verify the queryable serves the cursor via session.get()
    let bookmark = CursorBookmark::new("persist-consumer", "test/persist-rt/**");
    let persistence_key = bookmark.persistence_key();

    let mut found = false;
    let replies = ztimeout!(session.get(&persistence_key).timeout(Duration::from_secs(5))).unwrap();
    while let Ok(reply) = replies.recv_async().await {
        if let Ok(sample) = reply.into_result() {
            let bytes = sample.payload().to_bytes();
            assert!(!bytes.is_empty(), "cursor payload should not be empty");
            found = true;
        }
    }
    assert!(found, "cursor should be retrievable via session.get() after flush");

    session.close().await.unwrap();
}

/// Scenario: Manual flush is a no-op when cursor hasn't advanced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_subscriber_flush_noop_when_unchanged() {
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

    let sub: EventSubscriber = ztimeout!(session
        .declare_subscriber("test/flush-noop/**")
        .event()
        .consumer_name("noop-consumer"))
    .unwrap();

    // Flush without any events — should succeed (no-op)
    assert!(ztimeout!(sub.flush_cursor()).is_ok());
    // Flush again — still no-op
    assert!(ztimeout!(sub.flush_cursor()).is_ok());

    // Queryable should NOT serve data since nothing was flushed
    let bookmark = CursorBookmark::new("noop-consumer", "test/flush-noop/**");
    let mut got_reply = false;
    let replies = ztimeout!(session
        .get(&bookmark.persistence_key())
        .timeout(Duration::from_secs(2)))
    .unwrap();
    while let Ok(reply) = replies.recv_async().await {
        if reply.into_result().is_ok() {
            got_reply = true;
        }
    }
    assert!(!got_reply, "queryable should not serve data when cursor was never set");

    session.close().await.unwrap();
}

/// Scenario 6: Auto-flush at interval — configure 1s, process events,
/// verify cursor was persisted without explicit flush.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_subscriber_auto_flush_at_interval() {
    const SLEEP: Duration = Duration::from_secs(1);

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

    let sub: EventSubscriber = ztimeout!(session
        .declare_subscriber("test/auto-flush/**")
        .event()
        .consumer_name("auto-flush-consumer")
        .flush_interval(Duration::from_secs(1)))
    .unwrap();

    // Publish and consume so cursor advances
    ztimeout!(session.put("test/auto-flush/x", "auto-1")).unwrap();
    tokio::time::sleep(SLEEP).await;

    let sample = ztimeout!(sub.recv_async()).unwrap();
    assert_eq!(
        sample.payload().try_to_string().unwrap().as_ref(),
        "auto-1"
    );

    let cursor_ts = sub.cursor_position();
    assert!(cursor_ts.is_some());

    // Wait for auto-flush to fire (> 1s interval + margin)
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify cursor was auto-flushed via the queryable
    let bookmark = CursorBookmark::new("auto-flush-consumer", "test/auto-flush/**");
    let mut found = false;
    let replies = ztimeout!(session
        .get(&bookmark.persistence_key())
        .timeout(Duration::from_secs(5)))
    .unwrap();
    while let Ok(reply) = replies.recv_async().await {
        if let Ok(sample) = reply.into_result() {
            let bytes = sample.payload().to_bytes();
            assert!(!bytes.is_empty(), "auto-flushed cursor payload should not be empty");
            found = true;
        }
    }
    assert!(found, "auto-flush should have persisted the cursor");

    session.close().await.unwrap();
}

/// Scenario: cursor_position() advances as events are received.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_subscriber_cursor_advances_with_events() {
    const SLEEP: Duration = Duration::from_millis(500);

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

    let sub: EventSubscriber = ztimeout!(session
        .declare_subscriber("test/cursor-advance/**")
        .event()
        .consumer_name("advance-consumer"))
    .unwrap();

    assert_eq!(sub.cursor_position(), None);

    // Send 3 events, verify cursor advances monotonically
    let mut prev_cursor = None;
    for i in 0..3 {
        ztimeout!(session.put(
            &format!("test/cursor-advance/{i}"),
            format!("msg-{i}")
        ))
        .unwrap();
        tokio::time::sleep(SLEEP).await;

        let sample = ztimeout!(sub.recv_async()).unwrap();
        assert_eq!(
            sample.payload().try_to_string().unwrap().as_ref(),
            &format!("msg-{i}")
        );

        let cursor = sub.cursor_position();
        assert!(cursor.is_some());
        if let Some(prev) = prev_cursor {
            assert!(
                cursor.unwrap() > prev,
                "cursor should advance monotonically"
            );
        }
        prev_cursor = cursor;
    }

    session.close().await.unwrap();
}

/// Scenario 4: Dedup — events received by the live subscriber before
/// catch-up completes should not be duplicated.
/// Since catch-up only fires when a cursor exists, this tests the
/// transition_to_live dedup path by ensuring no duplicate timestamps
/// appear in the output.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_subscriber_no_duplicate_timestamps() {
    const SLEEP: Duration = Duration::from_millis(500);

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

    let sub: EventSubscriber = ztimeout!(session
        .declare_subscriber("test/dedup/**")
        .event()
        .consumer_name("dedup-consumer"))
    .unwrap();

    // Publish several events
    for i in 0..5 {
        ztimeout!(session.put(&format!("test/dedup/{i}"), format!("val-{i}"))).unwrap();
    }
    tokio::time::sleep(SLEEP).await;

    // Collect all received samples
    let mut timestamps = Vec::new();
    for _ in 0..5 {
        if let Ok(sample) =
            tokio::time::timeout(Duration::from_secs(5), sub.recv_async()).await
        {
            let sample = sample.unwrap();
            if let Some(ts) = sample.timestamp() {
                timestamps.push(*ts);
            }
        }
    }

    // Verify no duplicate timestamps
    let unique: std::collections::HashSet<_> = timestamps.iter().collect();
    assert_eq!(
        timestamps.len(),
        unique.len(),
        "should have no duplicate timestamps"
    );
    assert!(!timestamps.is_empty(), "should have received events");

    session.close().await.unwrap();
}
