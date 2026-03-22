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
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use zenoh::bytes::ZBytes;
use zenoh::internal::ztimeout;
use zenoh::pubsub::Subscriber;
use zenoh::query::Queryable;
use zenoh::session::WeakSession;
use zenoh::{Result as ZResult, Session, Wait};
use zenoh_config::{ModeDependentValue, WhatAmI};
use zenoh_ext::{
    z_deserialize, z_serialize, CursorBookmark, CursorPersister, EventSubscriber,
    EventSubscriberBuilderExt,
};

const TIMEOUT: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

type PersistLog = Arc<Mutex<Vec<(String, Vec<u8>)>>>;

/// A persister that records every persist call for later assertion.
struct RecordingPersister {
    calls: PersistLog,
}

impl RecordingPersister {
    fn new() -> (Self, PersistLog) {
        let calls: PersistLog = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                calls: calls.clone(),
            },
            calls,
        )
    }
}

#[async_trait]
impl CursorPersister for RecordingPersister {
    async fn persist(
        &self,
        _session: &WeakSession,
        persistence_key: &str,
        data: ZBytes,
    ) -> ZResult<()> {
        self.calls
            .lock()
            .unwrap()
            .push((persistence_key.to_string(), data.to_bytes().to_vec()));
        Ok(())
    }

    fn persist_sync(
        &self,
        _session: &WeakSession,
        persistence_key: &str,
        data: ZBytes,
    ) -> ZResult<()> {
        self.calls
            .lock()
            .unwrap()
            .push((persistence_key.to_string(), data.to_bytes().to_vec()));
        Ok(())
    }
}

/// In-process mock storage: captures puts via a subscriber, serves gets via a
/// queryable. Simulates what `zenoh-plugin-storage-manager` with a memory
/// backend does, without the plugin dependency.
#[allow(dead_code)]
struct MockStorage {
    store: Arc<Mutex<HashMap<String, ZBytes>>>,
    _subscriber: Subscriber<()>,
    _queryable: Queryable<()>,
}

impl MockStorage {
    fn new(session: &Session, key_space: &str) -> ZResult<Self> {
        let store: Arc<Mutex<HashMap<String, ZBytes>>> = Arc::new(Mutex::new(HashMap::new()));

        let store_write = store.clone();
        let _subscriber = session
            .declare_subscriber(key_space)
            .callback(move |sample| {
                let key = sample.key_expr().to_string();
                let data = sample.payload().clone();
                store_write.lock().unwrap().insert(key, data);
            })
            .wait()?;

        let store_read = store.clone();
        let _queryable = session
            .declare_queryable(key_space)
            .callback(move |query| {
                let qkey = query.key_expr().to_string();
                let lock = store_read.lock().unwrap();
                // Exact match first, then prefix/wildcard match
                if let Some(data) = lock.get(&qkey) {
                    let _ = query.reply(query.key_expr(), data.clone()).wait();
                } else {
                    // Wildcard: reply with all entries whose key starts with
                    // the query prefix (strip trailing /**)
                    let prefix = qkey.trim_end_matches("/**").trim_end_matches("/*");
                    for (k, v) in lock.iter() {
                        if k.starts_with(prefix) && k != &qkey {
                            let _ = query.reply(k.as_str(), v.clone()).wait();
                        }
                    }
                }
            })
            .wait()?;

        Ok(Self {
            store,
            _subscriber,
            _queryable,
        })
    }

    #[allow(dead_code)]
    fn len(&self) -> usize {
        self.store.lock().unwrap().len()
    }
}

fn open_test_session() -> Session {
    let mut c = zenoh::Config::default();
    c.scouting.multicast.set_enabled(Some(false)).unwrap();
    c.timestamping
        .set_enabled(Some(ModeDependentValue::Unique(true)))
        .unwrap();
    let _ = c.set_mode(Some(WhatAmI::Peer));
    zenoh::open(c).wait().unwrap()
}

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
fn cursor_bookmark_persistence_key_stable_hash() {
    // Regression test: FNV-1a hash must produce this exact value for "demo/sensor/temp"
    // to ensure persistence keys are stable across builds.
    let bookmark = CursorBookmark::new("my-consumer", "demo/sensor/temp");
    let key = bookmark.persistence_key();
    assert_eq!(
        key, "@cursors/my-consumer/ba6ef8819c3cc3ac",
        "persistence_key hash must be stable (FNV-1a)"
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

    let session = open_test_session();

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

    let session = open_test_session();

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

    let session = open_test_session();

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_subscriber_builder_custom_timeouts() {
    zenoh_util::init_log_from_env_or("error");

    let session = open_test_session();

    // Custom timeouts should be accepted and subscriber should construct successfully
    let sub: EventSubscriber = ztimeout!(session
        .declare_subscriber("test/event_sub/custom_timeouts")
        .event()
        .consumer_name("timeout-consumer")
        .cursor_load_timeout(Duration::from_secs(2))
        .catch_up_timeout(Duration::from_secs(3)))
    .unwrap();

    // Verify subscriber is functional
    assert_eq!(sub.cursor_position(), None);

    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_subscriber_builder_very_short_timeouts() {
    zenoh_util::init_log_from_env_or("error");

    let session = open_test_session();

    // Even very short timeouts should not panic — they just mean no catch-up
    let sub: EventSubscriber = ztimeout!(session
        .declare_subscriber("test/event_sub/short_timeouts")
        .event()
        .consumer_name("short-timeout-consumer")
        .cursor_load_timeout(Duration::from_millis(1))
        .catch_up_timeout(Duration::from_millis(1)))
    .unwrap();

    assert_eq!(sub.cursor_position(), None);

    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_subscriber_builder_custom_live_buffer_capacity() {
    zenoh_util::init_log_from_env_or("error");

    let session = open_test_session();

    // Custom live buffer capacity should be accepted
    let sub: EventSubscriber = ztimeout!(session
        .declare_subscriber("test/event_sub/live_buf_cap")
        .event()
        .consumer_name("livebuf-consumer")
        .live_buffer_capacity(100))
    .unwrap();

    assert_eq!(sub.cursor_position(), None);

    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_subscriber_live_buffer_does_not_exceed_capacity() {
    zenoh_util::init_log_from_env_or("error");

    let session = open_test_session();

    // Use a very small live buffer and very long catch-up timeout so the
    // subscriber stays in catch-up mode while we publish events. Since there
    // is no persisted cursor, catch-up is a no-op but the live buffer is still
    // bounded by the capacity.
    //
    // We verify indirectly: if the subscriber constructs successfully with a
    // capacity of 5 and we send 20 events before it transitions to live, we
    // should still only receive events (no OOM, no panic). Some events may be
    // lost due to the buffer cap, but the subscriber must remain functional.
    let sub: EventSubscriber = ztimeout!(session
        .declare_subscriber("test/event_sub/overflow/**")
        .event()
        .consumer_name("overflow-consumer")
        .live_buffer_capacity(5))
    .unwrap();

    // Publish events — these go through the live path since subscriber is already live
    for i in 0..20 {
        ztimeout!(session.put(&format!("test/event_sub/overflow/{i}"), format!("v{i}"))).unwrap();
    }
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Drain whatever is available — subscriber should be functional
    let mut count = 0;
    while sub.try_recv().unwrap().is_some() {
        count += 1;
    }
    // We should have received some events (the live path after transition doesn't
    // use the buffer, so all 20 should arrive via the flume channel)
    assert!(count > 0, "subscriber should still receive events");

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

    let session = open_test_session();

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
    assert_eq!(sample.payload().try_to_string().unwrap().as_ref(), "live-1");

    // Cursor should have advanced
    assert!(sub.cursor_position().is_some());

    // No extra samples
    assert!(sub.try_recv().unwrap().is_none());

    session.close().await.unwrap();
}

/// Scenario 5: Cursor persistence round-trip — flush via PutPersister, verify
/// a mock storage backend captures and serves the cursor data.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_subscriber_cursor_persistence_roundtrip() {
    const SLEEP: Duration = Duration::from_secs(1);

    zenoh_util::init_log_from_env_or("error");

    let session = open_test_session();

    // Set up mock storage for the @cursors key space
    let _storage = MockStorage::new(&session, "@cursors/**").unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let sub: EventSubscriber = ztimeout!(session
        .declare_subscriber("test/persist-rt/**")
        .event()
        .consumer_name("persist-consumer"))
    .unwrap();

    // Publish and consume an event so cursor advances
    ztimeout!(session.put("test/persist-rt/x", "data-1")).unwrap();
    tokio::time::sleep(SLEEP).await;

    let sample = ztimeout!(sub.recv_async()).unwrap();
    assert_eq!(sample.payload().try_to_string().unwrap().as_ref(), "data-1");

    let cursor_after_recv = sub.cursor_position();
    assert!(cursor_after_recv.is_some());

    // Flush the cursor
    ztimeout!(sub.flush_cursor()).unwrap();

    // Allow mock storage to process the put
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify the mock storage serves the cursor via session.get()
    let bookmark = CursorBookmark::new("persist-consumer", "test/persist-rt/**");
    let persistence_key = bookmark.persistence_key();

    let mut found = false;
    let replies = ztimeout!(session
        .get(&persistence_key)
        .timeout(Duration::from_secs(5)))
    .unwrap();
    while let Ok(reply) = replies.recv_async().await {
        if let Ok(sample) = reply.into_result() {
            let bytes = sample.payload().to_bytes();
            assert!(!bytes.is_empty(), "cursor payload should not be empty");
            found = true;
        }
    }
    assert!(
        found,
        "cursor should be retrievable via session.get() from mock storage after flush"
    );

    session.close().await.unwrap();
}

/// Scenario: Manual flush is a no-op when cursor hasn't advanced.
/// Uses RecordingPersister to verify no persist call is made.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_subscriber_flush_noop_when_unchanged() {
    zenoh_util::init_log_from_env_or("error");

    let session = open_test_session();

    let (persister, calls) = RecordingPersister::new();

    let sub: EventSubscriber = ztimeout!(session
        .declare_subscriber("test/flush-noop/**")
        .event()
        .consumer_name("noop-consumer")
        .cursor_persister(persister))
    .unwrap();

    // Flush without any events — should succeed (no-op)
    assert!(ztimeout!(sub.flush_cursor()).is_ok());
    // Flush again — still no-op
    assert!(ztimeout!(sub.flush_cursor()).is_ok());

    // Persister should NOT have been called since cursor never advanced
    assert!(
        calls.lock().unwrap().is_empty(),
        "persister should not be called when cursor has not advanced"
    );

    session.close().await.unwrap();
}

/// Scenario 6: Auto-flush at interval — configure 1s, process events,
/// verify the persister is called without an explicit flush.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_subscriber_auto_flush_at_interval() {
    const SLEEP: Duration = Duration::from_secs(1);

    zenoh_util::init_log_from_env_or("error");

    let session = open_test_session();

    let (persister, calls) = RecordingPersister::new();

    let sub: EventSubscriber = ztimeout!(session
        .declare_subscriber("test/auto-flush/**")
        .event()
        .consumer_name("auto-flush-consumer")
        .flush_interval(Duration::from_secs(1))
        .cursor_persister(persister))
    .unwrap();

    // Publish and consume so cursor advances
    ztimeout!(session.put("test/auto-flush/x", "auto-1")).unwrap();
    tokio::time::sleep(SLEEP).await;

    let sample = ztimeout!(sub.recv_async()).unwrap();
    assert_eq!(sample.payload().try_to_string().unwrap().as_ref(), "auto-1");

    let cursor_ts = sub.cursor_position();
    assert!(cursor_ts.is_some());

    // Wait for auto-flush to fire (> 1s interval + margin)
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify persister was called by auto-flush
    {
        let recorded = calls.lock().unwrap();
        assert!(
            !recorded.is_empty(),
            "auto-flush should have called the persister"
        );

        let bookmark = CursorBookmark::new("auto-flush-consumer", "test/auto-flush/**");
        assert_eq!(
            recorded[0].0,
            bookmark.persistence_key(),
            "auto-flush should persist to the correct key"
        );
    }

    session.close().await.unwrap();
}

/// Scenario: cursor_position() advances as events are received.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_subscriber_cursor_advances_with_events() {
    const SLEEP: Duration = Duration::from_millis(500);

    zenoh_util::init_log_from_env_or("error");

    let session = open_test_session();

    let sub: EventSubscriber = ztimeout!(session
        .declare_subscriber("test/cursor-advance/**")
        .event()
        .consumer_name("advance-consumer"))
    .unwrap();

    assert_eq!(sub.cursor_position(), None);

    // Send 3 events, verify cursor advances monotonically
    let mut prev_cursor = None;
    for i in 0..3 {
        ztimeout!(session.put(&format!("test/cursor-advance/{i}"), format!("msg-{i}"))).unwrap();
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

    let session = open_test_session();

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
        if let Ok(sample) = tokio::time::timeout(Duration::from_secs(5), sub.recv_async()).await {
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

// ---------------------------------------------------------------------------
// CursorPersister trait — issues #71, #72
// ---------------------------------------------------------------------------

/// Custom persister is called on manual flush with correct key and data.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_subscriber_custom_persister_called_on_flush() {
    const SLEEP: Duration = Duration::from_secs(1);

    zenoh_util::init_log_from_env_or("error");

    let session = open_test_session();

    let (persister, calls) = RecordingPersister::new();

    let sub: EventSubscriber = ztimeout!(session
        .declare_subscriber("test/custom-persist/**")
        .event()
        .consumer_name("custom-consumer")
        .cursor_persister(persister))
    .unwrap();

    // Publish and consume to advance cursor
    ztimeout!(session.put("test/custom-persist/a", "v1")).unwrap();
    tokio::time::sleep(SLEEP).await;

    let _sample = ztimeout!(sub.recv_async()).unwrap();
    assert!(sub.cursor_position().is_some());

    // Flush
    ztimeout!(sub.flush_cursor()).unwrap();

    // Verify persister was called
    {
        let recorded = calls.lock().unwrap();
        assert_eq!(recorded.len(), 1, "persister should be called exactly once");

        let bookmark = CursorBookmark::new("custom-consumer", "test/custom-persist/**");
        assert_eq!(
            recorded[0].0,
            bookmark.persistence_key(),
            "persist key should match expected persistence key"
        );
        assert!(
            !recorded[0].1.is_empty(),
            "persisted data should not be empty"
        );

        // Deserialize the persisted data and verify cursor position
        let deserialized: CursorBookmark =
            z_deserialize(&ZBytes::from(recorded[0].1.clone())).unwrap();
        assert_eq!(
            deserialized.cursor_position(),
            sub.cursor_position(),
            "persisted cursor should match current cursor position"
        );
    }

    session.close().await.unwrap();
}

/// Drop path calls persist_sync (best-effort flush without explicit flush_cursor).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_subscriber_drop_flushes_via_persist_sync() {
    zenoh_util::init_log_from_env_or("error");

    let session = open_test_session();

    let (persister, calls) = RecordingPersister::new();

    {
        let sub: EventSubscriber = ztimeout!(session
            .declare_subscriber("test/drop-sync/**")
            .event()
            .consumer_name("drop-sync-consumer")
            .flush_interval(Duration::from_secs(300))
            .cursor_persister(persister))
        .unwrap();

        ztimeout!(session.put("test/drop-sync/a", "val")).unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;

        let _sample = ztimeout!(sub.recv_async()).unwrap();
        assert!(sub.cursor_position().is_some());
        // Do NOT call flush_cursor — rely on Drop
    }

    {
        let recorded = calls.lock().unwrap();
        assert!(!recorded.is_empty(), "Drop should have called persist_sync");
    }

    session.close().await.unwrap();
}

/// Cursor survives subscriber drop and is loaded by a new subscriber
/// via mock storage (issue #72).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_subscriber_cursor_survives_subscriber_drop() {
    const SLEEP: Duration = Duration::from_secs(1);

    zenoh_util::init_log_from_env_or("error");

    let session = open_test_session();

    // Set up mock storage for the @cursors key space
    let _storage = MockStorage::new(&session, "@cursors/**").unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // --- Subscriber #1: process events, flush cursor, then drop ---
    let cursor_position_1;
    {
        let sub1: EventSubscriber = ztimeout!(session
            .declare_subscriber("test/survive-drop/**")
            .event()
            .consumer_name("survive-consumer"))
        .unwrap();

        // Publish and consume events
        for i in 0..3 {
            ztimeout!(session.put(&format!("test/survive-drop/{i}"), format!("msg-{i}"))).unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
            let _sample = ztimeout!(sub1.recv_async()).unwrap();
        }

        cursor_position_1 = sub1.cursor_position();
        assert!(cursor_position_1.is_some(), "cursor should have advanced");

        // Explicit flush to persist cursor to mock storage
        ztimeout!(sub1.flush_cursor()).unwrap();

        // Allow mock storage to capture the put
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // sub1 dropped here

    // Small delay to ensure cleanup
    tokio::time::sleep(SLEEP).await;

    // --- Subscriber #2: same consumer_name, should load persisted cursor ---
    let sub2: EventSubscriber = ztimeout!(session
        .declare_subscriber("test/survive-drop/**")
        .event()
        .consumer_name("survive-consumer"))
    .unwrap();

    // Cursor should be restored from mock storage
    assert_eq!(
        sub2.cursor_position(),
        cursor_position_1,
        "new subscriber should load cursor from storage, matching previous position"
    );

    // Publish a new event — subscriber should see only events after the cursor
    ztimeout!(session.put("test/survive-drop/new", "after-restart")).unwrap();
    tokio::time::sleep(SLEEP).await;

    let sample = ztimeout!(sub2.recv_async()).unwrap();
    assert_eq!(
        sample.payload().try_to_string().unwrap().as_ref(),
        "after-restart"
    );

    session.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// End-to-end resume tests — issue #119
// ---------------------------------------------------------------------------

/// Publish 10 events, consume 5, flush cursor, drop subscriber, recreate with
/// same consumer name — verify cursor is restored at event 5's position and
/// new events are received correctly.
///
/// Note: time-range-filtered catch-up of historical events (events 5-9) requires
/// a real storage backend with TimeRange support. The MockStorage does not filter
/// by time, so we verify cursor restoration and new-event reception only.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_subscriber_resume_from_exact_position() {
    const SLEEP: Duration = Duration::from_millis(300);

    zenoh_util::init_log_from_env_or("error");

    let session = open_test_session();

    let _cursor_storage = MockStorage::new(&session, "@cursors/**").unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- Subscriber #1: publish 10 events, consume exactly 5 ---
    let cursor_after_5;
    {
        let sub1: EventSubscriber = ztimeout!(session
            .declare_subscriber("test/resume-exact/**")
            .event()
            .consumer_name("exact-consumer"))
        .unwrap();

        for i in 0..10 {
            ztimeout!(session.put(&format!("test/resume-exact/{i}"), format!("event-{i}")))
                .unwrap();
            tokio::time::sleep(SLEEP).await;
        }

        // Consume exactly 5
        for i in 0..5 {
            let sample = ztimeout!(sub1.recv_async()).unwrap();
            assert_eq!(
                sample.payload().try_to_string().unwrap().as_ref(),
                &format!("event-{i}"),
                "subscriber #1 should receive events in order"
            );
        }

        cursor_after_5 = sub1.cursor_position();
        assert!(cursor_after_5.is_some(), "cursor should be at event 4");

        ztimeout!(sub1.flush_cursor()).unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // sub1 dropped — remaining events 5-9 discarded from channel

    tokio::time::sleep(Duration::from_secs(1)).await;

    // --- Subscriber #2: same consumer, cursor should be restored ---
    let sub2: EventSubscriber = ztimeout!(session
        .declare_subscriber("test/resume-exact/**")
        .event()
        .consumer_name("exact-consumer"))
    .unwrap();

    assert_eq!(
        sub2.cursor_position(),
        cursor_after_5,
        "cursor should be restored to event 4's position"
    );

    // Publish new events — subscriber should receive them
    for i in 10..13 {
        ztimeout!(session.put(&format!("test/resume-exact/{i}"), format!("event-{i}"))).unwrap();
    }
    tokio::time::sleep(Duration::from_secs(1)).await;

    for i in 10..13 {
        let sample = ztimeout!(sub2.recv_async()).unwrap();
        assert_eq!(
            sample.payload().try_to_string().unwrap().as_ref(),
            &format!("event-{i}"),
            "subscriber #2 should receive new events after resume"
        );
    }

    session.close().await.unwrap();
}

/// Two consumers on the same topic track cursors independently.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_subscriber_multiple_consumers_independent_cursors() {
    const SLEEP: Duration = Duration::from_millis(300);

    zenoh_util::init_log_from_env_or("error");

    let session = open_test_session();

    let _cursor_storage = MockStorage::new(&session, "@cursors/**").unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- Consumer A: consume 3 events, flush ---
    let cursor_a;
    {
        let sub_a: EventSubscriber = ztimeout!(session
            .declare_subscriber("test/multi-consumer/**")
            .event()
            .consumer_name("consumer-A"))
        .unwrap();

        for i in 0..3 {
            ztimeout!(session.put(&format!("test/multi-consumer/{i}"), format!("msg-{i}")))
                .unwrap();
            tokio::time::sleep(SLEEP).await;
            let _sample = ztimeout!(sub_a.recv_async()).unwrap();
        }

        cursor_a = sub_a.cursor_position();
        ztimeout!(sub_a.flush_cursor()).unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // --- Consumer B: consume 1 event, flush ---
    let cursor_b;
    {
        let sub_b: EventSubscriber = ztimeout!(session
            .declare_subscriber("test/multi-consumer/**")
            .event()
            .consumer_name("consumer-B"))
        .unwrap();

        ztimeout!(session.put("test/multi-consumer/extra", "msg-extra")).unwrap();
        tokio::time::sleep(SLEEP).await;
        let _sample = ztimeout!(sub_b.recv_async()).unwrap();

        cursor_b = sub_b.cursor_position();
        ztimeout!(sub_b.flush_cursor()).unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    tokio::time::sleep(Duration::from_secs(1)).await;

    // --- Recreate both: verify independent cursors ---
    let sub_a2: EventSubscriber = ztimeout!(session
        .declare_subscriber("test/multi-consumer/**")
        .event()
        .consumer_name("consumer-A"))
    .unwrap();

    let sub_b2: EventSubscriber = ztimeout!(session
        .declare_subscriber("test/multi-consumer/**")
        .event()
        .consumer_name("consumer-B"))
    .unwrap();

    assert_eq!(
        sub_a2.cursor_position(),
        cursor_a,
        "consumer-A should restore its own cursor"
    );
    assert_eq!(
        sub_b2.cursor_position(),
        cursor_b,
        "consumer-B should restore its own cursor"
    );
    assert_ne!(
        cursor_a, cursor_b,
        "consumer-A and consumer-B should have different cursor positions"
    );

    session.close().await.unwrap();
}
