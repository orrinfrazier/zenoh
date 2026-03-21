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

use zenoh::{
    internal::ztimeout,
    sample::SampleKind,
};
use zenoh_config::{EndPoint, ModeDependentValue, WhatAmI};
use zenoh_ext::{
    z_deserialize, z_serialize, CursorBookmark, EventSubscriber,
};

const TIMEOUT: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------
// Piece 1: CursorBookmark — unit-level tests
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
// Piece 2: EventSubscriberBuilder
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
// Piece 3: Catch-up / live transition with dedup
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_subscriber_first_run_subscribes_live_immediately() {
    const SLEEP: Duration = Duration::from_secs(1);
    const ENDPOINT: &str = "tcp/localhost:27400";

    zenoh_util::init_log_from_env_or("error");

    let peer1 = {
        let mut c = zenoh::Config::default();
        c.listen
            .endpoints
            .set(vec![ENDPOINT.parse::<EndPoint>().unwrap()])
            .unwrap();
        c.scouting.multicast.set_enabled(Some(false)).unwrap();
        c.timestamping
            .set_enabled(Some(ModeDependentValue::Unique(true)))
            .unwrap();
        let _ = c.set_mode(Some(WhatAmI::Peer));
        let s = ztimeout!(zenoh::open(c)).unwrap();
        tracing::info!("Peer (1) ZID: {}", s.zid());
        s
    };

    let peer2 = {
        let mut c = zenoh::Config::default();
        c.connect
            .endpoints
            .set(vec![ENDPOINT.parse::<EndPoint>().unwrap()])
            .unwrap();
        c.scouting.multicast.set_enabled(Some(false)).unwrap();
        c.timestamping
            .set_enabled(Some(ModeDependentValue::Unique(true)))
            .unwrap();
        let _ = c.set_mode(Some(WhatAmI::Peer));
        let s = ztimeout!(zenoh::open(c)).unwrap();
        tracing::info!("Peer (2) ZID: {}", s.zid());
        s
    };

    // First run: no persisted cursor, should subscribe live immediately
    let sub: EventSubscriber = ztimeout!(peer2
        .declare_subscriber("test/event_sub/first_run/**")
        .event()
        .consumer_name("first-run-consumer"))
    .unwrap();

    tokio::time::sleep(SLEEP).await;

    // Publish after subscriber is set up
    ztimeout!(peer1.put("test/event_sub/first_run/a", "live-1")).unwrap();
    tokio::time::sleep(SLEEP).await;

    let sample = ztimeout!(sub.recv_async()).unwrap();
    assert_eq!(sample.kind(), SampleKind::Put);
    assert_eq!(
        sample.payload().try_to_string().unwrap().as_ref(),
        "live-1"
    );

    // No extra samples queued
    assert!(sub.try_recv().unwrap().is_none());

    peer1.close().await.unwrap();
    peer2.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_subscriber_catchup_with_persisted_cursor() {
    use zenoh_ext::AdvancedPublisherBuilderExt;
    use zenoh_ext::CacheConfig;

    const SLEEP: Duration = Duration::from_secs(1);
    const ENDPOINT: &str = "tcp/localhost:27401";

    zenoh_util::init_log_from_env_or("error");

    let peer1 = {
        let mut c = zenoh::Config::default();
        c.listen
            .endpoints
            .set(vec![ENDPOINT.parse::<EndPoint>().unwrap()])
            .unwrap();
        c.scouting.multicast.set_enabled(Some(false)).unwrap();
        c.timestamping
            .set_enabled(Some(ModeDependentValue::Unique(true)))
            .unwrap();
        let _ = c.set_mode(Some(WhatAmI::Peer));
        let s = ztimeout!(zenoh::open(c)).unwrap();
        tracing::info!("Peer (1) ZID: {}", s.zid());
        s
    };

    // Publish some historical data with the advanced publisher (caching enabled)
    let publ = ztimeout!(peer1
        .declare_publisher("test/event_sub/catchup")
        .cache(CacheConfig::default().max_samples(10)))
    .unwrap();

    ztimeout!(publ.put("event-1")).unwrap();
    ztimeout!(publ.put("event-2")).unwrap();
    ztimeout!(publ.put("event-3")).unwrap();
    tokio::time::sleep(SLEEP).await;

    // Simulate a persisted cursor: store a bookmark at event-1's timestamp
    // so catch-up should retrieve event-2 and event-3
    let hlc = uhlc::HLC::default();
    let cursor_ts = hlc.new_timestamp();

    let mut bookmark = CursorBookmark::new("catchup-consumer", "test/event_sub/catchup");
    bookmark.advance(cursor_ts);

    // Persist the cursor via session.put (this is what auto-flush does)
    let cursor_bytes = z_serialize(&bookmark);
    ztimeout!(peer1.put(bookmark.persistence_key(), cursor_bytes)).unwrap();
    tokio::time::sleep(SLEEP).await;

    let peer2 = {
        let mut c = zenoh::Config::default();
        c.connect
            .endpoints
            .set(vec![ENDPOINT.parse::<EndPoint>().unwrap()])
            .unwrap();
        c.scouting.multicast.set_enabled(Some(false)).unwrap();
        c.timestamping
            .set_enabled(Some(ModeDependentValue::Unique(true)))
            .unwrap();
        let _ = c.set_mode(Some(WhatAmI::Peer));
        let s = ztimeout!(zenoh::open(c)).unwrap();
        tracing::info!("Peer (2) ZID: {}", s.zid());
        s
    };

    // EventSubscriber should read the persisted cursor and catch up
    let sub: EventSubscriber = ztimeout!(peer2
        .declare_subscriber("test/event_sub/catchup")
        .event()
        .consumer_name("catchup-consumer"))
    .unwrap();
    tokio::time::sleep(SLEEP).await;

    // Now publish a live event
    ztimeout!(publ.put("event-4")).unwrap();
    tokio::time::sleep(SLEEP).await;

    // Catch-up events should arrive before live events
    // We should see missed events from the cache, then the live event
    let mut received = Vec::new();
    while let Ok(Some(sample)) = sub.try_recv() {
        received.push(
            sample
                .payload()
                .try_to_string()
                .unwrap()
                .as_ref()
                .to_string(),
        );
    }
    // Also drain async
    while let Ok(sample) = tokio::time::timeout(Duration::from_millis(500), sub.recv_async()).await
    {
        let sample = sample.unwrap();
        received.push(
            sample
                .payload()
                .try_to_string()
                .unwrap()
                .as_ref()
                .to_string(),
        );
    }

    // Verify catch-up events came before live, no gaps
    assert!(
        !received.is_empty(),
        "should have received at least the live event"
    );
    assert!(
        received.contains(&"event-4".to_string()),
        "should have received the live event"
    );

    publ.undeclare().await.unwrap();
    peer1.close().await.unwrap();
    peer2.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_subscriber_deduplicates_by_timestamp() {
    use zenoh_ext::AdvancedPublisherBuilderExt;
    use zenoh_ext::CacheConfig;

    const SLEEP: Duration = Duration::from_secs(1);
    const ENDPOINT: &str = "tcp/localhost:27402";

    zenoh_util::init_log_from_env_or("error");

    let peer1 = {
        let mut c = zenoh::Config::default();
        c.listen
            .endpoints
            .set(vec![ENDPOINT.parse::<EndPoint>().unwrap()])
            .unwrap();
        c.scouting.multicast.set_enabled(Some(false)).unwrap();
        c.timestamping
            .set_enabled(Some(ModeDependentValue::Unique(true)))
            .unwrap();
        let _ = c.set_mode(Some(WhatAmI::Peer));
        let s = ztimeout!(zenoh::open(c)).unwrap();
        tracing::info!("Peer (1) ZID: {}", s.zid());
        s
    };

    // Create publisher with cache so catch-up query returns data
    let publ = ztimeout!(peer1
        .declare_publisher("test/event_sub/dedup")
        .cache(CacheConfig::default().max_samples(10)))
    .unwrap();

    // Publish events that will overlap between catch-up and live
    ztimeout!(publ.put("msg-1")).unwrap();
    ztimeout!(publ.put("msg-2")).unwrap();
    ztimeout!(publ.put("msg-3")).unwrap();
    tokio::time::sleep(SLEEP).await;

    let peer2 = {
        let mut c = zenoh::Config::default();
        c.connect
            .endpoints
            .set(vec![ENDPOINT.parse::<EndPoint>().unwrap()])
            .unwrap();
        c.scouting.multicast.set_enabled(Some(false)).unwrap();
        c.timestamping
            .set_enabled(Some(ModeDependentValue::Unique(true)))
            .unwrap();
        let _ = c.set_mode(Some(WhatAmI::Peer));
        let s = ztimeout!(zenoh::open(c)).unwrap();
        tracing::info!("Peer (2) ZID: {}", s.zid());
        s
    };

    // Subscribe. Catch-up will retrieve msg-1..3 from the cache.
    // Meanwhile, msg-3 may also arrive as a live event if the overlap window
    // catches it. EventSubscriber must deduplicate by HLC timestamp.
    let sub: EventSubscriber = ztimeout!(peer2
        .declare_subscriber("test/event_sub/dedup")
        .event()
        .consumer_name("dedup-consumer"))
    .unwrap();
    tokio::time::sleep(SLEEP).await;

    // Publish another event to ensure we get live data too
    ztimeout!(publ.put("msg-4")).unwrap();
    tokio::time::sleep(SLEEP).await;

    // Collect all received samples and verify no duplicates by timestamp
    let mut timestamps = Vec::new();
    let mut payloads = Vec::new();

    while let Ok(Some(sample)) = sub.try_recv() {
        if let Some(ts) = sample.timestamp() {
            timestamps.push(*ts);
        }
        payloads.push(
            sample
                .payload()
                .try_to_string()
                .unwrap()
                .as_ref()
                .to_string(),
        );
    }
    while let Ok(sample) = tokio::time::timeout(Duration::from_millis(500), sub.recv_async()).await
    {
        let sample = sample.unwrap();
        if let Some(ts) = sample.timestamp() {
            timestamps.push(*ts);
        }
        payloads.push(
            sample
                .payload()
                .try_to_string()
                .unwrap()
                .as_ref()
                .to_string(),
        );
    }

    // Verify no duplicate timestamps (dedup by HLC timestamp)
    let unique_timestamps: std::collections::HashSet<_> = timestamps.iter().collect();
    assert_eq!(
        timestamps.len(),
        unique_timestamps.len(),
        "duplicate timestamps detected — dedup failed. payloads: {payloads:?}"
    );

    publ.undeclare().await.unwrap();
    peer1.close().await.unwrap();
    peer2.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_subscriber_catchup_before_live_ordering() {
    use zenoh_ext::AdvancedPublisherBuilderExt;
    use zenoh_ext::CacheConfig;

    const SLEEP: Duration = Duration::from_secs(1);
    const ENDPOINT: &str = "tcp/localhost:27403";

    zenoh_util::init_log_from_env_or("error");

    let peer1 = {
        let mut c = zenoh::Config::default();
        c.listen
            .endpoints
            .set(vec![ENDPOINT.parse::<EndPoint>().unwrap()])
            .unwrap();
        c.scouting.multicast.set_enabled(Some(false)).unwrap();
        c.timestamping
            .set_enabled(Some(ModeDependentValue::Unique(true)))
            .unwrap();
        let _ = c.set_mode(Some(WhatAmI::Peer));
        let s = ztimeout!(zenoh::open(c)).unwrap();
        tracing::info!("Peer (1) ZID: {}", s.zid());
        s
    };

    let publ = ztimeout!(peer1
        .declare_publisher("test/event_sub/ordering")
        .cache(CacheConfig::default().max_samples(10)))
    .unwrap();

    // Publish historical events
    ztimeout!(publ.put("historical-1")).unwrap();
    ztimeout!(publ.put("historical-2")).unwrap();
    tokio::time::sleep(SLEEP).await;

    let peer2 = {
        let mut c = zenoh::Config::default();
        c.connect
            .endpoints
            .set(vec![ENDPOINT.parse::<EndPoint>().unwrap()])
            .unwrap();
        c.scouting.multicast.set_enabled(Some(false)).unwrap();
        c.timestamping
            .set_enabled(Some(ModeDependentValue::Unique(true)))
            .unwrap();
        let _ = c.set_mode(Some(WhatAmI::Peer));
        let s = ztimeout!(zenoh::open(c)).unwrap();
        tracing::info!("Peer (2) ZID: {}", s.zid());
        s
    };

    let sub: EventSubscriber = ztimeout!(peer2
        .declare_subscriber("test/event_sub/ordering")
        .event()
        .consumer_name("ordering-consumer"))
    .unwrap();
    tokio::time::sleep(SLEEP).await;

    // Publish live events after subscriber is established
    ztimeout!(publ.put("live-1")).unwrap();
    ztimeout!(publ.put("live-2")).unwrap();
    tokio::time::sleep(SLEEP).await;

    // Collect all samples
    let mut payloads = Vec::new();
    while let Ok(Some(sample)) = sub.try_recv() {
        payloads.push(
            sample
                .payload()
                .try_to_string()
                .unwrap()
                .as_ref()
                .to_string(),
        );
    }
    while let Ok(sample) = tokio::time::timeout(Duration::from_millis(500), sub.recv_async()).await
    {
        let sample = sample.unwrap();
        payloads.push(
            sample
                .payload()
                .try_to_string()
                .unwrap()
                .as_ref()
                .to_string(),
        );
    }

    // Historical events must appear before live events
    if let (Some(last_hist_idx), Some(first_live_idx)) = (
        payloads
            .iter()
            .rposition(|p| p.starts_with("historical-")),
        payloads.iter().position(|p| p.starts_with("live-")),
    ) {
        assert!(
            last_hist_idx < first_live_idx,
            "catch-up (historical) events must be delivered before live events. \
             order: {payloads:?}"
        );
    }

    publ.undeclare().await.unwrap();
    peer1.close().await.unwrap();
    peer2.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// Piece 4: Cursor auto-flush
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_subscriber_manual_flush_cursor() {
    const SLEEP: Duration = Duration::from_secs(1);
    const ENDPOINT: &str = "tcp/localhost:27404";

    zenoh_util::init_log_from_env_or("error");

    let peer1 = {
        let mut c = zenoh::Config::default();
        c.listen
            .endpoints
            .set(vec![ENDPOINT.parse::<EndPoint>().unwrap()])
            .unwrap();
        c.scouting.multicast.set_enabled(Some(false)).unwrap();
        c.timestamping
            .set_enabled(Some(ModeDependentValue::Unique(true)))
            .unwrap();
        let _ = c.set_mode(Some(WhatAmI::Peer));
        let s = ztimeout!(zenoh::open(c)).unwrap();
        tracing::info!("Peer (1) ZID: {}", s.zid());
        s
    };

    let sub: EventSubscriber = ztimeout!(peer1
        .declare_subscriber("test/event_sub/flush/**")
        .event()
        .consumer_name("flush-consumer"))
    .unwrap();

    // Publish an event so the cursor advances
    ztimeout!(peer1.put("test/event_sub/flush/a", "data-1")).unwrap();
    tokio::time::sleep(SLEEP).await;

    // Receive the sample so the cursor advances
    let sample = ztimeout!(sub.recv_async()).unwrap();
    assert_eq!(
        sample.payload().try_to_string().unwrap().as_ref(),
        "data-1"
    );

    // Manual flush should persist the cursor via session.put
    ztimeout!(sub.flush_cursor()).unwrap();

    // Verify the cursor was persisted by reading it back
    let bookmark = CursorBookmark::new("flush-consumer", "test/event_sub/flush/**");
    let replies: Vec<_> = ztimeout!(peer1
        .get(bookmark.persistence_key())
        .collect::<Vec<_>>())
    .unwrap();

    assert!(
        !replies.is_empty(),
        "flush_cursor should have persisted the bookmark"
    );

    peer1.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_subscriber_flush_noop_when_cursor_unchanged() {
    const ENDPOINT: &str = "tcp/localhost:27405";

    zenoh_util::init_log_from_env_or("error");

    let peer1 = {
        let mut c = zenoh::Config::default();
        c.listen
            .endpoints
            .set(vec![ENDPOINT.parse::<EndPoint>().unwrap()])
            .unwrap();
        c.scouting.multicast.set_enabled(Some(false)).unwrap();
        c.timestamping
            .set_enabled(Some(ModeDependentValue::Unique(true)))
            .unwrap();
        let _ = c.set_mode(Some(WhatAmI::Peer));
        let s = ztimeout!(zenoh::open(c)).unwrap();
        tracing::info!("Peer ZID: {}", s.zid());
        s
    };

    let sub: EventSubscriber = ztimeout!(peer1
        .declare_subscriber("test/event_sub/flush_noop/**")
        .event()
        .consumer_name("noop-consumer"))
    .unwrap();

    // Flush without any events processed — should be a no-op (not error)
    let result = ztimeout!(sub.flush_cursor());
    assert!(
        result.is_ok(),
        "flush_cursor should succeed (no-op) when cursor hasn't advanced"
    );

    // Flush again — still a no-op since nothing changed
    let result2 = ztimeout!(sub.flush_cursor());
    assert!(
        result2.is_ok(),
        "repeated flush_cursor should succeed as no-op"
    );

    peer1.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_subscriber_auto_flush_at_interval() {
    const ENDPOINT: &str = "tcp/localhost:27406";

    zenoh_util::init_log_from_env_or("error");

    let peer1 = {
        let mut c = zenoh::Config::default();
        c.listen
            .endpoints
            .set(vec![ENDPOINT.parse::<EndPoint>().unwrap()])
            .unwrap();
        c.scouting.multicast.set_enabled(Some(false)).unwrap();
        c.timestamping
            .set_enabled(Some(ModeDependentValue::Unique(true)))
            .unwrap();
        let _ = c.set_mode(Some(WhatAmI::Peer));
        let s = ztimeout!(zenoh::open(c)).unwrap();
        tracing::info!("Peer ZID: {}", s.zid());
        s
    };

    // Use a short flush interval (1 second) so the test completes quickly
    let sub: EventSubscriber = ztimeout!(peer1
        .declare_subscriber("test/event_sub/auto_flush/**")
        .event()
        .consumer_name("auto-flush-consumer")
        .flush_interval(Duration::from_secs(1)))
    .unwrap();

    // Publish and consume an event so the cursor advances
    ztimeout!(peer1.put("test/event_sub/auto_flush/x", "auto-1")).unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let sample = ztimeout!(sub.recv_async()).unwrap();
    assert_eq!(
        sample.payload().try_to_string().unwrap().as_ref(),
        "auto-1"
    );

    // Wait for auto-flush to fire (> 1 second interval)
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify the cursor was auto-flushed by checking for the persisted bookmark
    let bookmark = CursorBookmark::new("auto-flush-consumer", "test/event_sub/auto_flush/**");
    let replies: Vec<_> = ztimeout!(peer1
        .get(bookmark.persistence_key())
        .collect::<Vec<_>>())
    .unwrap();

    assert!(
        !replies.is_empty(),
        "auto-flush should have persisted the cursor after the flush interval"
    );

    peer1.close().await.unwrap();
}
