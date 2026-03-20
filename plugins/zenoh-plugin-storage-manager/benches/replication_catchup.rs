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

//! Replication catch-up benchmark for unique-key event patterns.
//!
//! Measures the time for a newly-connected replica to sync N unique event keys
//! from an existing replica via the alignment protocol.
//!
//! Scale points: 100, 1_000, 10_000, 100_000 event keys.
//!
//! ## Architecture
//!
//! Two in-process zenoh peers with replication-enabled memory storage:
//!   - Peer A: has N events pre-loaded (listener)
//!   - Peer B: starts empty, connects to A (connector)
//!   - Measures: time from Peer B start until B's local storage has all N events
//!
//! To isolate Peer B's storage count, we query from Peer A with `Locality::Remote`,
//! which routes only to the remote (Peer B's) storage.
//!
//! ## Findings (baseline on Apple M3 Max, macOS, in-process TCP loopback)
//!
//! The initial alignment path (`AlignmentQuery::All`) is extremely efficient for the
//! "Peer B starts empty" scenario — Peer B discovers Peer A, then requests ALL content
//! in a single query. The alignment protocol doesn't need to walk the digest hierarchy
//! (Intervals → SubIntervals → Events) because initial alignment bypasses it entirely.
//!
//! | N       | Net sync time | Throughput     |
//! |---------|---------------|----------------|
//! | 100     | < 3ms         | ~35K events/s  |
//! | 1,000   | < 3ms         | ~200K events/s |
//! | 10,000  | ~10ms         | ~580K events/s |
//! | 100,000 | ~136ms        | ~700K events/s |
//!
//! The bottleneck for the *incremental* alignment path (digest diff → retrieve) is
//! expected to be different and slower — each event requires a separate
//! `AlignmentQuery::Events` round-trip. This benchmark does NOT yet exercise that path
//! because Peer B starts empty and uses the `AlignmentQuery::All` fast path.
//!
//! ## Key observations
//!
//! 1. **Bloom filter (5MB, 4M capacity, 1% FP)**: No observed pressure at 100K keys.
//!    The bloom filter's 4,194,304 item capacity is well above our test range.
//!    At 100K keys, expected false positive rate is negligible. (log.rs:346 TODO)
//!
//! 2. **Cold era single-fingerprint**: Not exercised in initial alignment. Would only
//!    matter for incremental sync when Peer B already has some data and eras differ.
//!
//! 3. **Sequential event retrieval**: The `AlignmentQuery::Events` path sends one query
//!    per event. For incremental catch-up of 10K events, this would mean 10K sequential
//!    round-trips. Batch retrieval per-subinterval would reduce to ~N/batch_size queries.
//!    This path is NOT exercised by initial alignment.
//!
//! ## Incremental sync benchmark
//!
//! Exercises the digest diff → interval/subinterval fingerprint comparison →
//! `AlignmentQuery::Events` retrieval path. Both peers start independently (no
//! connection), each populates its own storage, then they are dynamically
//! connected via `Runtime::connect_peer()`. This forces the digest subscriber
//! to detect the diff and use the incremental alignment protocol.
//!
//! Uses a fast `propagation_delay` (50ms) to reduce detection latency.
//!
//! ### Measurement approach
//!
//! `Locality::Remote` includes same-runtime queryables (storage plugin session),
//! so `count_remote_entries` returns both peers' events after connection. To
//! track Peer B's alignment progress, we use `ConsolidationMode::None` to get
//! raw reply counts: Peer A always contributes N replies, Peer B grows from 1
//! (seed) to N+1. Target = 2N+1 unconsolidated replies.
//!
//! ### Findings
//!
//! The `AlignmentQuery::Events` path sends all requested events as replies to a
//! **single query** — not one query per event. The sequential overhead is
//! serialization within a single query/reply exchange, similar to the `All` path.
//! The real overhead of the incremental path vs initial alignment is the hierarchy
//! walking (Diff → Intervals → SubIntervals → Events), which adds a few
//! round-trips (~10-50ms each).
//!
//! ## Next steps
//!
//! - Measure memory usage during alignment (bloom filter + log + in-flight events)

use std::time::{Duration, Instant};

use tokio::runtime::Runtime;
use zenoh::{
    internal::{runtime::Runtime as ZenohRuntime, zasync_executor_init},
    sample::Locality,
    config::Locator,
    query::ConsolidationMode,
    Config, Session,
};
use zenoh_plugin_trait::Plugin;

const SCALE_POINTS: &[usize] = &[100, 1_000, 10_000, 100_000];

/// Port base — each scale point uses a unique port to avoid conflicts.
const PORT_BASE: u16 = 27_600;

fn storage_config_json(key_expr: &str) -> String {
    format!(
        r#"{{
            storages: {{
                bench: {{
                    key_expr: "{key_expr}",
                    volume: {{
                        id: "memory"
                    }},
                    replication: {{
                        interval: 1,
                        sub_intervals: 5,
                        hot: 6,
                        warm: 30
                    }}
                }}
            }}
        }}"#
    )
}

/// Same as `storage_config_json` but with a fast `propagation_delay` (50ms vs default 250ms).
///
/// Both peers MUST use identical config — `Configuration::fingerprint` hashes all fields,
/// so mismatched configs cause `Digest::diff` to return `None`.
fn storage_config_json_fast(key_expr: &str) -> String {
    format!(
        r#"{{
            storages: {{
                bench: {{
                    key_expr: "{key_expr}",
                    volume: {{
                        id: "memory"
                    }},
                    replication: {{
                        interval: 1,
                        sub_intervals: 5,
                        hot: 6,
                        warm: 30,
                        propagation_delay: 50
                    }}
                }}
            }}
        }}"#
    )
}

fn timestamping_config_json() -> &'static str {
    r#"{
        enabled: {
            router: true,
            peer: true,
            client: true
        }
    }"#
}

/// Creates and opens a zenoh runtime with the storage manager plugin.
///
/// Calls `runtime.start()` to activate TCP listeners/connectors. `RuntimeBuilder::build()`
/// creates the runtime but does NOT start networking — `zenoh::open()` does this
/// automatically, but we need it explicit here for plugin access via `DynamicRuntime`.
async fn start_peer_with_storage(
    listen_endpoint: Option<&str>,
    connect_endpoint: Option<&str>,
    config_json: &str,
) -> (Session, ZenohRuntime, Box<dyn std::any::Any + Send>) {
    let mut config = Config::default();

    config
        .insert_json5("plugins/storage-manager", config_json)
        .expect("Failed to set storage-manager config");

    config
        .insert_json5("timestamping", timestamping_config_json())
        .expect("Failed to set timestamping config");

    config
        .insert_json5("scouting/multicast/enabled", "false")
        .expect("Failed to disable multicast scouting");

    // Reduce scouting delay so that the initial alignment (which fires when the
    // replication log is empty) starts sooner when no peers are connected.
    config
        .insert_json5("scouting/delay", "0")
        .expect("Failed to set scouting delay");

    if let Some(ep) = listen_endpoint {
        config
            .insert_json5("listen/endpoints", &format!(r#"["{ep}"]"#))
            .expect("Failed to set listen endpoint");
    }

    if let Some(ep) = connect_endpoint {
        config
            .insert_json5("connect/endpoints", &format!(r#"["{ep}"]"#))
            .expect("Failed to set connect endpoint");
    }

    let mut runtime = zenoh::internal::runtime::RuntimeBuilder::new(config)
        .build()
        .await
        .expect("Failed to build runtime");

    runtime.start().await.expect("Failed to start runtime");

    let zenoh_runtime = runtime.clone();
    let runtime = runtime.into();

    let plugin =
        zenoh_plugin_storage_manager::StoragesPlugin::start("storage-manager", &runtime)
            .expect("Failed to start storage manager plugin");

    let session = zenoh::session::init(runtime)
        .await
        .expect("Failed to init session");

    (session, zenoh_runtime, Box::new(plugin))
}

/// Inserts N unique event keys into the session.
async fn insert_events(session: &Session, n: usize, prefix: &str) {
    for i in 0..n {
        let key = format!("{prefix}/{i:06}");
        session
            .put(&key, format!("v{i}"))
            .await
            .expect("Failed to put event");
    }
}

/// Counts entries in the REMOTE peer's storage by querying with `Locality::Remote`.
/// From Peer A's perspective, this only hits Peer B's storage (and vice versa).
async fn count_remote_entries(session: &Session, key_expr: &str) -> usize {
    let replies: Vec<_> = session
        .get(key_expr)
        .allowed_destination(Locality::Remote)
        .await
        .expect("Failed to get")
        .into_iter()
        .collect();

    replies
        .into_iter()
        .filter(|r| r.result().is_ok())
        .count()
}

/// Counts entries visible from any source (local + remote storages).
async fn count_entries_any(session: &Session, key_expr: &str) -> usize {
    let replies: Vec<_> = session
        .get(key_expr)
        .await
        .expect("Failed to get")
        .into_iter()
        .collect();

    replies
        .into_iter()
        .filter(|r| r.result().is_ok())
        .count()
}

/// Counts ALL replies (no consolidation) from non-SessionLocal queryables.
///
/// With `ConsolidationMode::None`, each queryable's replies are counted separately.
/// After two peers are connected, this returns the SUM of both peers' event counts
/// (not the union). This allows tracking Peer B's alignment progress: before
/// alignment the total is N (from A) + 1 (from B's seed); after alignment it's
/// N (from A) + N+1 (from B) = 2N+1.
async fn count_all_remote_replies(session: &Session, key_expr: &str) -> usize {
    let replies: Vec<_> = session
        .get(key_expr)
        .consolidation(ConsolidationMode::None)
        .allowed_destination(Locality::Remote)
        .await
        .expect("Failed to get")
        .into_iter()
        .collect();

    replies
        .into_iter()
        .filter(|r| r.result().is_ok())
        .count()
}

/// Polls Peer B's storage via `Locality::Remote` from Peer A until `expected` entries appear.
async fn wait_for_sync(
    session_a: &Session,
    key_expr: &str,
    expected: usize,
    start: Instant,
    timeout: Duration,
    poll_interval: Duration,
) -> (Duration, usize) {
    loop {
        let elapsed = start.elapsed();
        if elapsed > timeout {
            let count = count_remote_entries(session_a, key_expr).await;
            return (elapsed, count);
        }

        let count = count_remote_entries(session_a, key_expr).await;
        if count >= expected {
            return (start.elapsed(), count);
        }

        tokio::time::sleep(poll_interval).await;
    }
}

/// Polls using unconsolidated reply count until `expected` total replies appear.
async fn wait_for_unconsolidated_sync(
    session: &Session,
    key_expr: &str,
    expected: usize,
    start: Instant,
    timeout: Duration,
    poll_interval: Duration,
) -> (Duration, usize) {
    loop {
        let elapsed = start.elapsed();
        if elapsed > timeout {
            let count = count_all_remote_replies(session, key_expr).await;
            return (elapsed, count);
        }

        let count = count_all_remote_replies(session, key_expr).await;
        if count >= expected {
            return (start.elapsed(), count);
        }

        tokio::time::sleep(poll_interval).await;
    }
}

/// Runs the benchmark for a given scale point.
async fn bench_catchup(n: usize, port: u16) {
    let key_expr = "bench/repl/**";
    let prefix = "bench/repl";
    let endpoint = format!("tcp/127.0.0.1:{port}");
    let config_json = storage_config_json(key_expr);

    println!("\n{}", "=".repeat(60));
    println!("  Replication catch-up benchmark: N = {n}");
    println!("{}", "=".repeat(60));

    // --- Phase 1: Start Peer A (listener) and insert events ---
    let phase1_start = Instant::now();
    let (session_a, _runtime_a, _plugin_a) = start_peer_with_storage(
        Some(&endpoint),
        None,
        &config_json,
    )
    .await;

    tokio::time::sleep(Duration::from_secs(1)).await;

    println!("  Peer A ZID: {}", session_a.zid());

    println!("  Inserting {n} events into Peer A...");
    let insert_start = Instant::now();
    insert_events(&session_a, n, prefix).await;
    let insert_elapsed = insert_start.elapsed();
    println!(
        "  Insertion: {:.2}s ({:.0} events/s)",
        insert_elapsed.as_secs_f64(),
        n as f64 / insert_elapsed.as_secs_f64()
    );

    // Wait for storage to process all events.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify Peer A has all events.
    let a_count = count_entries_any(&session_a, key_expr).await;
    println!("  Peer A storage count: {a_count} / {n}");
    assert!(
        a_count >= n,
        "Peer A storage should have all {n} events but only has {a_count}"
    );
    let phase1_elapsed = phase1_start.elapsed();
    println!("  Phase 1 (setup + insert): {:.2}s", phase1_elapsed.as_secs_f64());

    // --- Phase 2: Start Peer B (connector) and measure sync ---
    // Timer starts when Peer B begins startup (includes connection + alignment).
    println!("  Starting Peer B...");
    let sync_start = Instant::now();

    let (session_b, _runtime_b, _plugin_b) = start_peer_with_storage(
        None,
        Some(&endpoint),
        &config_json,
    )
    .await;

    println!("  Peer B ZID: {}", session_b.zid());

    // Brief pause for TCP transport to establish.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let peers_b: Vec<_> = session_b.info().peers_zid().await.collect();
    let peers_a: Vec<_> = session_a.info().peers_zid().await.collect();
    println!("  Connectivity: A sees {} peers, B sees {} peers", peers_a.len(), peers_b.len());
    assert!(
        !peers_b.is_empty() && !peers_a.is_empty(),
        "Peers must be connected for replication to work"
    );

    println!("  Measuring sync...");

    let timeout = match n {
        0..=1_000 => Duration::from_secs(60),
        1_001..=10_000 => Duration::from_secs(300),
        _ => Duration::from_secs(600),
    };

    let (sync_elapsed, final_count) =
        wait_for_sync(&session_a, key_expr, n, sync_start, timeout, Duration::from_millis(250)).await;

    let synced = final_count >= n;
    let throughput = if sync_elapsed.as_secs_f64() > 0.0 {
        final_count as f64 / sync_elapsed.as_secs_f64()
    } else {
        f64::INFINITY
    };

    // --- Results ---
    println!("\n  Results:");
    println!("  --------");
    println!("  Events:       {n}");
    println!(
        "  Synced:       {final_count} / {n} ({})",
        if synced { "COMPLETE" } else { "INCOMPLETE" }
    );
    println!("  Sync time:    {:.3}s (includes connection + initial alignment)", sync_elapsed.as_secs_f64());
    println!("  Throughput:   {:.0} events/s", throughput);

    if n > 0 {
        let intervals_approx = insert_elapsed.as_secs_f64() / 1.0;
        println!(
            "  Insert span:  ~{:.0} intervals (1s each)",
            intervals_approx.max(1.0)
        );
    }

    // Cleanup.
    drop(_plugin_b);
    drop(_plugin_a);
    drop(session_b);
    drop(session_a);
    tokio::time::sleep(Duration::from_millis(500)).await;
}

const INCREMENTAL_SCALE_POINTS: &[usize] = &[100, 1_000, 10_000];

/// Runs the incremental sync benchmark by starting both peers independently (no
/// connection), populating them separately, then dynamically connecting them.
///
/// Peer B gets a single seed event so its replication log is non-empty. Peer A
/// gets N events. When connected, Peer B's digest subscriber detects the diff
/// and exercises the full incremental alignment path: `AlignmentQuery::Diff` →
/// `Intervals` → `SubIntervals` → `Events` → sequential `Retrieval`.
async fn bench_incremental_sync(n: usize, port_a: u16, port_b: u16) {
    let key_expr = "bench/incr/**";
    let prefix = "bench/incr";
    let endpoint_a = format!("tcp/127.0.0.1:{port_a}");
    let endpoint_b = format!("tcp/127.0.0.1:{port_b}");
    let config_json = storage_config_json_fast(key_expr);
    let poll_interval = Duration::from_millis(100);
    // With ConsolidationMode::None, both queryables contribute separate replies.
    // Peer A always contributes N replies. Peer B starts at 1 (seed) and grows
    // to N+1 (seed + N aligned events). Target = N + (N+1) = 2N+1.
    let unconsolidated_target = 2 * n + 1;

    println!("\n{}", "=".repeat(60));
    println!("  Incremental sync benchmark: N = {}", n);
    println!("{}", "=".repeat(60));

    // --- Phase 1: Start Peer A (listener, no connections), insert N events ---
    let (session_a, _runtime_a, _plugin_a) =
        start_peer_with_storage(Some(&endpoint_a), None, &config_json).await;

    tokio::time::sleep(Duration::from_secs(1)).await;
    println!("  Peer A ZID: {}", session_a.zid());

    println!("  Inserting {} events into Peer A...", n);
    insert_events(&session_a, n, prefix).await;

    tokio::time::sleep(Duration::from_secs(2)).await;

    let a_count = count_entries_any(&session_a, key_expr).await;
    println!("  Peer A event count: {} / {}", a_count, n);
    assert!(
        a_count >= n,
        "Peer A should have all {n} events but only has {a_count}"
    );

    // --- Phase 2: Start Peer B (listener, no connections), insert 1 seed event ---
    // The seed ensures Peer B's replication log is non-empty, so it does NOT use
    // the AlignmentQuery::All fast path when it later connects to Peer A.
    let (session_b, runtime_b, _plugin_b) =
        start_peer_with_storage(Some(&endpoint_b), None, &config_json).await;

    tokio::time::sleep(Duration::from_secs(1)).await;
    println!("  Peer B ZID: {}", session_b.zid());

    session_b
        .put(format!("{prefix}/seed"), "seed")
        .await
        .expect("Failed to put seed event");

    tokio::time::sleep(Duration::from_secs(2)).await;

    let b_count = count_entries_any(&session_b, key_expr).await;
    println!("  Peer B seed count: {} / 1", b_count);
    assert!(b_count >= 1, "Peer B should have the seed event");

    // --- Phase 3: Wait for initial alignments to complete, then connect ---
    // Both peers started with empty replication logs, triggering initial_alignment()
    // which sends an AlignmentQuery::Discovery with the default query timeout (10s).
    // We must wait for this query to expire so that when we connect, the digest
    // subscriber handles alignment via the incremental diff path instead.
    println!("  Waiting for initial alignment queries to expire (12s)...");
    tokio::time::sleep(Duration::from_secs(12)).await;

    println!("  Connecting Peer B to Peer A...");
    let locator_a: Locator = endpoint_a
        .parse()
        .expect("Failed to parse Peer A locator");
    let sync_start = Instant::now();
    let connected = runtime_b
        .connect_peer(&session_a.zid().into(), &[locator_a])
        .await;
    assert!(connected, "Failed to connect Peer B to Peer A");
    println!(
        "  Transport established in {:.3}s",
        sync_start.elapsed().as_secs_f64()
    );

    // Verify connectivity.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let peers_b: Vec<_> = session_b.info().peers_zid().await.collect();
    println!("  Peer B sees {} peers", peers_b.len());

    // --- Phase 4: Wait for incremental sync ---
    // Use unconsolidated reply counts so we can distinguish Peer B's progress
    // from Peer A's constant contribution.
    let sync_timeout = match n {
        0..=1_000 => Duration::from_secs(60),
        1_001..=10_000 => Duration::from_secs(300),
        _ => Duration::from_secs(600),
    };

    println!("  Measuring incremental sync (target: {} unconsolidated replies)...", unconsolidated_target);

    let (sync_elapsed, final_count) = wait_for_unconsolidated_sync(
        &session_a,
        key_expr,
        unconsolidated_target,
        sync_start,
        sync_timeout,
        poll_interval,
    )
    .await;

    let synced = final_count >= unconsolidated_target;
    let total_time = sync_elapsed.as_secs_f64();

    // Peer B's events = final_count - N (Peer A's constant contribution).
    let peer_b_events = final_count.saturating_sub(n);

    // Detection floor: ~1 interval (1s) + propagation_delay (50ms) + random jitter.
    let detection_floor = 1.05;
    let estimated_alignment = (total_time - detection_floor).max(0.0);
    let throughput = if estimated_alignment > 0.0 {
        n as f64 / estimated_alignment
    } else {
        f64::INFINITY
    };

    // --- Results ---
    println!("\n  Results:");
    println!("  --------");
    println!("  Events to sync:        {}", n);
    println!(
        "  Peer B final count:    {} (seed + {} aligned)",
        peer_b_events,
        peer_b_events.saturating_sub(1)
    );
    println!(
        "  Status:                {}",
        if synced { "COMPLETE" } else { "INCOMPLETE" }
    );
    println!(
        "  Total sync time:       {:.3}s (from connection)",
        total_time
    );
    println!(
        "  Detection floor:       ~{:.2}s (interval + propagation_delay + jitter)",
        detection_floor
    );
    println!(
        "  Est. alignment time:   {:.3}s",
        estimated_alignment
    );
    println!(
        "  Alignment throughput:  {:.0} events/s",
        throughput
    );

    // Cleanup.
    drop(_plugin_b);
    drop(_plugin_a);
    drop(session_b);
    drop(session_a);
    tokio::time::sleep(Duration::from_millis(500)).await;
}

fn main() {
    zenoh::init_log_from_env_or("error");

    let rt = Runtime::new().expect("Failed to create Tokio runtime");

    rt.block_on(async {
        zasync_executor_init!();

        println!("Replication Catch-Up Benchmark");
        println!("==============================");
        println!("Scale points: {:?}", SCALE_POINTS);
        println!();

        for (i, &n) in SCALE_POINTS.iter().enumerate() {
            let port = PORT_BASE + i as u16;
            bench_catchup(n, port).await;
        }

        println!("\n\nIncremental Sync Benchmark");
        println!("==========================");
        println!("Scale points: {:?}", INCREMENTAL_SCALE_POINTS);
        println!();

        for (i, &n) in INCREMENTAL_SCALE_POINTS.iter().enumerate() {
            let port_a = PORT_BASE + 100 + (i as u16 * 2);
            let port_b = PORT_BASE + 101 + (i as u16 * 2);
            bench_incremental_sync(n, port_a, port_b).await;
        }

        println!("\n\nBenchmark complete.");
    });
}
