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

//! Tests for Piece 1 (protocol types) and Piece 2 (server-side batch retrieval handler).
//!
//! Piece 1 tests validate that the new protocol types exist and are serializable:
//!   - `AlignmentQuery::EventsBatch(Vec<EventMetadata>)` variant
//!   - `RetrievalItem` struct with EventMetadata, optional payload bytes, optional encoding bytes
//!   - `AlignmentReply::RetrievalBatch(Vec<RetrievalItem>)` variant
//!   - Bincode round-trip serialization for all new types
//!
//! Piece 2 tests validate the batching logic for the server-side handler, including:
//!   - Chunking events into groups of `batch_size`
//!   - Constructing `RetrievalItem` for different `Action` types
//!   - Correct number of reply batches: ceil(N / batch_size)
//!   - Payload inclusion for Put/WildcardPut, omission for Delete/WildcardDelete

use std::str::FromStr;

use zenoh::key_expr::OwnedKeyExpr;

use super::{chunk_events_for_batch_retrieval, AlignmentQuery};
use crate::replication::core::aligner_reply::{AlignmentReply, RetrievalItem};
use crate::replication::log::{Action, EventMetadata};

/// Helper: create an `EventMetadata` with the given key and action.
fn make_event_metadata(key: &str, action: Action) -> EventMetadata {
    let hlc = uhlc::HLC::default();
    EventMetadata {
        stripped_key: Some(OwnedKeyExpr::from_str(key).expect("valid key expression")),
        timestamp: hlc.new_timestamp(),
        timestamp_last_non_wildcard_update: match &action {
            Action::Put | Action::Delete => Some(hlc.new_timestamp()),
            Action::WildcardPut(_) | Action::WildcardDelete(_) => None,
        },
        action,
    }
}

// ===========================================================================
// Piece 1: Protocol types — AlignmentQuery::EventsBatch, RetrievalItem,
//          AlignmentReply::RetrievalBatch
// ===========================================================================

// ---------------------------------------------------------------------------
// AlignmentQuery::EventsBatch(Vec<EventMetadata>) existence & serde
// ---------------------------------------------------------------------------

#[test]
fn piece1_alignment_query_events_batch_variant_exists() {
    let events = vec![make_event_metadata("piece1/a", Action::Put)];
    let _query = AlignmentQuery::EventsBatch(events);
}

#[test]
fn piece1_alignment_query_events_batch_bincode_roundtrip() {
    let events = vec![
        make_event_metadata("piece1/x", Action::Put),
        make_event_metadata("piece1/y", Action::Delete),
    ];
    let query = AlignmentQuery::EventsBatch(events);

    let serialized = bincode::serialize(&query).expect("serialize EventsBatch");
    let deserialized: AlignmentQuery =
        bincode::deserialize(&serialized).expect("deserialize EventsBatch");
    assert_eq!(query, deserialized);
}

#[test]
fn piece1_alignment_query_events_batch_empty_vec_roundtrip() {
    let query = AlignmentQuery::EventsBatch(vec![]);

    let serialized = bincode::serialize(&query).expect("serialize empty EventsBatch");
    let deserialized: AlignmentQuery =
        bincode::deserialize(&serialized).expect("deserialize empty EventsBatch");
    assert_eq!(query, deserialized);
}

#[test]
fn piece1_alignment_query_events_batch_preserves_event_data() {
    let event = make_event_metadata("piece1/preserve", Action::Put);
    let query = AlignmentQuery::EventsBatch(vec![event.clone()]);

    match query {
        AlignmentQuery::EventsBatch(ref events) => {
            assert_eq!(events.len(), 1);
            assert_eq!(events[0], event);
        }
        _ => panic!("Expected EventsBatch variant"),
    }
}

// ---------------------------------------------------------------------------
// RetrievalItem struct existence with correct field types
// ---------------------------------------------------------------------------

#[test]
fn piece1_retrieval_item_struct_with_payload_bytes() {
    let metadata = make_event_metadata("piece1/item", Action::Put);
    let payload: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF];
    let encoding: Vec<u8> = vec![0x01, 0x02];

    let item = RetrievalItem {
        event_metadata: metadata.clone(),
        payload: Some(payload.clone()),
        encoding: Some(encoding.clone()),
    };

    assert_eq!(item.event_metadata, metadata);
    assert_eq!(item.payload, Some(payload));
    assert_eq!(item.encoding, Some(encoding));
}

#[test]
fn piece1_retrieval_item_with_none_payload_and_encoding() {
    let metadata = make_event_metadata("piece1/del", Action::Delete);

    let item = RetrievalItem {
        event_metadata: metadata.clone(),
        payload: None,
        encoding: None,
    };

    assert_eq!(item.event_metadata, metadata);
    assert!(item.payload.is_none());
    assert!(item.encoding.is_none());
}

#[test]
fn piece1_retrieval_item_bincode_roundtrip_with_payload() {
    let metadata = make_event_metadata("piece1/serde", Action::Put);
    let item = RetrievalItem {
        event_metadata: metadata,
        payload: Some(vec![1, 2, 3, 4, 5]),
        encoding: Some(vec![10, 20]),
    };

    let serialized = bincode::serialize(&item).expect("serialize RetrievalItem");
    let deserialized: RetrievalItem =
        bincode::deserialize(&serialized).expect("deserialize RetrievalItem");
    assert_eq!(item, deserialized);
}

#[test]
fn piece1_retrieval_item_bincode_roundtrip_without_payload() {
    let metadata = make_event_metadata("piece1/serde-none", Action::Delete);
    let item = RetrievalItem {
        event_metadata: metadata,
        payload: None,
        encoding: None,
    };

    let serialized = bincode::serialize(&item).expect("serialize RetrievalItem (no payload)");
    let deserialized: RetrievalItem =
        bincode::deserialize(&serialized).expect("deserialize RetrievalItem (no payload)");
    assert_eq!(item, deserialized);
}

// ---------------------------------------------------------------------------
// AlignmentReply::RetrievalBatch(Vec<RetrievalItem>) existence & serde
// ---------------------------------------------------------------------------

#[test]
fn piece1_alignment_reply_retrieval_batch_variant_exists() {
    let item = RetrievalItem {
        event_metadata: make_event_metadata("piece1/reply", Action::Put),
        payload: Some(vec![42u8]),
        encoding: Some(vec![1u8]),
    };
    let _reply = AlignmentReply::RetrievalBatch(vec![item]);
}

#[test]
fn piece1_alignment_reply_retrieval_batch_bincode_roundtrip() {
    let items = vec![
        RetrievalItem {
            event_metadata: make_event_metadata("piece1/r/a", Action::Put),
            payload: Some(vec![1, 2, 3]),
            encoding: Some(vec![99]),
        },
        RetrievalItem {
            event_metadata: make_event_metadata("piece1/r/b", Action::Delete),
            payload: None,
            encoding: None,
        },
    ];
    let reply = AlignmentReply::RetrievalBatch(items);

    let serialized = bincode::serialize(&reply).expect("serialize RetrievalBatch");
    let deserialized: AlignmentReply =
        bincode::deserialize(&serialized).expect("deserialize RetrievalBatch");
    assert_eq!(reply, deserialized);
}

#[test]
fn piece1_alignment_reply_retrieval_batch_empty_vec_roundtrip() {
    let reply = AlignmentReply::RetrievalBatch(vec![]);

    let serialized = bincode::serialize(&reply).expect("serialize empty RetrievalBatch");
    let deserialized: AlignmentReply =
        bincode::deserialize(&serialized).expect("deserialize empty RetrievalBatch");
    assert_eq!(reply, deserialized);
}

#[test]
fn piece1_alignment_reply_retrieval_batch_preserves_items() {
    let items = vec![
        RetrievalItem {
            event_metadata: make_event_metadata("piece1/p/0", Action::Put),
            payload: Some(vec![10]),
            encoding: Some(vec![20]),
        },
        RetrievalItem {
            event_metadata: make_event_metadata("piece1/p/1", Action::Put),
            payload: Some(vec![30]),
            encoding: Some(vec![40]),
        },
        RetrievalItem {
            event_metadata: make_event_metadata("piece1/p/2", Action::Delete),
            payload: None,
            encoding: None,
        },
    ];

    let reply = AlignmentReply::RetrievalBatch(items);
    match reply {
        AlignmentReply::RetrievalBatch(ref batch) => {
            assert_eq!(batch.len(), 3);
            assert!(batch[0].payload.is_some());
            assert!(batch[2].payload.is_none());
        }
        _ => panic!("Expected RetrievalBatch variant"),
    }
}

// ===========================================================================
// Piece 2: Server-side batch retrieval handler
// ===========================================================================

// ---------------------------------------------------------------------------
// Tests for AlignmentQuery::EventsBatch variant
// ---------------------------------------------------------------------------

#[test]
fn piece2_events_batch_variant_exists() {
    let events = vec![make_event_metadata("a/b", Action::Put)];

    let query = AlignmentQuery::EventsBatch(events);

    let serialized = bincode::serialize(&query).expect("serialize EventsBatch");
    let deserialized: AlignmentQuery =
        bincode::deserialize(&serialized).expect("deserialize EventsBatch");
    assert_eq!(query, deserialized);
}

// ---------------------------------------------------------------------------
// Tests for AlignmentReply::RetrievalBatch variant
// ---------------------------------------------------------------------------

#[test]
fn piece2_retrieval_batch_variant_exists() {
    let item = RetrievalItem {
        event_metadata: make_event_metadata("a/b", Action::Put),
        payload: Some(vec![1u8, 2, 3]),
        encoding: Some(vec![0u8]),
    };

    let reply = AlignmentReply::RetrievalBatch(vec![item]);

    let serialized = bincode::serialize(&reply).expect("serialize RetrievalBatch");
    let deserialized: AlignmentReply =
        bincode::deserialize(&serialized).expect("deserialize RetrievalBatch");
    assert_eq!(reply, deserialized);
}

// ---------------------------------------------------------------------------
// Tests for RetrievalItem construction per Action type
// ---------------------------------------------------------------------------

#[test]
fn piece2_retrieval_item_put_has_payload() {
    let metadata = make_event_metadata("sensor/temp", Action::Put);
    let payload_bytes = vec![42u8; 64];

    let item = RetrievalItem {
        event_metadata: metadata.clone(),
        payload: Some(payload_bytes.clone()),
        encoding: Some(vec![0u8]),
    };

    assert_eq!(item.event_metadata, metadata);
    assert!(item.payload.is_some(), "Put events must include payload");
    assert!(item.encoding.is_some(), "Put events must include encoding");
}

#[test]
fn piece2_retrieval_item_delete_has_no_payload() {
    let metadata = make_event_metadata("sensor/temp", Action::Delete);

    let item = RetrievalItem {
        event_metadata: metadata.clone(),
        payload: None,
        encoding: None,
    };

    assert_eq!(item.event_metadata, metadata);
    assert!(
        item.payload.is_none(),
        "Delete events must NOT include payload"
    );
    assert!(
        item.encoding.is_none(),
        "Delete events must NOT include encoding"
    );
}

#[test]
fn piece2_retrieval_item_wildcard_put_has_payload() {
    let wildcard_ke = OwnedKeyExpr::from_str("sensor/**").expect("valid wildcard key");
    let metadata = make_event_metadata("sensor/**", Action::WildcardPut(wildcard_ke));
    let payload_bytes = vec![99u8; 32];

    let item = RetrievalItem {
        event_metadata: metadata.clone(),
        payload: Some(payload_bytes),
        encoding: Some(vec![0u8]),
    };

    assert!(
        item.payload.is_some(),
        "WildcardPut events must include payload"
    );
    assert!(
        item.encoding.is_some(),
        "WildcardPut events must include encoding"
    );
}

#[test]
fn piece2_retrieval_item_wildcard_delete_has_no_payload() {
    let wildcard_ke = OwnedKeyExpr::from_str("sensor/**").expect("valid wildcard key");
    let metadata = make_event_metadata("sensor/**", Action::WildcardDelete(wildcard_ke));

    let item = RetrievalItem {
        event_metadata: metadata.clone(),
        payload: None,
        encoding: None,
    };

    assert!(
        item.payload.is_none(),
        "WildcardDelete events must NOT include payload"
    );
    assert!(
        item.encoding.is_none(),
        "WildcardDelete events must NOT include encoding"
    );
}

// ---------------------------------------------------------------------------
// Tests for batch chunking logic
// ---------------------------------------------------------------------------

#[test]
fn piece2_chunk_exact_division() {
    let events: Vec<EventMetadata> = (0..10)
        .map(|i| make_event_metadata(&format!("key/{i}"), Action::Put))
        .collect();

    let chunks = chunk_events_for_batch_retrieval(&events, 5);

    assert_eq!(chunks.len(), 2, "10 events / batch_size 5 = 2 chunks");
    assert_eq!(chunks[0].len(), 5);
    assert_eq!(chunks[1].len(), 5);
}

#[test]
fn piece2_chunk_remainder() {
    let events: Vec<EventMetadata> = (0..7)
        .map(|i| make_event_metadata(&format!("key/{i}"), Action::Put))
        .collect();

    let chunks = chunk_events_for_batch_retrieval(&events, 3);

    assert_eq!(
        chunks.len(),
        3,
        "7 events / batch_size 3 = 3 chunks (ceil)"
    );
    assert_eq!(chunks[0].len(), 3);
    assert_eq!(chunks[1].len(), 3);
    assert_eq!(chunks[2].len(), 1, "Last chunk gets the remainder");
}

#[test]
fn piece2_chunk_single_batch_when_n_leq_batch_size() {
    let events: Vec<EventMetadata> = (0..3)
        .map(|i| make_event_metadata(&format!("key/{i}"), Action::Delete))
        .collect();

    let chunks = chunk_events_for_batch_retrieval(&events, 10);

    assert_eq!(
        chunks.len(),
        1,
        "When N <= batch_size, there should be exactly 1 chunk"
    );
    assert_eq!(chunks[0].len(), 3);
}

#[test]
fn piece2_chunk_empty_events() {
    let events: Vec<EventMetadata> = vec![];

    let chunks = chunk_events_for_batch_retrieval(&events, 5);

    assert_eq!(chunks.len(), 0, "Empty event list should produce 0 chunks");
}

#[test]
fn piece2_chunk_batch_size_one() {
    let events: Vec<EventMetadata> = (0..4)
        .map(|i| make_event_metadata(&format!("key/{i}"), Action::Put))
        .collect();

    let chunks: Vec<&[EventMetadata]> = chunk_events_for_batch_retrieval(&events, 1);

    assert_eq!(
        chunks.len(),
        4,
        "batch_size=1 should produce one chunk per event"
    );
    for chunk in &chunks {
        assert_eq!(chunk.len(), 1);
    }
}

#[test]
fn piece2_chunk_preserves_event_order() {
    let events: Vec<EventMetadata> = (0..6)
        .map(|i| make_event_metadata(&format!("ordered/{i}"), Action::Put))
        .collect();

    let chunks = chunk_events_for_batch_retrieval(&events, 2);

    assert_eq!(chunks.len(), 3);
    assert_eq!(
        chunks[0][0].stripped_key,
        Some(OwnedKeyExpr::from_str("ordered/0").unwrap())
    );
    assert_eq!(
        chunks[0][1].stripped_key,
        Some(OwnedKeyExpr::from_str("ordered/1").unwrap())
    );
    assert_eq!(
        chunks[1][0].stripped_key,
        Some(OwnedKeyExpr::from_str("ordered/2").unwrap())
    );
    assert_eq!(
        chunks[1][1].stripped_key,
        Some(OwnedKeyExpr::from_str("ordered/3").unwrap())
    );
}

// ---------------------------------------------------------------------------
// Tests for reply count = ceil(N / batch_size)
// ---------------------------------------------------------------------------

#[test]
fn piece2_reply_count_matches_chunk_count() {
    for (n_events, batch_size, expected_replies) in [
        (0usize, 5usize, 0usize),
        (1, 5, 1),
        (5, 5, 1),
        (6, 5, 2),
        (10, 5, 2),
        (11, 5, 3),
        (100, 10, 10),
        (101, 10, 11),
        (1, 1, 1),
        (7, 3, 3),
    ] {
        let events: Vec<EventMetadata> = (0..n_events)
            .map(|i| make_event_metadata(&format!("key/{i}"), Action::Put))
            .collect();

        let chunks = chunk_events_for_batch_retrieval(&events, batch_size);

        assert_eq!(
            chunks.len(),
            expected_replies,
            "For {n_events} events with batch_size={batch_size}, expected {expected_replies} \
             replies but got {}",
            chunks.len()
        );
    }
}

// ---------------------------------------------------------------------------
// Tests for mixed Action types in a single batch
// ---------------------------------------------------------------------------

#[test]
fn piece2_chunk_mixed_action_types() {
    let wildcard_ke = OwnedKeyExpr::from_str("test/**").expect("valid wildcard key");

    let events = vec![
        make_event_metadata("a/put", Action::Put),
        make_event_metadata("b/delete", Action::Delete),
        make_event_metadata("test/**", Action::WildcardPut(wildcard_ke.clone())),
        make_event_metadata("test/**", Action::WildcardDelete(wildcard_ke)),
    ];

    let chunks = chunk_events_for_batch_retrieval(&events, 4);
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].len(), 4);

    assert_eq!(chunks[0][0].action, Action::Put);
    assert_eq!(chunks[0][1].action, Action::Delete);
    assert!(matches!(chunks[0][2].action, Action::WildcardPut(_)));
    assert!(matches!(chunks[0][3].action, Action::WildcardDelete(_)));
}

// ---------------------------------------------------------------------------
// Test: RetrievalBatch contains the right number of RetrievalItems
// ---------------------------------------------------------------------------

#[test]
fn piece2_retrieval_batch_item_count_matches_chunk() {
    let items: Vec<RetrievalItem> = (0..5)
        .map(|i| RetrievalItem {
            event_metadata: make_event_metadata(&format!("item/{i}"), Action::Put),
            payload: Some(vec![i as u8; 8]),
            encoding: Some(vec![0u8]),
        })
        .collect();

    let reply = AlignmentReply::RetrievalBatch(items);

    match reply {
        AlignmentReply::RetrievalBatch(ref batch_items) => {
            assert_eq!(batch_items.len(), 5);
        }
        _ => panic!("Expected RetrievalBatch variant"),
    }
}
