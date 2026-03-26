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

//! Tests for batch retrieval protocol types, serialization, and chunking logic.

use std::str::FromStr;

use zenoh::key_expr::OwnedKeyExpr;

use super::{chunk_events_for_batch_retrieval, AlignmentQuery};
use crate::replication::core::aligner_reply::{AlignmentReply, RetrievalItem};
use crate::replication::log::{Action, EventMetadata};

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
// AlignmentQuery::EventsBatch
// ===========================================================================

#[test]
fn events_batch_bincode_roundtrip() {
    let events = vec![
        make_event_metadata("x", Action::Put),
        make_event_metadata("y", Action::Delete),
    ];
    let query = AlignmentQuery::EventsBatch(events);
    let bytes = bincode::serialize(&query).unwrap();
    assert_eq!(query, bincode::deserialize::<AlignmentQuery>(&bytes).unwrap());
}

#[test]
fn events_batch_empty_roundtrip() {
    let query = AlignmentQuery::EventsBatch(vec![]);
    let bytes = bincode::serialize(&query).unwrap();
    assert_eq!(query, bincode::deserialize::<AlignmentQuery>(&bytes).unwrap());
}

#[test]
fn events_batch_is_distinct_from_events() {
    let event = make_event_metadata("a", Action::Put);
    assert_ne!(
        AlignmentQuery::EventsBatch(vec![event.clone()]),
        AlignmentQuery::Events(vec![event]),
    );
}

// ===========================================================================
// RetrievalItem
// ===========================================================================

#[test]
fn retrieval_item_with_payload() {
    let metadata = make_event_metadata("k", Action::Put);
    let item = RetrievalItem {
        event_metadata: metadata.clone(),
        payload: Some(vec![0xDE, 0xAD]),
        encoding: Some(vec![0x01]),
    };
    assert_eq!(item.event_metadata, metadata);
    assert!(item.payload.is_some());
    assert!(item.encoding.is_some());
}

#[test]
fn retrieval_item_without_payload() {
    let item = RetrievalItem {
        event_metadata: make_event_metadata("k", Action::Delete),
        payload: None,
        encoding: None,
    };
    assert!(item.payload.is_none());
    assert!(item.encoding.is_none());
}

#[test]
fn retrieval_item_bincode_roundtrip() {
    let item = RetrievalItem {
        event_metadata: make_event_metadata("k", Action::Put),
        payload: Some(vec![1, 2, 3]),
        encoding: Some(vec![10]),
    };
    let bytes = bincode::serialize(&item).unwrap();
    assert_eq!(item, bincode::deserialize::<RetrievalItem>(&bytes).unwrap());
}

#[test]
fn retrieval_item_none_bincode_roundtrip() {
    let item = RetrievalItem {
        event_metadata: make_event_metadata("k", Action::Delete),
        payload: None,
        encoding: None,
    };
    let bytes = bincode::serialize(&item).unwrap();
    assert_eq!(item, bincode::deserialize::<RetrievalItem>(&bytes).unwrap());
}

// ===========================================================================
// AlignmentReply::RetrievalBatch
// ===========================================================================

#[test]
fn retrieval_batch_bincode_roundtrip() {
    let reply = AlignmentReply::RetrievalBatch(vec![
        RetrievalItem {
            event_metadata: make_event_metadata("a", Action::Put),
            payload: Some(vec![1]),
            encoding: Some(vec![2]),
        },
        RetrievalItem {
            event_metadata: make_event_metadata("b", Action::Delete),
            payload: None,
            encoding: None,
        },
    ]);
    let bytes = bincode::serialize(&reply).unwrap();
    assert_eq!(reply, bincode::deserialize::<AlignmentReply>(&bytes).unwrap());
}

#[test]
fn retrieval_batch_empty_roundtrip() {
    let reply = AlignmentReply::RetrievalBatch(vec![]);
    let bytes = bincode::serialize(&reply).unwrap();
    assert_eq!(reply, bincode::deserialize::<AlignmentReply>(&bytes).unwrap());
}

#[test]
fn retrieval_batch_preserves_item_count() {
    let items: Vec<RetrievalItem> = (0..5)
        .map(|i| RetrievalItem {
            event_metadata: make_event_metadata(&format!("k/{i}"), Action::Put),
            payload: Some(vec![i as u8]),
            encoding: Some(vec![0]),
        })
        .collect();

    match AlignmentReply::RetrievalBatch(items) {
        AlignmentReply::RetrievalBatch(batch) => assert_eq!(batch.len(), 5),
        _ => panic!("Expected RetrievalBatch"),
    }
}

// ===========================================================================
// RetrievalItem per Action type
// ===========================================================================

#[test]
fn retrieval_item_wildcard_put_has_payload() {
    let ke = OwnedKeyExpr::from_str("sensor/**").unwrap();
    let item = RetrievalItem {
        event_metadata: make_event_metadata("sensor/**", Action::WildcardPut(ke)),
        payload: Some(vec![99]),
        encoding: Some(vec![0]),
    };
    assert!(item.payload.is_some());
}

#[test]
fn retrieval_item_wildcard_delete_has_no_payload() {
    let ke = OwnedKeyExpr::from_str("sensor/**").unwrap();
    let item = RetrievalItem {
        event_metadata: make_event_metadata("sensor/**", Action::WildcardDelete(ke)),
        payload: None,
        encoding: None,
    };
    assert!(item.payload.is_none());
}

// ===========================================================================
// chunk_events_for_batch_retrieval
// ===========================================================================

#[test]
fn chunk_exact_division() {
    let events: Vec<EventMetadata> = (0..10)
        .map(|i| make_event_metadata(&format!("k/{i}"), Action::Put))
        .collect();
    let chunks = chunk_events_for_batch_retrieval(&events, 5);
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].len(), 5);
    assert_eq!(chunks[1].len(), 5);
}

#[test]
fn chunk_with_remainder() {
    let events: Vec<EventMetadata> = (0..7)
        .map(|i| make_event_metadata(&format!("k/{i}"), Action::Put))
        .collect();
    let chunks = chunk_events_for_batch_retrieval(&events, 3);
    assert_eq!(chunks.len(), 3);
    assert_eq!(chunks[2].len(), 1);
}

#[test]
fn chunk_single_when_under_batch_size() {
    let events: Vec<EventMetadata> = (0..3)
        .map(|i| make_event_metadata(&format!("k/{i}"), Action::Put))
        .collect();
    let chunks = chunk_events_for_batch_retrieval(&events, 10);
    assert_eq!(chunks.len(), 1);
}

#[test]
fn chunk_empty_events() {
    let chunks = chunk_events_for_batch_retrieval(&[], 5);
    assert_eq!(chunks.len(), 0);
}

#[test]
fn chunk_batch_size_one() {
    let events: Vec<EventMetadata> = (0..4)
        .map(|i| make_event_metadata(&format!("k/{i}"), Action::Put))
        .collect();
    let chunks = chunk_events_for_batch_retrieval(&events, 1);
    assert_eq!(chunks.len(), 4);
}

#[test]
fn chunk_preserves_order() {
    let events: Vec<EventMetadata> = (0..6)
        .map(|i| make_event_metadata(&format!("ordered/{i}"), Action::Put))
        .collect();
    let chunks = chunk_events_for_batch_retrieval(&events, 2);
    assert_eq!(
        chunks[0][0].stripped_key,
        Some(OwnedKeyExpr::from_str("ordered/0").unwrap())
    );
    assert_eq!(
        chunks[1][0].stripped_key,
        Some(OwnedKeyExpr::from_str("ordered/2").unwrap())
    );
}

#[test]
fn chunk_count_matches_ceil_division() {
    for (n, bs, expected) in [
        (0, 5, 0),
        (1, 5, 1),
        (5, 5, 1),
        (6, 5, 2),
        (10, 5, 2),
        (11, 5, 3),
        (100, 10, 10),
        (101, 10, 11),
    ] {
        let events: Vec<EventMetadata> = (0..n)
            .map(|i| make_event_metadata(&format!("k/{i}"), Action::Put))
            .collect();
        assert_eq!(
            chunk_events_for_batch_retrieval(&events, bs).len(),
            expected,
            "n={n}, bs={bs}"
        );
    }
}

#[test]
fn chunk_mixed_action_types() {
    let ke = OwnedKeyExpr::from_str("test/**").unwrap();
    let events = vec![
        make_event_metadata("a", Action::Put),
        make_event_metadata("b", Action::Delete),
        make_event_metadata("test/**", Action::WildcardPut(ke.clone())),
        make_event_metadata("test/**", Action::WildcardDelete(ke)),
    ];
    let chunks = chunk_events_for_batch_retrieval(&events, 4);
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].len(), 4);
}
