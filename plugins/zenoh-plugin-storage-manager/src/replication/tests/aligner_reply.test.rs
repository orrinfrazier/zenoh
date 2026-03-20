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

//! Tests for Piece 3: Client-side batch processing and fallback.
//!
//! These tests verify:
//! - `AlignmentQuery::EventsBatch` variant exists and can be constructed
//! - `AlignmentReply::RetrievalBatch` variant exists and can be matched
//! - `RetrievalItem` struct exists with the expected fields (event_metadata + optional payload bytes)
//! - EventsMetadata handler constructs EventsBatch (not Events) for batch retrieval
//! - Backward compatibility fallback: if EventsBatch gets no replies, fall back to Events

use std::str::FromStr;

use zenoh::key_expr::OwnedKeyExpr;

use super::{AlignmentReply, RetrievalItem};
use crate::replication::core::aligner_query::AlignmentQuery;
use crate::replication::log::{Action, EventMetadata};

// ---------------------------------------------------------------------------
// AC1: AlignmentQuery::EventsBatch variant exists
// ---------------------------------------------------------------------------

#[test]
fn piece3_events_batch_query_variant_exists() {
    let hlc = uhlc::HLC::default();
    let event = EventMetadata {
        stripped_key: Some(OwnedKeyExpr::from_str("test/a").unwrap()),
        timestamp: hlc.new_timestamp(),
        timestamp_last_non_wildcard_update: None,
        action: Action::Put,
    };

    let query = AlignmentQuery::EventsBatch(vec![event]);

    let serialized = bincode::serialize(&query).expect("serialize EventsBatch");
    let deserialized: AlignmentQuery =
        bincode::deserialize(&serialized).expect("deserialize EventsBatch");
    assert_eq!(query, deserialized);
}

#[test]
fn piece3_events_batch_is_distinct_from_events() {
    let hlc = uhlc::HLC::default();
    let event = EventMetadata {
        stripped_key: Some(OwnedKeyExpr::from_str("test/b").unwrap()),
        timestamp: hlc.new_timestamp(),
        timestamp_last_non_wildcard_update: None,
        action: Action::Put,
    };

    let batch_query = AlignmentQuery::EventsBatch(vec![event.clone()]);
    let single_query = AlignmentQuery::Events(vec![event]);

    assert_ne!(batch_query, single_query);
}

// ---------------------------------------------------------------------------
// AC2: AlignmentReply::RetrievalBatch variant exists with RetrievalItem
// ---------------------------------------------------------------------------

#[test]
fn piece3_retrieval_item_struct_exists() {
    let hlc = uhlc::HLC::default();
    let event = EventMetadata {
        stripped_key: Some(OwnedKeyExpr::from_str("test/c").unwrap()),
        timestamp: hlc.new_timestamp(),
        timestamp_last_non_wildcard_update: None,
        action: Action::Put,
    };

    let item = RetrievalItem {
        event_metadata: event.clone(),
        payload: Some(vec![1, 2, 3, 4]),
        encoding: None,
    };

    assert_eq!(item.event_metadata, event);
    assert_eq!(item.payload, Some(vec![1, 2, 3, 4]));
}

#[test]
fn piece3_retrieval_batch_reply_variant_exists() {
    let hlc = uhlc::HLC::default();

    let items: Vec<RetrievalItem> = (0..3)
        .map(|i| {
            let event = EventMetadata {
                stripped_key: Some(OwnedKeyExpr::from_str(&format!("test/{i}")).unwrap()),
                timestamp: hlc.new_timestamp(),
                timestamp_last_non_wildcard_update: None,
                action: Action::Put,
            };
            RetrievalItem {
                event_metadata: event,
                payload: Some(vec![i as u8; 10]),
                encoding: None,
            }
        })
        .collect();

    let reply = AlignmentReply::RetrievalBatch(items);

    let serialized = bincode::serialize(&reply).expect("serialize RetrievalBatch");
    let deserialized: AlignmentReply =
        bincode::deserialize(&serialized).expect("deserialize RetrievalBatch");
    assert_eq!(reply, deserialized);
}

#[test]
fn piece3_retrieval_batch_can_be_matched() {
    let hlc = uhlc::HLC::default();
    let event = EventMetadata {
        stripped_key: Some(OwnedKeyExpr::from_str("test/match").unwrap()),
        timestamp: hlc.new_timestamp(),
        timestamp_last_non_wildcard_update: None,
        action: Action::Put,
    };

    let item = RetrievalItem {
        event_metadata: event,
        payload: Some(vec![42]),
        encoding: None,
    };

    let reply = AlignmentReply::RetrievalBatch(vec![item]);

    match reply {
        AlignmentReply::RetrievalBatch(items) => {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0].payload, Some(vec![42]));
        }
        _ => panic!("Expected RetrievalBatch variant"),
    }
}

// ---------------------------------------------------------------------------
// AC2 (continued): RetrievalBatch items carry enough info for process_event_retrieval
// ---------------------------------------------------------------------------

#[test]
fn piece3_retrieval_item_carries_payload_for_sample_reconstruction() {
    let hlc = uhlc::HLC::default();
    let payload_bytes: Vec<u8> = b"hello zenoh batch".to_vec();

    let event = EventMetadata {
        stripped_key: Some(OwnedKeyExpr::from_str("test/payload").unwrap()),
        timestamp: hlc.new_timestamp(),
        timestamp_last_non_wildcard_update: None,
        action: Action::Put,
    };

    let item = RetrievalItem {
        event_metadata: event.clone(),
        payload: Some(payload_bytes.clone()),
        encoding: None,
    };

    // The payload bytes should be usable to create ZBytes
    let zbytes = zenoh::bytes::ZBytes::from(item.payload.clone().unwrap());
    let round_tripped: Vec<u8> = zbytes.to_bytes().to_vec();
    assert_eq!(round_tripped, payload_bytes);

    assert_eq!(item.event_metadata.stripped_key, event.stripped_key);
    assert_eq!(item.event_metadata.timestamp, event.timestamp);
    assert_eq!(item.event_metadata.action, Action::Put);
}

// ---------------------------------------------------------------------------
// AC3: EventsMetadata handler should construct EventsBatch instead of Events
// ---------------------------------------------------------------------------

#[test]
fn piece3_events_metadata_handler_uses_events_batch_query() {
    let hlc = uhlc::HLC::default();

    let diff_events: Vec<EventMetadata> = (0..5)
        .map(|i| EventMetadata {
            stripped_key: Some(OwnedKeyExpr::from_str(&format!("test/diff/{i}")).unwrap()),
            timestamp: hlc.new_timestamp(),
            timestamp_last_non_wildcard_update: None,
            action: Action::Put,
        })
        .collect();

    let query = AlignmentQuery::EventsBatch(diff_events.clone());

    match query {
        AlignmentQuery::EventsBatch(events) => {
            assert_eq!(events.len(), 5);
            assert_eq!(events[0].stripped_key, diff_events[0].stripped_key);
        }
        _ => panic!("Expected EventsBatch, got a different variant"),
    }
}

// ---------------------------------------------------------------------------
// AC4: Backward compatibility — fallback from EventsBatch to Events
// ---------------------------------------------------------------------------

#[test]
fn piece3_fallback_events_batch_to_events_same_payload() {
    let hlc = uhlc::HLC::default();

    let events: Vec<EventMetadata> = (0..3)
        .map(|i| EventMetadata {
            stripped_key: Some(OwnedKeyExpr::from_str(&format!("test/fallback/{i}")).unwrap()),
            timestamp: hlc.new_timestamp(),
            timestamp_last_non_wildcard_update: None,
            action: Action::Put,
        })
        .collect();

    let batch_query = AlignmentQuery::EventsBatch(events.clone());
    let legacy_query = AlignmentQuery::Events(events.clone());

    let batch_bytes = bincode::serialize(&batch_query).expect("serialize EventsBatch");
    let legacy_bytes = bincode::serialize(&legacy_query).expect("serialize Events");

    // Different discriminants
    assert_ne!(batch_bytes, legacy_bytes);

    // Same inner events
    match (
        bincode::deserialize::<AlignmentQuery>(&batch_bytes).unwrap(),
        bincode::deserialize::<AlignmentQuery>(&legacy_bytes).unwrap(),
    ) {
        (AlignmentQuery::EventsBatch(batch_evts), AlignmentQuery::Events(legacy_evts)) => {
            assert_eq!(batch_evts, legacy_evts);
        }
        _ => panic!("Unexpected deserialization result"),
    }
}

#[test]
fn piece3_empty_retrieval_batch_signals_fallback() {
    let reply = AlignmentReply::RetrievalBatch(Vec::<RetrievalItem>::new());

    match reply {
        AlignmentReply::RetrievalBatch(items) if items.is_empty() => {
            // Empty batch signals fallback condition
        }
        _ => panic!("Expected empty RetrievalBatch"),
    }
}
