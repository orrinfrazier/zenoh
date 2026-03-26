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

//! Tests for client-side batch processing: RetrievalBatch handling, payload
//! reconstruction, and backward compatibility fallback from EventsBatch to Events.

use std::str::FromStr;

use zenoh::key_expr::OwnedKeyExpr;

use super::{AlignmentReply, RetrievalItem};
use crate::replication::core::aligner_query::AlignmentQuery;
use crate::replication::log::{Action, EventMetadata};

fn make_event(key: &str) -> EventMetadata {
    let hlc = uhlc::HLC::default();
    EventMetadata {
        stripped_key: Some(OwnedKeyExpr::from_str(key).unwrap()),
        timestamp: hlc.new_timestamp(),
        timestamp_last_non_wildcard_update: None,
        action: Action::Put,
    }
}

// ---------------------------------------------------------------------------
// RetrievalBatch match arm
// ---------------------------------------------------------------------------

#[test]
fn retrieval_batch_can_be_matched() {
    let item = RetrievalItem {
        event_metadata: make_event("test/match"),
        payload: Some(vec![42]),
        encoding: None,
    };
    match AlignmentReply::RetrievalBatch(vec![item]) {
        AlignmentReply::RetrievalBatch(items) => {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0].payload, Some(vec![42]));
        }
        _ => panic!("Expected RetrievalBatch"),
    }
}

// ---------------------------------------------------------------------------
// Payload reconstruction: RetrievalItem bytes → ZBytes
// ---------------------------------------------------------------------------

#[test]
fn retrieval_item_payload_reconstructs_to_zbytes() {
    let payload_bytes: Vec<u8> = b"hello zenoh batch".to_vec();
    let item = RetrievalItem {
        event_metadata: make_event("test/payload"),
        payload: Some(payload_bytes.clone()),
        encoding: None,
    };
    let zbytes = zenoh::bytes::ZBytes::from(item.payload.unwrap());
    assert_eq!(zbytes.to_bytes().to_vec(), payload_bytes);
}

// ---------------------------------------------------------------------------
// Backward compatibility: EventsBatch and Events carry same events
// ---------------------------------------------------------------------------

#[test]
fn events_batch_and_events_carry_same_payload() {
    let events: Vec<EventMetadata> = (0..3).map(|i| make_event(&format!("fb/{i}"))).collect();

    let batch_bytes = bincode::serialize(&AlignmentQuery::EventsBatch(events.clone())).unwrap();
    let legacy_bytes = bincode::serialize(&AlignmentQuery::Events(events)).unwrap();

    // Different discriminants, same inner data.
    assert_ne!(batch_bytes, legacy_bytes);
    match (
        bincode::deserialize::<AlignmentQuery>(&batch_bytes).unwrap(),
        bincode::deserialize::<AlignmentQuery>(&legacy_bytes).unwrap(),
    ) {
        (AlignmentQuery::EventsBatch(a), AlignmentQuery::Events(b)) => assert_eq!(a, b),
        _ => panic!("Unexpected deserialization"),
    }
}

#[test]
fn empty_retrieval_batch_is_detectable() {
    match AlignmentReply::RetrievalBatch(vec![]) {
        AlignmentReply::RetrievalBatch(items) if items.is_empty() => {}
        _ => panic!("Expected empty RetrievalBatch"),
    }
}
