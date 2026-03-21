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

// Re-export from zenoh crate
pub use zenoh::pubsub::{CursorBookmark, EventSubscriber, EventSubscriberBuilder};

use crate::serialization::{Deserialize, Serialize, ZDeserializeError, ZDeserializer, ZSerializer};

// Implement zenoh-ext Serialize/Deserialize for CursorBookmark.
// Uses a bool flag + conditional string since Option<T> is not directly
// supported by zenoh-ext serialization.

#[zenoh_macros::unstable]
impl Serialize for CursorBookmark {
    fn serialize(&self, serializer: &mut ZSerializer) {
        self.consumer_name().serialize(serializer);
        self.key_expr_str().serialize(serializer);
        match self.cursor_position() {
            Some(ts) => {
                true.serialize(serializer);
                ts.to_string().serialize(serializer);
            }
            None => {
                false.serialize(serializer);
            }
        }
    }
}

#[zenoh_macros::unstable]
impl Deserialize for CursorBookmark {
    fn deserialize(deserializer: &mut ZDeserializer) -> Result<Self, ZDeserializeError> {
        let consumer_name: String = Deserialize::deserialize(deserializer)?;
        let key_expr: String = Deserialize::deserialize(deserializer)?;
        let has_ts: bool = Deserialize::deserialize(deserializer)?;

        let mut bookmark = CursorBookmark::new(&consumer_name, &key_expr);
        if has_ts {
            let ts_str: String = Deserialize::deserialize(deserializer)?;
            let ts: zenoh::time::Timestamp = ts_str.parse().map_err(|_| ZDeserializeError)?;
            bookmark.advance(ts);
        }
        Ok(bookmark)
    }
}
