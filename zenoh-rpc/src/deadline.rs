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

use std::{
    collections::HashMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use zenoh_ext::z_deserialize;

/// Attachment key for the absolute deadline (millis since UNIX epoch).
pub const DEADLINE_ATTACHMENT_KEY: &str = "rpc:deadline_ms";

/// Server-side helper that computes remaining budget from the attached deadline.
///
/// A `DeadlineContext` is created from the absolute deadline (milliseconds since
/// the UNIX epoch) that the client attaches to its query. The server can then
/// check how much time remains before the deadline expires.
pub struct DeadlineContext {
    deadline_ms: u64,
}

impl DeadlineContext {
    /// Create from the deadline attachment value (millis since UNIX epoch).
    pub fn from_millis(deadline_ms: u64) -> Self {
        Self { deadline_ms }
    }

    /// Extract from a zenoh Query's attachment, if present.
    ///
    /// The attachment is expected to be a serialized `HashMap<String, String>`
    /// containing the key [`DEADLINE_ATTACHMENT_KEY`] with the deadline value
    /// as a decimal string of milliseconds since the UNIX epoch.
    ///
    /// Returns `None` if the attachment is missing, cannot be deserialized,
    /// or does not contain the deadline key.
    pub fn from_query(query: &zenoh::query::Query) -> Option<Self> {
        let attachment = query.attachment()?;
        let map: HashMap<String, String> = z_deserialize(attachment).ok()?;
        let deadline_str = map.get(DEADLINE_ATTACHMENT_KEY)?;
        let deadline_ms: u64 = deadline_str.parse().ok()?;
        Some(Self { deadline_ms })
    }

    /// Returns the remaining time budget. Returns `Duration::ZERO` if expired.
    pub fn remaining(&self) -> Duration {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before UNIX epoch")
            .as_millis() as u64;
        if self.deadline_ms > now_ms {
            Duration::from_millis(self.deadline_ms - now_ms)
        } else {
            Duration::ZERO
        }
    }

    /// Returns `true` if the deadline has passed.
    pub fn is_expired(&self) -> bool {
        self.remaining() == Duration::ZERO
    }

    /// Returns the absolute deadline as millis since UNIX epoch.
    pub fn deadline_ms(&self) -> u64 {
        self.deadline_ms
    }
}

/// Compute the deadline attachment key-value pair from a timeout duration.
///
/// The returned pair can be inserted into an attachment map (`HashMap<String, String>`)
/// and serialized via [`zenoh_ext::z_serialize`] before attaching to a query.
///
/// The deadline is computed as `now + timeout`, expressed as milliseconds since
/// the UNIX epoch.
pub fn deadline_attachment(timeout: Duration) -> (String, String) {
    let deadline_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_millis() as u64
        + timeout.as_millis() as u64;
    (DEADLINE_ATTACHMENT_KEY.to_string(), deadline_ms.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_millis_stores_deadline() {
        let ctx = DeadlineContext::from_millis(1_000_000);
        assert_eq!(ctx.deadline_ms(), 1_000_000);
    }

    #[test]
    fn future_deadline_is_not_expired() {
        let future_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 60_000; // 60 seconds from now
        let ctx = DeadlineContext::from_millis(future_ms);
        assert!(!ctx.is_expired());
        assert!(ctx.remaining() > Duration::ZERO);
        assert!(ctx.remaining() <= Duration::from_secs(60));
    }

    #[test]
    fn past_deadline_is_expired() {
        let ctx = DeadlineContext::from_millis(0); // epoch = always in the past
        assert!(ctx.is_expired());
        assert_eq!(ctx.remaining(), Duration::ZERO);
    }

    #[test]
    fn deadline_attachment_produces_valid_pair() {
        let timeout = Duration::from_secs(5);
        let (key, value) = deadline_attachment(timeout);
        assert_eq!(key, DEADLINE_ATTACHMENT_KEY);

        let deadline_ms: u64 = value.parse().expect("value should be a u64 string");
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        // The deadline should be roughly now + 5s (within 1s tolerance)
        assert!(deadline_ms >= now_ms + 4_000);
        assert!(deadline_ms <= now_ms + 6_000);
    }

    #[test]
    fn remaining_decreases_over_time() {
        let future_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 10_000;
        let ctx = DeadlineContext::from_millis(future_ms);
        let r1 = ctx.remaining();
        // remaining should be <= 10s
        assert!(r1 <= Duration::from_secs(10));
        assert!(r1 > Duration::from_secs(9));
    }

    #[test]
    fn zero_timeout_produces_near_now_deadline() {
        let (_, value) = deadline_attachment(Duration::ZERO);
        let deadline_ms: u64 = value.parse().unwrap();
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        // Should be within 100ms of now
        assert!(deadline_ms <= now_ms + 100);
    }
}
