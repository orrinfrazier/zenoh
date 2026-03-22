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

use std::time::Duration;

use zenoh_ext::{z_deserialize, z_serialize};
use zenoh_rpc::{
    deadline_attachment, DeadlineContext, ServiceError, StatusCode, DEADLINE_ATTACHMENT_KEY,
};

// -- Round-trip serialization for each variant --

#[test]
fn service_error_round_trip_invalid_request() {
    let err = ServiceError::InvalidRequest {
        message: "field 'name' is required".to_string(),
    };
    let zbytes = z_serialize(&err);
    let recovered: ServiceError = z_deserialize(&zbytes).expect("deserialization failed");
    assert_eq!(recovered, err);
}

#[test]
fn service_error_round_trip_not_found() {
    let err = ServiceError::NotFound {
        message: "device 42".to_string(),
    };
    let zbytes = z_serialize(&err);
    let recovered: ServiceError = z_deserialize(&zbytes).expect("deserialization failed");
    assert_eq!(recovered, err);
}

#[test]
fn service_error_round_trip_deadline_exceeded() {
    let err = ServiceError::DeadlineExceeded { budget_ms: 5000 };
    let zbytes = z_serialize(&err);
    let recovered: ServiceError = z_deserialize(&zbytes).expect("deserialization failed");
    assert_eq!(recovered, err);
}

#[test]
fn service_error_round_trip_internal() {
    let err = ServiceError::Internal {
        message: "unexpected null pointer".to_string(),
    };
    let zbytes = z_serialize(&err);
    let recovered: ServiceError = z_deserialize(&zbytes).expect("deserialization failed");
    assert_eq!(recovered, err);
}

#[test]
fn service_error_round_trip_method_not_found() {
    let err = ServiceError::MethodNotFound {
        method: "rpc/getConfig".to_string(),
    };
    let zbytes = z_serialize(&err);
    let recovered: ServiceError = z_deserialize(&zbytes).expect("deserialization failed");
    assert_eq!(recovered, err);
}

#[test]
fn service_error_round_trip_application() {
    let err = ServiceError::Application {
        code: 429,
        message: "rate limit exceeded".to_string(),
    };
    let zbytes = z_serialize(&err);
    let recovered: ServiceError = z_deserialize(&zbytes).expect("deserialization failed");
    assert_eq!(recovered, err);
}

// -- status_code() returns correct code for each variant --

#[test]
fn status_code_matches_variant() {
    let cases: Vec<(ServiceError, StatusCode)> = vec![
        (
            ServiceError::InvalidRequest {
                message: String::new(),
            },
            StatusCode::InvalidRequest,
        ),
        (
            ServiceError::NotFound {
                message: String::new(),
            },
            StatusCode::NotFound,
        ),
        (
            ServiceError::DeadlineExceeded { budget_ms: 0 },
            StatusCode::DeadlineExceeded,
        ),
        (
            ServiceError::Internal {
                message: String::new(),
            },
            StatusCode::Internal,
        ),
        (
            ServiceError::MethodNotFound {
                method: String::new(),
            },
            StatusCode::MethodNotFound,
        ),
        (
            ServiceError::Application {
                code: 0,
                message: String::new(),
            },
            StatusCode::Application,
        ),
    ];

    for (err, expected_code) in cases {
        assert_eq!(
            err.status_code(),
            expected_code,
            "wrong status code for {err:?}"
        );
    }
}

// -- Display formatting is human-readable --

#[test]
fn display_is_human_readable() {
    assert_eq!(
        ServiceError::InvalidRequest {
            message: "bad".to_string()
        }
        .to_string(),
        "invalid request: bad"
    );

    assert_eq!(
        ServiceError::NotFound {
            message: "key".to_string()
        }
        .to_string(),
        "not found: key"
    );

    assert_eq!(
        ServiceError::DeadlineExceeded { budget_ms: 100 }.to_string(),
        "deadline exceeded: budget was 100ms"
    );

    assert_eq!(
        ServiceError::Internal {
            message: "boom".to_string()
        }
        .to_string(),
        "internal error: boom"
    );

    assert_eq!(
        ServiceError::MethodNotFound {
            method: "doStuff".to_string()
        }
        .to_string(),
        "method not found: doStuff"
    );

    assert_eq!(
        ServiceError::Application {
            code: 503,
            message: "unavailable".to_string()
        }
        .to_string(),
        "application error (503): unavailable"
    );
}

// -- ZBytes ergonomic conversion --

#[test]
fn zbytes_conversion_round_trip() {
    use zenoh::bytes::ZBytes;

    let err = ServiceError::Application {
        code: 1001,
        message: "quota exceeded".to_string(),
    };
    let zbytes: ZBytes = err.clone().into();
    let recovered = ServiceError::try_from(zbytes).expect("conversion failed");
    assert_eq!(recovered, err);
}

// -- Error trait is implemented --

#[test]
fn implements_std_error() {
    let err = ServiceError::Internal {
        message: "test".to_string(),
    };
    // Verify it can be used as a dyn Error
    let _: &dyn std::error::Error = &err;
}

// -- DeadlineContext tests --

#[test]
fn deadline_context_remaining_returns_positive_for_future_deadline() {
    use std::time::{SystemTime, UNIX_EPOCH};

    // Set the deadline 10 seconds from now
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_millis() as u64;
    let ctx = DeadlineContext::from_millis(now_ms + 10_000);

    let remaining = ctx.remaining();
    // Should be close to 10 seconds (allow some margin for test execution)
    assert!(
        remaining > Duration::from_secs(9),
        "remaining: {remaining:?}"
    );
    assert!(
        remaining <= Duration::from_secs(10),
        "remaining: {remaining:?}"
    );
}

#[test]
fn deadline_context_remaining_returns_zero_for_past_deadline() {
    // Set the deadline 10 seconds in the past
    use std::time::{SystemTime, UNIX_EPOCH};

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_millis() as u64;
    let ctx = DeadlineContext::from_millis(now_ms.saturating_sub(10_000));

    assert_eq!(ctx.remaining(), Duration::ZERO);
}

#[test]
fn deadline_context_is_expired_for_past_deadline() {
    // Deadline at UNIX epoch — long expired
    let ctx = DeadlineContext::from_millis(0);
    assert!(ctx.is_expired());
}

#[test]
fn deadline_context_is_not_expired_for_future_deadline() {
    use std::time::{SystemTime, UNIX_EPOCH};

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_millis() as u64;
    let ctx = DeadlineContext::from_millis(now_ms + 60_000);

    assert!(!ctx.is_expired());
}

#[test]
fn deadline_attachment_produces_valid_key_value() {
    let timeout = Duration::from_secs(5);
    let (key, value) = deadline_attachment(timeout);

    assert_eq!(key, DEADLINE_ATTACHMENT_KEY);

    // The value should be parseable as u64 and be in the future
    let deadline_ms: u64 = value.parse().expect("value should be a valid u64");

    use std::time::{SystemTime, UNIX_EPOCH};
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_millis() as u64;

    // The deadline should be approximately now + 5 seconds (allow 1 second margin)
    assert!(
        deadline_ms >= now_ms + 4_000,
        "deadline_ms {deadline_ms} should be >= now + 4s ({now_ms})"
    );
    assert!(
        deadline_ms <= now_ms + 6_000,
        "deadline_ms {deadline_ms} should be <= now + 6s ({now_ms})"
    );
}

#[test]
fn deadline_context_from_millis_roundtrip() {
    let original_ms: u64 = 1_700_000_000_000; // ~2023-11-14
    let ctx = DeadlineContext::from_millis(original_ms);
    assert_eq!(ctx.deadline_ms(), original_ms);
}
