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

use std::fmt;

use zenoh::bytes::ZBytes;

use crate::{
    z_deserialize, z_serialize, Deserialize, Serialize, ZDeserializeError, ZDeserializer,
    ZSerializer,
};

/// Status codes for fast-path error detection via attachments.
///
/// Each variant maps to a single byte discriminant, enabling efficient
/// status checks without deserializing the full error payload.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusCode {
    /// The request completed successfully.
    Ok = 0,
    /// The request payload was malformed or invalid.
    InvalidRequest = 1,
    /// The requested resource was not found.
    NotFound = 2,
    /// The request exceeded its time budget.
    DeadlineExceeded = 3,
    /// An internal service error occurred.
    Internal = 4,
    /// The requested RPC method does not exist.
    MethodNotFound = 5,
    /// An application-defined error with a custom code.
    Application = 6,
}

impl StatusCode {
    /// Try to convert a raw `u8` discriminant into a [`StatusCode`].
    fn from_u8(v: u8) -> Result<Self, ZDeserializeError> {
        match v {
            0 => Ok(Self::Ok),
            1 => Ok(Self::InvalidRequest),
            2 => Ok(Self::NotFound),
            3 => Ok(Self::DeadlineExceeded),
            4 => Ok(Self::Internal),
            5 => Ok(Self::MethodNotFound),
            6 => Ok(Self::Application),
            _ => Err(ZDeserializeError),
        }
    }
}

impl Serialize for StatusCode {
    fn serialize(&self, serializer: &mut ZSerializer) {
        (*self as u8).serialize(serializer);
    }
}

impl Deserialize for StatusCode {
    fn deserialize(deserializer: &mut ZDeserializer) -> Result<Self, ZDeserializeError> {
        Self::from_u8(u8::deserialize(deserializer)?)
    }
}

/// Structured error type for the RPC layer.
///
/// Each variant carries the data needed to diagnose the failure.
/// The wire format is: `status_code: u8` followed by variant-specific fields,
/// using the same serialization conventions as the rest of zenoh-ext.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServiceError {
    /// The request payload was malformed or failed validation.
    InvalidRequest { message: String },
    /// The requested resource (key, entity, etc.) was not found.
    NotFound { message: String },
    /// The request exceeded its time budget.
    DeadlineExceeded { budget_ms: u64 },
    /// An unexpected internal error.
    Internal { message: String },
    /// The named RPC method does not exist on this service.
    MethodNotFound { method: String },
    /// An application-defined error carrying a custom numeric code.
    Application { code: u32, message: String },
}

impl ServiceError {
    /// Returns the [`StatusCode`] corresponding to this error variant.
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::InvalidRequest { .. } => StatusCode::InvalidRequest,
            Self::NotFound { .. } => StatusCode::NotFound,
            Self::DeadlineExceeded { .. } => StatusCode::DeadlineExceeded,
            Self::Internal { .. } => StatusCode::Internal,
            Self::MethodNotFound { .. } => StatusCode::MethodNotFound,
            Self::Application { .. } => StatusCode::Application,
        }
    }
}

impl fmt::Display for ServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { message } => write!(f, "invalid request: {message}"),
            Self::NotFound { message } => write!(f, "not found: {message}"),
            Self::DeadlineExceeded { budget_ms } => {
                write!(f, "deadline exceeded: budget was {budget_ms}ms")
            }
            Self::Internal { message } => write!(f, "internal error: {message}"),
            Self::MethodNotFound { method } => write!(f, "method not found: {method}"),
            Self::Application { code, message } => {
                write!(f, "application error ({code}): {message}")
            }
        }
    }
}

impl std::error::Error for ServiceError {}

impl Serialize for ServiceError {
    fn serialize(&self, serializer: &mut ZSerializer) {
        // Discriminant byte
        self.status_code().serialize(serializer);
        // Variant-specific fields
        match self {
            Self::InvalidRequest { message } => serializer.serialize(message),
            Self::NotFound { message } => serializer.serialize(message),
            Self::DeadlineExceeded { budget_ms } => serializer.serialize(*budget_ms),
            Self::Internal { message } => serializer.serialize(message),
            Self::MethodNotFound { method } => serializer.serialize(method),
            Self::Application { code, message } => {
                serializer.serialize(*code);
                serializer.serialize(message);
            }
        }
    }
}

impl Deserialize for ServiceError {
    fn deserialize(deserializer: &mut ZDeserializer) -> Result<Self, ZDeserializeError> {
        let code = StatusCode::deserialize(deserializer)?;
        match code {
            StatusCode::Ok => Err(ZDeserializeError), // Ok is not an error variant
            StatusCode::InvalidRequest => Ok(Self::InvalidRequest {
                message: deserializer.deserialize()?,
            }),
            StatusCode::NotFound => Ok(Self::NotFound {
                message: deserializer.deserialize()?,
            }),
            StatusCode::DeadlineExceeded => Ok(Self::DeadlineExceeded {
                budget_ms: deserializer.deserialize()?,
            }),
            StatusCode::Internal => Ok(Self::Internal {
                message: deserializer.deserialize()?,
            }),
            StatusCode::MethodNotFound => Ok(Self::MethodNotFound {
                method: deserializer.deserialize()?,
            }),
            StatusCode::Application => Ok(Self::Application {
                code: deserializer.deserialize()?,
                message: deserializer.deserialize()?,
            }),
        }
    }
}

impl From<ServiceError> for ZBytes {
    fn from(err: ServiceError) -> Self {
        z_serialize(&err)
    }
}

impl TryFrom<ZBytes> for ServiceError {
    type Error = ZDeserializeError;

    fn try_from(zbytes: ZBytes) -> Result<Self, Self::Error> {
        z_deserialize(&zbytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: serialize then deserialize a ServiceError, assert round-trip equality.
    fn round_trip(err: &ServiceError) -> ServiceError {
        let zbytes = z_serialize(err);
        z_deserialize::<ServiceError>(&zbytes).expect("round-trip deserialization failed")
    }

    // -- Round-trip tests for each variant --

    #[test]
    fn round_trip_invalid_request() {
        let err = ServiceError::InvalidRequest {
            message: "missing field 'id'".to_string(),
        };
        assert_eq!(round_trip(&err), err);
    }

    #[test]
    fn round_trip_not_found() {
        let err = ServiceError::NotFound {
            message: "device 42 not found".to_string(),
        };
        assert_eq!(round_trip(&err), err);
    }

    #[test]
    fn round_trip_deadline_exceeded() {
        let err = ServiceError::DeadlineExceeded { budget_ms: 5000 };
        assert_eq!(round_trip(&err), err);
    }

    #[test]
    fn round_trip_internal() {
        let err = ServiceError::Internal {
            message: "database connection lost".to_string(),
        };
        assert_eq!(round_trip(&err), err);
    }

    #[test]
    fn round_trip_method_not_found() {
        let err = ServiceError::MethodNotFound {
            method: "getDeviceConfig".to_string(),
        };
        assert_eq!(round_trip(&err), err);
    }

    #[test]
    fn round_trip_application() {
        let err = ServiceError::Application {
            code: 1001,
            message: "quota exceeded".to_string(),
        };
        assert_eq!(round_trip(&err), err);
    }

    // -- status_code() tests --

    #[test]
    fn status_code_mapping() {
        assert_eq!(
            ServiceError::InvalidRequest {
                message: String::new()
            }
            .status_code(),
            StatusCode::InvalidRequest
        );
        assert_eq!(
            ServiceError::NotFound {
                message: String::new()
            }
            .status_code(),
            StatusCode::NotFound
        );
        assert_eq!(
            ServiceError::DeadlineExceeded { budget_ms: 0 }.status_code(),
            StatusCode::DeadlineExceeded
        );
        assert_eq!(
            ServiceError::Internal {
                message: String::new()
            }
            .status_code(),
            StatusCode::Internal
        );
        assert_eq!(
            ServiceError::MethodNotFound {
                method: String::new()
            }
            .status_code(),
            StatusCode::MethodNotFound
        );
        assert_eq!(
            ServiceError::Application {
                code: 0,
                message: String::new()
            }
            .status_code(),
            StatusCode::Application
        );
    }

    // -- Display tests --

    #[test]
    fn display_formatting() {
        assert_eq!(
            ServiceError::InvalidRequest {
                message: "bad input".to_string()
            }
            .to_string(),
            "invalid request: bad input"
        );
        assert_eq!(
            ServiceError::NotFound {
                message: "key xyz".to_string()
            }
            .to_string(),
            "not found: key xyz"
        );
        assert_eq!(
            ServiceError::DeadlineExceeded { budget_ms: 3000 }.to_string(),
            "deadline exceeded: budget was 3000ms"
        );
        assert_eq!(
            ServiceError::Internal {
                message: "crash".to_string()
            }
            .to_string(),
            "internal error: crash"
        );
        assert_eq!(
            ServiceError::MethodNotFound {
                method: "foo".to_string()
            }
            .to_string(),
            "method not found: foo"
        );
        assert_eq!(
            ServiceError::Application {
                code: 42,
                message: "nope".to_string()
            }
            .to_string(),
            "application error (42): nope"
        );
    }

    // -- Ergonomic conversion tests --

    #[test]
    fn zbytes_from_service_error() {
        let err = ServiceError::Internal {
            message: "oops".to_string(),
        };
        let zbytes: ZBytes = err.clone().into();
        let recovered = ServiceError::try_from(zbytes).expect("conversion failed");
        assert_eq!(recovered, err);
    }

    // -- Edge cases --

    #[test]
    fn deserialize_ok_status_code_is_error() {
        // Manually serialize a StatusCode::Ok byte — this should fail to deserialize as ServiceError
        let zbytes = z_serialize(&(StatusCode::Ok as u8));
        assert!(z_deserialize::<ServiceError>(&zbytes).is_err());
    }

    #[test]
    fn deserialize_invalid_discriminant_is_error() {
        let zbytes = z_serialize(&255u8);
        assert!(z_deserialize::<ServiceError>(&zbytes).is_err());
    }

    #[test]
    fn round_trip_empty_strings() {
        let err = ServiceError::InvalidRequest {
            message: String::new(),
        };
        assert_eq!(round_trip(&err), err);

        let err = ServiceError::Application {
            code: 0,
            message: String::new(),
        };
        assert_eq!(round_trip(&err), err);
    }

    #[test]
    fn round_trip_large_budget() {
        let err = ServiceError::DeadlineExceeded {
            budget_ms: u64::MAX,
        };
        assert_eq!(round_trip(&err), err);
    }

    #[test]
    fn status_code_round_trip() {
        for code in [
            StatusCode::Ok,
            StatusCode::InvalidRequest,
            StatusCode::NotFound,
            StatusCode::DeadlineExceeded,
            StatusCode::Internal,
            StatusCode::MethodNotFound,
            StatusCode::Application,
        ] {
            let zbytes = z_serialize(&code);
            let recovered = z_deserialize::<StatusCode>(&zbytes).expect("status code round-trip");
            assert_eq!(recovered, code);
        }
    }
}
