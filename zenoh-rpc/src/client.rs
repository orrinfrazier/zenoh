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

use std::collections::HashMap;
use std::time::Duration;

use zenoh::bytes::ZBytes;
use zenoh::key_expr::KeyExpr;
use zenoh::session::Session;

use crate::deadline::deadline_attachment;
use crate::discovery::service_liveliness_prefix;
use crate::error::{ServiceError, StatusCode};
use crate::server::{METHOD_ATTACHMENT_KEY, STATUS_ATTACHMENT_KEY};
use zenoh_ext::{z_deserialize, z_serialize};

/// An RPC client that sends typed method calls to a remote [`ServiceServer`](crate::ServiceServer).
///
/// The client serializes requests, attaches method name and deadline metadata,
/// sends them via `session.get()`, and deserializes the response. Error replies
/// from the server are deserialized into [`ServiceError`] variants.
///
/// # Example
///
/// ```ignore
/// use std::time::Duration;
/// use zenoh_rpc::ServiceClient;
///
/// let client = ServiceClient::new(&session, "my/service", Duration::from_secs(5))?;
/// let response: String = client.call("get_config", &"key".to_string(), None).await?;
/// ```
pub struct ServiceClient {
    session: Session,
    key_expr: KeyExpr<'static>,
    default_timeout: Duration,
}

impl ServiceClient {
    /// Create a new `ServiceClient`.
    ///
    /// # Arguments
    ///
    /// * `session` — The zenoh session used to send queries.
    /// * `key_expr` — The key expression of the remote service.
    /// * `default_timeout` — Default timeout for calls when none is specified.
    pub fn new(
        session: &Session,
        key_expr: &str,
        default_timeout: Duration,
    ) -> zenoh::Result<Self> {
        let key_expr: KeyExpr<'static> = KeyExpr::try_from(key_expr.to_string())
            .map_err(|e| -> zenoh::Error { format!("invalid key expression: {e}").into() })?;
        Ok(Self {
            session: session.clone(),
            key_expr,
            default_timeout,
        })
    }

    /// Call a method on the remote service with typed request and response.
    ///
    /// Serializes the request using zenoh-ext `Serialize`, attaches the method
    /// name and deadline, sends via `session.get()`, and deserializes the response.
    ///
    /// # Arguments
    ///
    /// * `method` — The RPC method name (must match a handler on the server).
    /// * `request` — The request payload to serialize.
    /// * `timeout` — Optional timeout override; uses default if `None`.
    pub async fn call<Req, Resp>(
        &self,
        method: &str,
        request: &Req,
        timeout: Option<Duration>,
    ) -> Result<Resp, ServiceError>
    where
        Req: zenoh_ext::Serialize,
        Resp: zenoh_ext::Deserialize,
    {
        let payload = z_serialize(request);
        let response_bytes = self.call_raw(method, payload, timeout).await?;
        z_deserialize::<Resp>(&response_bytes).map_err(|_| ServiceError::Internal {
            message: "failed to deserialize response".to_string(),
        })
    }

    /// Call a method with raw `ZBytes` request/response (no serialization).
    ///
    /// This is useful when you want to handle serialization yourself or
    /// forward opaque payloads.
    ///
    /// # Arguments
    ///
    /// * `method` — The RPC method name.
    /// * `payload` — The raw request payload.
    /// * `timeout` — Optional timeout override; uses default if `None`.
    pub async fn call_raw(
        &self,
        method: &str,
        payload: ZBytes,
        timeout: Option<Duration>,
    ) -> Result<ZBytes, ServiceError> {
        let timeout = timeout.unwrap_or(self.default_timeout);

        // Build attachment map with method name and deadline
        let (deadline_key, deadline_value) = deadline_attachment(timeout);
        let attachment_map: HashMap<String, String> = HashMap::from([
            (METHOD_ATTACHMENT_KEY.to_string(), method.to_string()),
            (deadline_key, deadline_value),
        ]);
        let attachment = z_serialize(&attachment_map);

        // Send the query
        let replies = self
            .session
            .get(&self.key_expr)
            .payload(payload)
            .attachment(attachment)
            .timeout(timeout)
            .await
            .map_err(|e| ServiceError::Internal {
                message: format!("failed to send query: {e}"),
            })?;

        // Get the first reply
        let reply = replies
            .recv_async()
            .await
            .map_err(|e| ServiceError::Internal {
                message: format!("failed to receive reply: {e}"),
            })?;

        // Check if success or error reply.
        match reply.into_result() {
            Ok(sample) => {
                // Check status code attachment if present
                if let Some(attachment) = sample.attachment() {
                    if let Ok(map) = z_deserialize::<HashMap<String, String>>(attachment) {
                        if let Some(status_str) = map.get(STATUS_ATTACHMENT_KEY) {
                            if let Ok(code) = status_str.parse::<u8>() {
                                if code != StatusCode::Ok as u8 {
                                    // Non-OK status on a success reply — unexpected,
                                    // but treat the payload as data anyway since the
                                    // server sent it as a success reply.
                                    tracing::warn!(
                                        "ServiceClient: success reply has non-OK status code {code}"
                                    );
                                }
                            }
                        }
                    }
                }
                Ok(sample.payload().clone())
            }
            Err(reply_err) => {
                // Deserialize ServiceError from the error payload
                let err: ServiceError =
                    z_deserialize(reply_err.payload()).map_err(|_| ServiceError::Internal {
                        message: "failed to deserialize error reply".to_string(),
                    })?;
                Err(err)
            }
        }
    }

    /// Check if at least one instance of this service is currently available.
    ///
    /// Queries liveliness tokens declared by [`ServiceServer`](crate::ServiceServer)
    /// instances matching this client's key expression.
    pub async fn is_available(&self) -> zenoh::Result<bool> {
        let count = self.instance_count().await?;
        Ok(count > 0)
    }

    /// Get the count of available service instances.
    ///
    /// Queries liveliness tokens declared by [`ServiceServer`](crate::ServiceServer)
    /// instances matching this client's key expression. Each server declares a
    /// unique token keyed by its zenoh session ID, so the count reflects the
    /// number of distinct server instances.
    pub async fn instance_count(&self) -> zenoh::Result<usize> {
        let prefix = service_liveliness_prefix(self.key_expr.as_str());
        let replies = self.session.liveliness().get(&prefix).await?;

        let mut count = 0;
        while let Ok(reply) = replies.recv_async().await {
            if reply.result().is_ok() {
                count += 1;
            }
        }
        Ok(count)
    }
}
