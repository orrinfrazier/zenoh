//
// Copyright (c) 2023 ZettaScale Technology
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

//! Acknowledged put/delete helpers for confirmed storage writes.
//!
//! These functions use the existing query/reply path (`session.get()`) to send
//! writes to storage queryables and wait for a persistence confirmation. The
//! storage handler is expected to detect the `_ack_put` / `_ack_delete` query
//! parameters, perform the write, and reply with a single-byte
//! [`StorageInsertionResult`] payload.

use std::time::Duration;

use zenoh::{
    bytes::{Encoding, ZBytes},
    key_expr::KeyExpr,
    query::{Reply, Selector},
    Result as ZResult, Session,
};
pub use zenoh_backend_traits::StorageInsertionResult;

/// Query parameter indicating an acknowledged put.
const ACK_PUT_PARAM: &str = "_ack_put=true";

/// Query parameter indicating an acknowledged delete.
const ACK_DELETE_PARAM: &str = "_ack_delete=true";

/// Parse a single-byte reply payload into a [`StorageInsertionResult`].
fn parse_ack_reply(reply: Reply) -> ZResult<StorageInsertionResult> {
    let sample = reply.into_result().map_err(|err| -> zenoh::Error {
        let payload = err.payload().to_bytes();
        let len = payload.len();
        let preview: &[u8] = if len <= 64 { &payload } else { &payload[..64] };
        format!("ack reply error ({len} bytes): {preview:?}").into()
    })?;
    let bytes = sample.payload().to_bytes();
    if bytes.len() != 1 {
        return Err(format!(
            "expected 1-byte ack reply payload, got {} bytes",
            bytes.len()
        )
        .into());
    }
    StorageInsertionResult::try_from(bytes[0])
}

/// Send a put with acknowledgment, waiting for confirmation that the storage
/// backend has durably written the data.
///
/// Internally calls `session.get()` with a `_ack_put=true` query parameter and
/// the provided payload.  The storage queryable is expected to perform the write
/// and reply with a single byte encoding the [`StorageInsertionResult`].
///
/// Only the first reply is used.  If multiple storage backends reply, the
/// extras are dropped.
pub async fn ack_put(
    session: &Session,
    key_expr: &KeyExpr<'_>,
    payload: impl Into<ZBytes>,
    encoding: Encoding,
    timeout: Duration,
) -> ZResult<StorageInsertionResult> {
    let selector = Selector::owned(key_expr.clone(), ACK_PUT_PARAM);
    let replies = session
        .get(selector)
        .payload(payload)
        .encoding(encoding)
        .timeout(timeout)
        .await?;

    let reply: Reply = replies.recv_async().await.map_err(|_| -> zenoh::Error {
        "no ack reply received (timeout or channel closed)".into()
    })?;

    parse_ack_reply(reply)
}

/// Send a delete with acknowledgment, waiting for confirmation that the storage
/// backend has durably deleted the data.
///
/// Internally calls `session.get()` with a `_ack_delete=true` query parameter.
/// The storage queryable is expected to perform the delete and reply with a
/// single byte encoding the [`StorageInsertionResult`].
///
/// Only the first reply is used.  If multiple storage backends reply, the
/// extras are dropped.
pub async fn ack_delete(
    session: &Session,
    key_expr: &KeyExpr<'_>,
    timeout: Duration,
) -> ZResult<StorageInsertionResult> {
    let selector = Selector::owned(key_expr.clone(), ACK_DELETE_PARAM);
    let replies = session.get(selector).timeout(timeout).await?;

    let reply: Reply = replies.recv_async().await.map_err(|_| -> zenoh::Error {
        "no ack reply received (timeout or channel closed)".into()
    })?;

    parse_ack_reply(reply)
}
