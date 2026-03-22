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
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use zenoh::bytes::ZBytes;
use zenoh::internal::runtime::ZRuntime;
use zenoh::key_expr::KeyExpr;
use zenoh::query::{Query, Queryable};
use zenoh::session::Session;
use zenoh::Wait;

use super::deadline::DeadlineContext;
use super::error::{ServiceError, StatusCode};
use crate::{z_deserialize, z_serialize};

/// Attachment key used to identify the RPC method being invoked.
pub const METHOD_ATTACHMENT_KEY: &str = "rpc:method";

/// Status code attachment key for replies.
const STATUS_ATTACHMENT_KEY: &str = "rpc:status";

/// Trait for handling a specific RPC method.
///
/// Implementors receive the raw [`Query`] and must return either a serialized
/// response payload or a [`ServiceError`].
#[async_trait]
pub trait MethodHandler: Send + Sync {
    /// Handle an incoming query for this method.
    async fn handle(&self, query: &Query) -> Result<ZBytes, ServiceError>;
}

/// Boxed future type alias for method handler closures.
type BoxFuture<'a, T> = Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// Wrapper that adapts an async closure into a [`MethodHandler`].
///
/// The closure must return a [`BoxFuture`] to erase the lifetime of the
/// `&Query` reference, matching the `async_trait` desugaring.
struct MethodFn<F> {
    f: F,
}

#[async_trait]
impl<F> MethodHandler for MethodFn<F>
where
    F: for<'a> Fn(&'a Query) -> BoxFuture<'a, Result<ZBytes, ServiceError>> + Send + Sync,
{
    async fn handle(&self, query: &Query) -> Result<ZBytes, ServiceError> {
        (self.f)(query).await
    }
}

/// A multi-method RPC server backed by a single zenoh queryable.
///
/// The server dispatches incoming queries to registered [`MethodHandler`]s
/// based on the `rpc:method` attachment on each query. Construct one via
/// [`ServiceServer::builder`].
#[zenoh_macros::unstable]
pub struct ServiceServer {
    _queryable: Queryable<()>,
}

/// Builder for [`ServiceServer`].
///
/// Register methods via [`method`](Self::method) or [`method_fn`](Self::method_fn),
/// then `.await` (or call `.wait()`) to start the server.
#[zenoh_macros::unstable]
pub struct ServiceServerBuilder<'a, 'b> {
    session: &'a Session,
    key_expr: zenoh::Result<KeyExpr<'b>>,
    handlers: HashMap<String, Box<dyn MethodHandler>>,
}

#[zenoh_macros::unstable]
impl ServiceServer {
    /// Create a builder for a new `ServiceServer`.
    ///
    /// # Arguments
    ///
    /// * `session` — The zenoh session to declare the queryable on.
    /// * `key_expr` — The key expression the server will listen on.
    pub fn builder<'a, 'b, TryIntoKeyExpr>(
        session: &'a Session,
        key_expr: TryIntoKeyExpr,
    ) -> ServiceServerBuilder<'a, 'b>
    where
        TryIntoKeyExpr: TryInto<KeyExpr<'b>>,
        <TryIntoKeyExpr as TryInto<KeyExpr<'b>>>::Error: Into<zenoh::Error>,
    {
        ServiceServerBuilder {
            session,
            key_expr: key_expr.try_into().map_err(Into::into),
            handlers: HashMap::new(),
        }
    }
}

#[zenoh_macros::unstable]
impl<'a, 'b> ServiceServerBuilder<'a, 'b> {
    /// Register a method handler.
    ///
    /// The `name` is matched against the `rpc:method` attachment on incoming
    /// queries.
    pub fn method<H: MethodHandler + 'static>(mut self, name: &str, handler: H) -> Self {
        self.handlers.insert(name.to_string(), Box::new(handler));
        self
    }

    /// Register a method handler from an async closure.
    ///
    /// This is a convenience wrapper around [`method`](Self::method) that
    /// accepts closures returning a boxed future. Use [`Box::pin`] to wrap
    /// your async block:
    ///
    /// ```ignore
    /// .method_fn("get_config", |query| Box::pin(async move {
    ///     Ok(ZBytes::from(b"response".as_ref()))
    /// }))
    /// ```
    pub fn method_fn<F>(self, name: &str, f: F) -> Self
    where
        F: for<'q> Fn(&'q Query) -> BoxFuture<'q, Result<ZBytes, ServiceError>>
            + Send
            + Sync
            + 'static,
    {
        self.method(name, MethodFn { f })
    }

    /// Build and start the server.
    fn build(self) -> zenoh::Result<ServiceServer> {
        let key_expr = self.key_expr?;
        let handlers: Arc<HashMap<String, Box<dyn MethodHandler>>> = Arc::new(self.handlers);

        let queryable = self
            .session
            .declare_queryable(&key_expr)
            .callback({
                let handlers = handlers.clone();
                move |query| {
                    let handlers = handlers.clone();
                    // The callback is sync, so spawn the async dispatch on the
                    // application runtime.
                    drop(ZRuntime::Application.spawn(async move {
                        dispatch_query(&handlers, query).await;
                    }));
                }
            })
            .wait()?;

        Ok(ServiceServer {
            _queryable: queryable,
        })
    }
}

#[zenoh_macros::unstable]
impl std::future::IntoFuture for ServiceServerBuilder<'_, '_> {
    type Output = zenoh::Result<ServiceServer>;
    type IntoFuture = std::future::Ready<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        std::future::ready(self.build())
    }
}

#[zenoh_macros::unstable]
impl Wait for ServiceServerBuilder<'_, '_> {
    fn wait(self) -> <Self as zenoh::Resolvable>::To {
        self.build()
    }
}

#[zenoh_macros::unstable]
impl zenoh::Resolvable for ServiceServerBuilder<'_, '_> {
    type To = zenoh::Result<ServiceServer>;
}

/// Extract the method name from a query's attachment map.
fn extract_method(query: &Query) -> Option<String> {
    let attachment = query.attachment()?;
    let map: HashMap<String, String> = z_deserialize(attachment).ok()?;
    map.get(METHOD_ATTACHMENT_KEY).cloned()
}

/// Build a status-code attachment as serialized `HashMap<String, String>`.
fn status_attachment(code: StatusCode) -> ZBytes {
    let map: HashMap<String, String> = HashMap::from([(
        STATUS_ATTACHMENT_KEY.to_string(),
        (code as u8).to_string(),
    )]);
    z_serialize(&map)
}

/// Core dispatch logic: read method from attachment, look up handler, call it,
/// and reply with the result or an appropriate error.
async fn dispatch_query(handlers: &HashMap<String, Box<dyn MethodHandler>>, query: Query) {
    // 1. Extract method name from attachment
    let method = match extract_method(&query) {
        Some(m) => m,
        None => {
            let err = ServiceError::InvalidRequest {
                message: "missing rpc:method attachment".to_string(),
            };
            reply_error(&query, err).await;
            return;
        }
    };

    // 2. Look up handler
    let handler = match handlers.get(&method) {
        Some(h) => h,
        None => {
            let err = ServiceError::MethodNotFound { method };
            reply_error(&query, err).await;
            return;
        }
    };

    // 3. Check deadline if present
    if let Some(ctx) = DeadlineContext::from_query(&query) {
        if ctx.is_expired() {
            let err = ServiceError::DeadlineExceeded {
                budget_ms: ctx.deadline_ms(),
            };
            reply_error(&query, err).await;
            return;
        }
    }

    // 4. Call handler
    match handler.handle(&query).await {
        Ok(payload) => {
            reply_success(&query, payload).await;
        }
        Err(err) => {
            reply_error(&query, err).await;
        }
    }
}

/// Send a success reply with status code Ok.
async fn reply_success(query: &Query, payload: ZBytes) {
    let attachment = status_attachment(StatusCode::Ok);
    if let Err(e) = query
        .reply(query.key_expr(), payload)
        .attachment(attachment)
        .await
    {
        tracing::warn!("ServiceServer: failed to send success reply: {e}");
    }
}

/// Send an error reply with the appropriate status code.
async fn reply_error(query: &Query, err: ServiceError) {
    let code = err.status_code();
    tracing::warn!("ServiceServer: replying with error {code:?}: {err}");
    let err_payload: ZBytes = err.into();
    // ReplyErrBuilder does not support attachments, so we serialize the
    // status code into the error payload prefix. Clients can use
    // ServiceError deserialization which already includes the status code.
    if let Err(e) = query.reply_err(err_payload).await {
        tracing::warn!("ServiceServer: failed to send error reply: {e}");
    }
}
