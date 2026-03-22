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

//! Typed wrappers for compile-time payload safety.
//!
//! These wrappers build on the existing [`Serialize`] and [`Deserialize`] traits
//! in zenoh-ext to provide typed pub/sub and query/reply where the payload type
//! is part of the contract. Wrong types are compile errors, not runtime failures.

use std::{
    future::{IntoFuture, Ready},
    marker::PhantomData,
};

use zenoh::{
    bytes::{Encoding, ZBytes},
    handlers::FifoChannelHandler,
    key_expr::KeyExpr,
    pubsub::{Publisher, Subscriber},
    query::{Query, Queryable},
    sample::Sample,
    session::Session,
    Error, Result as ZResult, Wait,
};

use crate::{z_deserialize, z_serialize, Deserialize, Serialize, ZDeserializeError};

/// A publisher that only accepts payloads of type `T`.
///
/// Wraps a [`Publisher`] and serializes `T` via [`ZSerializer`](crate::ZSerializer)
/// on each `put()`. Attempting to publish a wrong type is a compile error.
pub struct TypedPublisher<'a, T: Serialize> {
    inner: Publisher<'a>,
    _phantom: PhantomData<T>,
}

impl<T: Serialize> TypedPublisher<'_, T> {
    /// Publish a typed payload.
    pub async fn put(&self, payload: &T) -> ZResult<()> {
        let zbytes = z_serialize(payload);
        self.inner.put(zbytes).await
    }

    /// Returns the [`KeyExpr`] this publisher publishes to.
    pub fn key_expr(&self) -> &KeyExpr<'_> {
        self.inner.key_expr()
    }

    /// Returns the [`Encoding`] set on this publisher.
    pub fn encoding(&self) -> &Encoding {
        self.inner.encoding()
    }
}

/// A subscriber that deserializes incoming payloads into type `T`.
///
/// Wraps a [`Subscriber`] and yields `Result<T, ZDeserializeError>` on each
/// received sample. Malformed payloads yield `Err`, never panic.
pub struct TypedSubscriber<T: Deserialize> {
    inner: Subscriber<FifoChannelHandler<Sample>>,
    _phantom: PhantomData<T>,
}

impl<T: Deserialize> TypedSubscriber<T> {
    /// Wait for an incoming typed message.
    ///
    /// The outer `ZResult` fails only if the channel is closed.
    /// The inner `Result` indicates deserialization success/failure.
    pub async fn recv_async(&self) -> ZResult<Result<T, ZDeserializeError>> {
        let sample = self.inner.recv_async().await?;
        Ok(z_deserialize::<T>(sample.payload()))
    }

    /// Blocking receive for an incoming typed message.
    pub fn recv(&self) -> ZResult<Result<T, ZDeserializeError>> {
        let sample = self.inner.recv()?;
        Ok(z_deserialize::<T>(sample.payload()))
    }

    /// Returns the [`KeyExpr`] this subscriber is subscribed to.
    pub fn key_expr(&self) -> &KeyExpr<'static> {
        self.inner.key_expr()
    }
}

/// Builder for [`TypedPublisher`].
pub struct TypedPublisherBuilder<'a, 'b, T: Serialize> {
    session: &'a Session,
    key_expr: ZResult<KeyExpr<'b>>,
    _phantom: PhantomData<T>,
}

impl<'b, T: Serialize> TypedPublisherBuilder<'_, 'b, T> {
    fn build(self) -> ZResult<TypedPublisher<'b, T>> {
        let key_expr = self.key_expr?;
        let encoding =
            Encoding::from(format!("zenoh-ext/typed:{}", std::any::type_name::<T>()));
        let inner = self
            .session
            .declare_publisher(key_expr)
            .encoding(encoding)
            .wait()?;
        Ok(TypedPublisher {
            inner,
            _phantom: PhantomData,
        })
    }
}

impl<'b, T: Serialize> IntoFuture for TypedPublisherBuilder<'_, 'b, T> {
    type Output = ZResult<TypedPublisher<'b, T>>;
    type IntoFuture = Ready<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        std::future::ready(self.build())
    }
}

/// Builder for [`TypedSubscriber`].
pub struct TypedSubscriberBuilder<'a, 'b, T: Deserialize> {
    session: &'a Session,
    key_expr: ZResult<KeyExpr<'b>>,
    _phantom: PhantomData<T>,
}

impl<T: Deserialize> TypedSubscriberBuilder<'_, '_, T> {
    fn build(self) -> ZResult<TypedSubscriber<T>> {
        let key_expr = self.key_expr?;
        let inner = self.session.declare_subscriber(key_expr).wait()?;
        Ok(TypedSubscriber {
            inner,
            _phantom: PhantomData,
        })
    }
}

impl<T: Deserialize> IntoFuture for TypedSubscriberBuilder<'_, '_, T> {
    type Output = ZResult<TypedSubscriber<T>>;
    type IntoFuture = Ready<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        std::future::ready(self.build())
    }
}

// -- Typed Query/Reply --

/// A typed query received by a [`TypedQueryable`].
///
/// Wraps a [`Query`] with typed deserialization of the request payload
/// and typed serialization of the reply.
pub struct TypedQuery<Req: Deserialize, Resp: Serialize> {
    inner: Query,
    _phantom: PhantomData<(Req, Resp)>,
}

impl<Req: Deserialize, Resp: Serialize> TypedQuery<Req, Resp> {
    /// Attempt to deserialize the query payload into the request type.
    ///
    /// Returns `Err` if the query has no payload or if deserialization fails.
    pub fn request(&self) -> Result<Req, ZDeserializeError> {
        match self.inner.payload() {
            Some(payload) => z_deserialize::<Req>(payload),
            None => Err(ZDeserializeError),
        }
    }

    /// Reply to this query with a typed response.
    pub async fn reply(&self, resp: &Resp) -> ZResult<()> {
        let zbytes = z_serialize(resp);
        self.inner.reply(self.inner.key_expr(), zbytes).await
    }

    /// Reply with an error payload.
    pub async fn reply_err<IntoZBytes: Into<ZBytes>>(&self, payload: IntoZBytes) -> ZResult<()> {
        self.inner.reply_err(payload).await
    }

    /// Access the underlying [`Query`] for metadata (key_expr, parameters, etc.).
    pub fn query(&self) -> &Query {
        &self.inner
    }
}

/// A queryable that yields typed queries.
///
/// Wraps a [`Queryable`] and yields [`TypedQuery<Req, Resp>`] on each
/// incoming query. Request payloads are deserialized into `Req`,
/// and `reply()` serializes `Resp`.
pub struct TypedQueryable<Req: Deserialize, Resp: Serialize> {
    inner: Queryable<FifoChannelHandler<Query>>,
    _phantom: PhantomData<(Req, Resp)>,
}

impl<Req: Deserialize, Resp: Serialize> TypedQueryable<Req, Resp> {
    /// Wait for an incoming typed query.
    pub async fn recv_async(&self) -> ZResult<TypedQuery<Req, Resp>> {
        let query = self.inner.recv_async().await?;
        Ok(TypedQuery {
            inner: query,
            _phantom: PhantomData,
        })
    }

    /// Blocking receive for an incoming typed query.
    pub fn recv(&self) -> ZResult<TypedQuery<Req, Resp>> {
        let query = self.inner.recv()?;
        Ok(TypedQuery {
            inner: query,
            _phantom: PhantomData,
        })
    }
}

/// Builder for [`TypedQueryable`].
pub struct TypedQueryableBuilder<'a, 'b, Req: Deserialize, Resp: Serialize> {
    session: &'a Session,
    key_expr: ZResult<KeyExpr<'b>>,
    _phantom: PhantomData<(Req, Resp)>,
}

impl<Req: Deserialize, Resp: Serialize> TypedQueryableBuilder<'_, '_, Req, Resp> {
    fn build(self) -> ZResult<TypedQueryable<Req, Resp>> {
        let key_expr = self.key_expr?;
        let inner = self.session.declare_queryable(key_expr).wait()?;
        Ok(TypedQueryable {
            inner,
            _phantom: PhantomData,
        })
    }
}

impl<Req: Deserialize, Resp: Serialize> IntoFuture for TypedQueryableBuilder<'_, '_, Req, Resp> {
    type Output = ZResult<TypedQueryable<Req, Resp>>;
    type IntoFuture = Ready<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        std::future::ready(self.build())
    }
}

/// A future that resolves to a vector of typed reply results.
///
/// Returned by [`TypedSessionExt::typed_get`].
pub struct TypedGetFuture<Resp: Deserialize> {
    result: ZResult<Vec<Result<Resp, ZDeserializeError>>>,
}

impl<Resp: Deserialize> std::future::IntoFuture for TypedGetFuture<Resp> {
    type Output = ZResult<Vec<Result<Resp, ZDeserializeError>>>;
    type IntoFuture = Ready<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        std::future::ready(self.result)
    }
}

/// Extension trait for [`Session`] to declare typed publishers, subscribers, and queryables.
pub trait TypedSessionExt {
    /// Declare a [`TypedPublisher`] for the given key expression.
    fn declare_typed_publisher<'b, T: Serialize, TryIntoKeyExpr>(
        &self,
        key_expr: TryIntoKeyExpr,
    ) -> TypedPublisherBuilder<'_, 'b, T>
    where
        TryIntoKeyExpr: TryInto<KeyExpr<'b>>,
        <TryIntoKeyExpr as TryInto<KeyExpr<'b>>>::Error: Into<Error>;

    /// Declare a [`TypedSubscriber`] for the given key expression.
    fn declare_typed_subscriber<'b, T: Deserialize, TryIntoKeyExpr>(
        &self,
        key_expr: TryIntoKeyExpr,
    ) -> TypedSubscriberBuilder<'_, 'b, T>
    where
        TryIntoKeyExpr: TryInto<KeyExpr<'b>>,
        <TryIntoKeyExpr as TryInto<KeyExpr<'b>>>::Error: Into<Error>;

    /// Declare a [`TypedQueryable`] for the given key expression.
    fn declare_typed_queryable<'b, Req: Deserialize, Resp: Serialize, TryIntoKeyExpr>(
        &self,
        key_expr: TryIntoKeyExpr,
    ) -> TypedQueryableBuilder<'_, 'b, Req, Resp>
    where
        TryIntoKeyExpr: TryInto<KeyExpr<'b>>,
        <TryIntoKeyExpr as TryInto<KeyExpr<'b>>>::Error: Into<Error>;

    /// Send a typed get (query) and collect typed replies.
    ///
    /// Serializes `Req` into the query payload, sends the query, and
    /// deserializes each reply into `Resp`.
    fn typed_get<'b, Req: Serialize, Resp: Deserialize, TryIntoKeyExpr>(
        &self,
        key_expr: TryIntoKeyExpr,
        request: &Req,
    ) -> TypedGetFuture<Resp>
    where
        TryIntoKeyExpr: TryInto<KeyExpr<'b>>,
        <TryIntoKeyExpr as TryInto<KeyExpr<'b>>>::Error: Into<Error>;
}

impl TypedSessionExt for Session {
    fn declare_typed_publisher<'b, T: Serialize, TryIntoKeyExpr>(
        &self,
        key_expr: TryIntoKeyExpr,
    ) -> TypedPublisherBuilder<'_, 'b, T>
    where
        TryIntoKeyExpr: TryInto<KeyExpr<'b>>,
        <TryIntoKeyExpr as TryInto<KeyExpr<'b>>>::Error: Into<Error>,
    {
        TypedPublisherBuilder {
            session: self,
            key_expr: key_expr.try_into().map_err(Into::into),
            _phantom: PhantomData,
        }
    }

    fn declare_typed_subscriber<'b, T: Deserialize, TryIntoKeyExpr>(
        &self,
        key_expr: TryIntoKeyExpr,
    ) -> TypedSubscriberBuilder<'_, 'b, T>
    where
        TryIntoKeyExpr: TryInto<KeyExpr<'b>>,
        <TryIntoKeyExpr as TryInto<KeyExpr<'b>>>::Error: Into<Error>,
    {
        TypedSubscriberBuilder {
            session: self,
            key_expr: key_expr.try_into().map_err(Into::into),
            _phantom: PhantomData,
        }
    }

    fn declare_typed_queryable<'b, Req: Deserialize, Resp: Serialize, TryIntoKeyExpr>(
        &self,
        key_expr: TryIntoKeyExpr,
    ) -> TypedQueryableBuilder<'_, 'b, Req, Resp>
    where
        TryIntoKeyExpr: TryInto<KeyExpr<'b>>,
        <TryIntoKeyExpr as TryInto<KeyExpr<'b>>>::Error: Into<Error>,
    {
        TypedQueryableBuilder {
            session: self,
            key_expr: key_expr.try_into().map_err(Into::into),
            _phantom: PhantomData,
        }
    }

    fn typed_get<'b, Req: Serialize, Resp: Deserialize, TryIntoKeyExpr>(
        &self,
        key_expr: TryIntoKeyExpr,
        request: &Req,
    ) -> TypedGetFuture<Resp>
    where
        TryIntoKeyExpr: TryInto<KeyExpr<'b>>,
        <TryIntoKeyExpr as TryInto<KeyExpr<'b>>>::Error: Into<Error>,
    {
        let result = (|| -> ZResult<Vec<Result<Resp, ZDeserializeError>>> {
            let key_expr = key_expr.try_into().map_err(Into::into)?;
            let payload = z_serialize(request);
            let receiver = self.get(key_expr).payload(payload).wait()?;
            let mut replies = Vec::new();
            while let Ok(reply) = receiver.recv() {
                match reply.into_result() {
                    Ok(sample) => {
                        replies.push(z_deserialize::<Resp>(sample.payload()));
                    }
                    Err(_reply_err) => {
                        // Reply errors are not deserialization errors — skip them
                    }
                }
            }
            Ok(replies)
        })();
        TypedGetFuture { result }
    }
}
