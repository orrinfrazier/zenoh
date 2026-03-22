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

//! Typed publisher and subscriber wrappers for compile-time payload safety.
//!
//! These wrappers build on the existing [`Serialize`] and [`Deserialize`] traits
//! in zenoh-ext to provide typed pub/sub where the payload type is part of the
//! publisher/subscriber contract. Wrong types are compile errors, not runtime failures.

use std::{
    future::{IntoFuture, Ready},
    marker::PhantomData,
};

use zenoh::{
    bytes::Encoding,
    handlers::FifoChannelHandler,
    key_expr::KeyExpr,
    pubsub::{Publisher, Subscriber},
    sample::Sample,
    session::Session,
    Error, Result as ZResult, Wait,
};

use crate::{z_deserialize, z_serialize, Deserialize, Serialize, ZDeserializeError};

/// Provides a stable, cross-language identifier for typed pub/sub encoding.
///
/// Implementors choose a name that is consistent across all participants
/// (including non-Rust clients). This replaces the use of [`std::any::type_name`],
/// whose output is not guaranteed stable across compiler versions.
///
/// # Example
/// ```
/// use zenoh_ext::TypedSchema;
///
/// struct Telemetry { /* ... */ }
///
/// impl TypedSchema for Telemetry {
///     const SCHEMA_NAME: &'static str = "com.example.telemetry.v1";
/// }
/// ```
pub trait TypedSchema {
    /// A stable, unique identifier for this type's wire encoding.
    const SCHEMA_NAME: &'static str;
}

/// A publisher that only accepts payloads of type `T`.
///
/// Wraps a [`Publisher`] and serializes `T` via [`ZSerializer`](crate::ZSerializer)
/// on each `put()`. Attempting to publish a wrong type is a compile error.
pub struct TypedPublisher<'a, T: Serialize + TypedSchema> {
    inner: Publisher<'a>,
    _phantom: PhantomData<T>,
}

impl<T: Serialize + TypedSchema> TypedPublisher<'_, T> {
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
pub struct TypedSubscriber<T: Deserialize + TypedSchema> {
    inner: Subscriber<FifoChannelHandler<Sample>>,
    _phantom: PhantomData<T>,
}

impl<T: Deserialize + TypedSchema> TypedSubscriber<T> {
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
pub struct TypedPublisherBuilder<'a, 'b, T: Serialize + TypedSchema> {
    session: &'a Session,
    key_expr: ZResult<KeyExpr<'b>>,
    _phantom: PhantomData<T>,
}

impl<'b, T: Serialize + TypedSchema> TypedPublisherBuilder<'_, 'b, T> {
    fn build(self) -> ZResult<TypedPublisher<'b, T>> {
        let key_expr = self.key_expr?;
        let encoding = Encoding::from(format!("zenoh-ext/typed:{}", T::SCHEMA_NAME));
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

impl<'b, T: Serialize + TypedSchema> IntoFuture for TypedPublisherBuilder<'_, 'b, T> {
    type Output = ZResult<TypedPublisher<'b, T>>;
    type IntoFuture = Ready<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        std::future::ready(self.build())
    }
}

/// Builder for [`TypedSubscriber`].
pub struct TypedSubscriberBuilder<'a, 'b, T: Deserialize + TypedSchema> {
    session: &'a Session,
    key_expr: ZResult<KeyExpr<'b>>,
    _phantom: PhantomData<T>,
}

impl<T: Deserialize + TypedSchema> TypedSubscriberBuilder<'_, '_, T> {
    fn build(self) -> ZResult<TypedSubscriber<T>> {
        let key_expr = self.key_expr?;
        let inner = self.session.declare_subscriber(key_expr).wait()?;
        Ok(TypedSubscriber {
            inner,
            _phantom: PhantomData,
        })
    }
}

impl<T: Deserialize + TypedSchema> IntoFuture for TypedSubscriberBuilder<'_, '_, T> {
    type Output = ZResult<TypedSubscriber<T>>;
    type IntoFuture = Ready<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        std::future::ready(self.build())
    }
}

/// Extension trait for [`Session`] to declare typed publishers and subscribers.
pub trait TypedSessionExt {
    /// Declare a [`TypedPublisher`] for the given key expression.
    fn declare_typed_publisher<'b, T: Serialize + TypedSchema, TryIntoKeyExpr>(
        &self,
        key_expr: TryIntoKeyExpr,
    ) -> TypedPublisherBuilder<'_, 'b, T>
    where
        TryIntoKeyExpr: TryInto<KeyExpr<'b>>,
        <TryIntoKeyExpr as TryInto<KeyExpr<'b>>>::Error: Into<Error>;

    /// Declare a [`TypedSubscriber`] for the given key expression.
    fn declare_typed_subscriber<'b, T: Deserialize + TypedSchema, TryIntoKeyExpr>(
        &self,
        key_expr: TryIntoKeyExpr,
    ) -> TypedSubscriberBuilder<'_, 'b, T>
    where
        TryIntoKeyExpr: TryInto<KeyExpr<'b>>,
        <TryIntoKeyExpr as TryInto<KeyExpr<'b>>>::Error: Into<Error>;
}

impl TypedSessionExt for Session {
    fn declare_typed_publisher<'b, T: Serialize + TypedSchema, TryIntoKeyExpr>(
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

    fn declare_typed_subscriber<'b, T: Deserialize + TypedSchema, TryIntoKeyExpr>(
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
}
