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
    handlers::{DefaultHandler, FifoChannelHandler},
    key_expr::KeyExpr,
    pubsub::{Publisher, Subscriber},
    query::{Query, Queryable},
    sample::Sample,
    session::Session,
    Error, Result as ZResult, Wait,
};

use crate::{z_deserialize, z_serialize, Deserialize, Serialize, ZDeserializeError};

/// Trait for types that provide a stable, user-defined schema name.
///
/// `std::any::type_name` is not guaranteed stable across Rust compiler versions,
/// so types used with [`TypedPublisher`] and [`TypedSubscriber`] must implement
/// this trait to provide a fixed identifier for wire encoding and error messages.
///
/// The schema name is embedded in the Zenoh [`Encoding`](zenoh::bytes::Encoding)
/// as `zenoh-ext/typed:{SCHEMA_NAME}`, making it cross-language compatible
/// (Python/C++ clients can match on the same string).
///
/// # Example
/// ```ignore
/// impl TypedSchema for TelemetryPayload {
///     const SCHEMA_NAME: &'static str = "telemetry-payload";
/// }
/// ```
pub trait TypedSchema {
    /// A stable, human-readable name for this type's schema.
    ///
    /// Must be unique per type within your application and stable across
    /// compiler versions and languages.
    const SCHEMA_NAME: &'static str;
}

/// Error type for typed receive operations that include encoding and version checking.
#[derive(Debug)]
pub enum TypedReceiveError {
    /// The sample's encoding doesn't match the expected `zenoh-ext/typed:{SCHEMA_NAME}`.
    EncodingMismatch { expected: String, received: String },
    /// The publisher's schema version doesn't match the subscriber's expected version.
    VersionMismatch {
        expected: u32,
        received: u32,
        type_name: String,
    },
    /// The payload could not be deserialized into the expected type.
    DeserializationFailed(ZDeserializeError),
}

impl std::fmt::Display for TypedReceiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EncodingMismatch { expected, received } => write!(
                f,
                "encoding mismatch: expected {expected}, received {received}"
            ),
            Self::VersionMismatch {
                expected,
                received,
                type_name,
            } => write!(
                f,
                "schema version mismatch for {type_name}: expected {expected}, received {received}"
            ),
            Self::DeserializationFailed(e) => write!(f, "deserialization failed: {e}"),
        }
    }
}

impl std::error::Error for TypedReceiveError {}

impl From<ZDeserializeError> for TypedReceiveError {
    fn from(e: ZDeserializeError) -> Self {
        Self::DeserializationFailed(e)
    }
}

/// Encode a schema version into a ZBytes attachment value.
fn encode_schema_version(version: u32) -> ZBytes {
    ZBytes::from(version.to_le_bytes().to_vec())
}

/// Decode a schema version from a ZBytes attachment value.
fn decode_schema_version(zbytes: &ZBytes) -> Option<u32> {
    let bytes = zbytes.to_bytes();
    if bytes.len() == 4 {
        Some(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    } else {
        None
    }
}

/// Check if a sample's attachment carries a matching schema version.
/// Returns Ok(()) if version matches or no version checking is configured.
/// Returns Err(TypedReceiveError::VersionMismatch) on mismatch.
fn check_schema_version(
    sample: &Sample,
    expected_version: Option<u32>,
    schema_name: &str,
) -> Result<(), TypedReceiveError> {
    let expected = match expected_version {
        Some(v) => v,
        None => return Ok(()), // No version checking configured
    };

    // Check if sample has a version attachment
    if let Some(attachment) = sample.attachment() {
        if let Some(received) = decode_schema_version(attachment) {
            if received != expected {
                return Err(TypedReceiveError::VersionMismatch {
                    expected,
                    received,
                    type_name: schema_name.to_string(),
                });
            }
        }
    }
    // No attachment = no version check (backward compatible)
    Ok(())
}

/// Check that a sample's encoding matches the expected typed encoding.
/// Returns `Ok(())` if the encoding matches `zenoh-ext/typed:{schema_name}`.
/// Returns `Err(TypedReceiveError::EncodingMismatch)` if it does not match.
fn check_encoding(sample: &Sample, schema_name: &str) -> Result<(), TypedReceiveError> {
    let expected = Encoding::from(format!("zenoh-ext/typed:{}", schema_name));
    let received = sample.encoding();
    if *received != expected {
        return Err(TypedReceiveError::EncodingMismatch {
            expected: expected.to_string(),
            received: received.to_string(),
        });
    }
    Ok(())
}

/// A publisher that only accepts payloads of type `T`.
///
/// Wraps a [`Publisher`] and serializes `T` via [`ZSerializer`](crate::ZSerializer)
/// on each `put()`. Attempting to publish a wrong type is a compile error.
pub struct TypedPublisher<'a, T: Serialize + TypedSchema> {
    inner: Publisher<'a>,
    schema_version: Option<u32>,
    _phantom: PhantomData<T>,
}

impl<T: Serialize + TypedSchema> TypedPublisher<'_, T> {
    /// Publish a typed payload.
    pub async fn put(&self, payload: &T) -> ZResult<()> {
        let zbytes = z_serialize(payload);
        let mut builder = self.inner.put(zbytes);
        if let Some(version) = self.schema_version {
            builder = builder.attachment(encode_schema_version(version));
        }
        builder.await
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
///
/// The `Handler` type parameter defaults to [`FifoChannelHandler<Sample>`].
/// Use [`TypedSubscriberBuilder::with`] to specify a custom handler.
pub struct TypedSubscriber<T: Deserialize + TypedSchema, Handler = FifoChannelHandler<Sample>> {
    inner: Subscriber<Handler>,
    schema_version: Option<u32>,
    _phantom: PhantomData<T>,
}

impl<T: Deserialize + TypedSchema> TypedSubscriber<T, FifoChannelHandler<Sample>> {
    /// Wait for an incoming typed message.
    ///
    /// The outer `ZResult` fails only if the channel is closed.
    /// The inner `Result` indicates deserialization success/failure,
    /// or a version mismatch if schema versioning is configured.
    pub async fn recv_async(&self) -> ZResult<Result<T, TypedReceiveError>> {
        let sample = self.inner.recv_async().await?;
        Ok(self.deserialize_sample(&sample))
    }

    /// Blocking receive for an incoming typed message.
    pub fn recv(&self) -> ZResult<Result<T, TypedReceiveError>> {
        let sample = self.inner.recv()?;
        Ok(self.deserialize_sample(&sample))
    }
}

impl<T: Deserialize + TypedSchema, Handler> TypedSubscriber<T, Handler> {
    fn deserialize_sample(&self, sample: &Sample) -> Result<T, TypedReceiveError> {
        check_encoding(sample, T::SCHEMA_NAME)?;
        check_schema_version(sample, self.schema_version, T::SCHEMA_NAME)?;
        z_deserialize::<T>(sample.payload()).map_err(TypedReceiveError::from)
    }
}

impl<T: Deserialize + TypedSchema, Handler> TypedSubscriber<T, Handler> {
    /// Returns the [`KeyExpr`] this subscriber is subscribed to.
    pub fn key_expr(&self) -> &KeyExpr<'static> {
        self.inner.key_expr()
    }

    /// Returns a reference to the inner handler.
    pub fn handler(&self) -> &Handler {
        self.inner.handler()
    }
}

/// Builder for [`TypedPublisher`].
pub struct TypedPublisherBuilder<'a, 'b, T: Serialize + TypedSchema> {
    session: &'a Session,
    key_expr: ZResult<KeyExpr<'b>>,
    schema_version: Option<u32>,
    _phantom: PhantomData<T>,
}

impl<'a, 'b, T: Serialize + TypedSchema> TypedPublisherBuilder<'a, 'b, T> {
    /// Set the schema version for this publisher.
    ///
    /// When set, the version is attached to every publication as metadata.
    /// Subscribers with a matching expected version will accept the message;
    /// subscribers expecting a different version will reject it.
    pub fn schema_version(mut self, version: u32) -> Self {
        self.schema_version = Some(version);
        self
    }

    fn build(self) -> ZResult<TypedPublisher<'b, T>> {
        let key_expr = self.key_expr?;
        debug_assert!(
            !T::SCHEMA_NAME.is_empty(),
            "TypedSchema::SCHEMA_NAME must not be empty"
        );
        let encoding = Encoding::from(format!("zenoh-ext/typed:{}", T::SCHEMA_NAME));
        let inner = self
            .session
            .declare_publisher(key_expr)
            .encoding(encoding)
            .wait()?;
        Ok(TypedPublisher {
            inner,
            schema_version: self.schema_version,
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
///
/// By default, uses [`FifoChannelHandler`] (the zenoh default handler).
/// Use [`.with(handler)`](TypedSubscriberBuilder::with) to specify a custom handler.
pub struct TypedSubscriberBuilder<'a, 'b, T: Deserialize + TypedSchema, Handler = DefaultHandler> {
    session: &'a Session,
    key_expr: ZResult<KeyExpr<'b>>,
    handler: Handler,
    schema_version: Option<u32>,
    _phantom: PhantomData<T>,
}

impl<'a, 'b, T: Deserialize + TypedSchema, Handler> TypedSubscriberBuilder<'a, 'b, T, Handler> {
    /// Set the expected schema version for this subscriber.
    ///
    /// When set, incoming messages with a mismatched version attachment
    /// will yield [`TypedReceiveError::VersionMismatch`] without attempting
    /// deserialization. Messages without a version attachment are accepted
    /// (backward compatible).
    pub fn schema_version(mut self, version: u32) -> Self {
        self.schema_version = Some(version);
        self
    }
}

impl<'a, 'b, T: Deserialize + TypedSchema> TypedSubscriberBuilder<'a, 'b, T, DefaultHandler> {
    /// Specify a custom handler for this subscriber.
    ///
    /// The handler must implement [`IntoHandler<Sample>`].
    pub fn with<NewHandler>(
        self,
        handler: NewHandler,
    ) -> TypedSubscriberBuilder<'a, 'b, T, NewHandler>
    where
        NewHandler: zenoh::handlers::IntoHandler<Sample>,
    {
        TypedSubscriberBuilder {
            session: self.session,
            key_expr: self.key_expr,
            handler,
            schema_version: self.schema_version,
            _phantom: PhantomData,
        }
    }
}

impl<T: Deserialize + TypedSchema, Handler> TypedSubscriberBuilder<'_, '_, T, Handler>
where
    Handler: zenoh::handlers::IntoHandler<Sample> + Send,
    Handler::Handler: Send,
{
    fn build(self) -> ZResult<TypedSubscriber<T, Handler::Handler>> {
        let key_expr = self.key_expr?;
        let inner = self
            .session
            .declare_subscriber(key_expr)
            .with(self.handler)
            .wait()?;
        Ok(TypedSubscriber {
            inner,
            schema_version: self.schema_version,
            _phantom: PhantomData,
        })
    }
}

impl<T: Deserialize + TypedSchema, Handler> IntoFuture
    for TypedSubscriberBuilder<'_, '_, T, Handler>
where
    Handler: zenoh::handlers::IntoHandler<Sample> + Send,
    Handler::Handler: Send,
{
    type Output = ZResult<TypedSubscriber<T, Handler::Handler>>;
    type IntoFuture = Ready<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        std::future::ready(self.build())
    }
}

// -- Typed Query/Reply --
//
// Note: TypedQuery/TypedQueryable do not require `TypedSchema` because
// the queryable path does not set a typed encoding on the wire. Schema
// identification for queries is deferred to the typed RPC layer (M7).

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
            schema_version: None,
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
            handler: DefaultHandler::default(),
            schema_version: None,
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

#[cfg(test)]
mod tests {
    use super::*;
    use zenoh::key_expr::KeyExpr;
    use zenoh::sample::SampleBuilder;

    fn make_sample_with_encoding(encoding: &str) -> Sample {
        let payload = z_serialize(&42u32);
        let key: KeyExpr<'static> = KeyExpr::try_from("test/key").unwrap();
        SampleBuilder::put(key, payload)
            .encoding(Encoding::from(encoding))
            .into()
    }

    fn make_sample_default_encoding() -> Sample {
        let payload = z_serialize(&42u32);
        let key: KeyExpr<'static> = KeyExpr::try_from("test/key").unwrap();
        SampleBuilder::put(key, payload).into()
    }

    #[test]
    fn check_encoding_accepts_correct_typed_encoding() {
        let sample = make_sample_with_encoding("zenoh-ext/typed:test-payload");
        assert!(check_encoding(&sample, "test-payload").is_ok());
    }

    #[test]
    fn check_encoding_rejects_wrong_typed_encoding() {
        let sample = make_sample_with_encoding("zenoh-ext/typed:other-type");
        let result = check_encoding(&sample, "test-payload");
        assert!(result.is_err());
        match result.unwrap_err() {
            TypedReceiveError::EncodingMismatch { expected, received } => {
                assert!(
                    expected.contains("test-payload"),
                    "expected should contain schema name, got: {expected}"
                );
                assert!(
                    received.contains("other-type"),
                    "received should contain actual schema name, got: {received}"
                );
            }
            other => panic!("expected EncodingMismatch, got: {other:?}"),
        }
    }

    #[test]
    fn check_encoding_rejects_untyped_encoding() {
        let sample = make_sample_with_encoding("application/json");
        let result = check_encoding(&sample, "test-payload");
        assert!(result.is_err());
        match result.unwrap_err() {
            TypedReceiveError::EncodingMismatch { expected, received } => {
                assert!(
                    expected.contains("test-payload"),
                    "expected should reference schema name, got: {expected}"
                );
                assert!(
                    received.contains("application/json"),
                    "received should show actual encoding, got: {received}"
                );
            }
            other => panic!("expected EncodingMismatch, got: {other:?}"),
        }
    }

    #[test]
    fn check_encoding_rejects_default_encoding() {
        let sample = make_sample_default_encoding();
        let result = check_encoding(&sample, "test-payload");
        assert!(
            result.is_err(),
            "default encoding should not pass typed encoding check"
        );
        assert!(
            matches!(
                result.unwrap_err(),
                TypedReceiveError::EncodingMismatch { .. }
            ),
            "should be EncodingMismatch variant"
        );
    }

    #[test]
    fn encoding_mismatch_display_distinguishes_from_deserialization_error() {
        let encoding_err = TypedReceiveError::EncodingMismatch {
            expected: "zenoh-ext/typed:foo".to_string(),
            received: "application/json".to_string(),
        };
        let deser_err = TypedReceiveError::DeserializationFailed(ZDeserializeError);

        let encoding_msg = encoding_err.to_string();
        let deser_msg = deser_err.to_string();

        assert!(
            encoding_msg.contains("encoding mismatch"),
            "encoding error should say 'encoding mismatch', got: {encoding_msg}"
        );
        assert!(
            deser_msg.contains("deserialization"),
            "deser error should mention 'deserialization', got: {deser_msg}"
        );
        assert_ne!(
            encoding_msg, deser_msg,
            "error messages should be distinguishable"
        );
    }
}
