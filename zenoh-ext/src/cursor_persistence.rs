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

//! Pluggable cursor persistence strategies.
//!
//! [`CursorPersister`] defines how an [`EventSubscriber`](crate::EventSubscriber)
//! durably writes its cursor bookmark. The default [`PutPersister`] uses
//! fire-and-forget `session.put()`. Alternative implementations can provide
//! confirmed writes (e.g., acknowledged put) or delegate to external storage.

use async_trait::async_trait;
use zenoh::{bytes::ZBytes, session::WeakSession, Result as ZResult, Wait};

/// Strategy for persisting cursor bookmarks.
///
/// Implementations are stored behind `Arc<dyn CursorPersister>` and shared
/// across the flush task and drop handler.
#[zenoh_macros::unstable]
#[async_trait]
pub trait CursorPersister: Send + Sync {
    /// Persist cursor data asynchronously.
    async fn persist(
        &self,
        session: &WeakSession,
        persistence_key: &str,
        data: ZBytes,
    ) -> ZResult<()>;

    /// Persist cursor data synchronously (best-effort, used in `Drop`).
    fn persist_sync(
        &self,
        session: &WeakSession,
        persistence_key: &str,
        data: ZBytes,
    ) -> ZResult<()>;
}

/// Default persister using fire-and-forget `session.put()`.
///
/// The cursor data is published to the persistence key expression. A storage
/// backend configured for the `@cursors/**` key space will capture the put and
/// serve it on subsequent `session.get()` queries, enabling cursor recovery
/// across process restarts.
///
/// No write confirmation is provided. For confirmed writes, substitute an
/// acknowledged-put persister once the `ack-put` feature is available.
#[zenoh_macros::unstable]
#[derive(Debug, Clone, Copy, Default)]
pub struct PutPersister;

#[cfg(feature = "unstable")]
#[async_trait]
impl CursorPersister for PutPersister {
    async fn persist(
        &self,
        session: &WeakSession,
        persistence_key: &str,
        data: ZBytes,
    ) -> ZResult<()> {
        session.put(persistence_key, data).await
    }

    fn persist_sync(
        &self,
        session: &WeakSession,
        persistence_key: &str,
        data: ZBytes,
    ) -> ZResult<()> {
        session.put(persistence_key, data).wait()
    }
}
