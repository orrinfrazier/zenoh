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
    collections::hash_map::DefaultHasher,
    future::{IntoFuture, Ready},
    hash::{Hash, Hasher},
    sync::{Arc, Mutex},
    time::Duration,
};

use tracing::warn;
use zenoh_core::{zlock, Resolvable, Wait};
use zenoh_result::ZResult;

use crate::{
    api::{
        handlers::DefaultHandler,
        key_expr::KeyExpr,
        queryable::Queryable,
        sample::Sample,
        session::WeakSession,
    },
    pubsub::Subscriber,
    Session,
    time::Timestamp,
};

use super::subscriber::SubscriberBuilder;

// ---------------------------------------------------------------------------
// CursorBookmark
// ---------------------------------------------------------------------------

/// A client-side cursor bookmark for event replay.
///
/// Tracks the consumer name, key expression, and last processed HLC timestamp.
#[zenoh_macros::unstable]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CursorBookmark {
    consumer_name: String,
    key_expr: String,
    last_processed: Option<Timestamp>,
}

#[zenoh_macros::unstable]
impl CursorBookmark {
    /// Create a new `CursorBookmark` with no last processed timestamp.
    pub fn new(consumer_name: &str, key_expr: &str) -> Self {
        Self {
            consumer_name: consumer_name.to_string(),
            key_expr: key_expr.to_string(),
            last_processed: None,
        }
    }

    /// Returns the persistence key.
    ///
    /// Format: `@cursors/{consumer_name}/{hex_hash_of_key_expr}`
    ///
    /// **Note:** The hash uses `DefaultHasher` which is deterministic within a
    /// Rust version and platform but not guaranteed stable across Rust upgrades.
    /// This is acceptable for the unstable API; a stable hash (e.g. FNV-1a) may
    /// be substituted before stabilization.
    pub fn persistence_key(&self) -> String {
        let mut hasher = DefaultHasher::new();
        self.key_expr.hash(&mut hasher);
        let hash = hasher.finish();
        format!("@cursors/{}/{:x}", self.consumer_name, hash)
    }

    /// Advance the cursor to the given timestamp.
    ///
    /// Only updates if `ts` is strictly greater than the current position.
    pub fn advance(&mut self, ts: Timestamp) {
        match self.last_processed {
            Some(current) if ts <= current => {}
            _ => {
                self.last_processed = Some(ts);
            }
        }
    }

    /// Returns the consumer name.
    pub fn consumer_name(&self) -> &str {
        &self.consumer_name
    }

    /// Returns the key expression string.
    pub fn key_expr_str(&self) -> &str {
        &self.key_expr
    }

    /// Returns the current cursor position.
    pub fn cursor_position(&self) -> Option<Timestamp> {
        self.last_processed
    }
}

// ---------------------------------------------------------------------------
// EventSubscriberBuilder
// ---------------------------------------------------------------------------

/// Builder for an [`EventSubscriber`].
///
/// Created via the `.event()` method on a `SubscriberBuilder`.
#[zenoh_macros::unstable]
pub struct EventSubscriberBuilder<'a, 'b> {
    pub(crate) session: &'a Session,
    pub(crate) key_expr: ZResult<KeyExpr<'b>>,
    pub(crate) consumer_name: Option<String>,
    pub(crate) flush_interval: Duration,
}

#[zenoh_macros::unstable]
impl<'a, 'b> EventSubscriberBuilder<'a, 'b> {
    /// Set the consumer name.
    ///
    /// The name must not be empty or contain key expression special characters
    /// (`/`, `*`, `?`, `$`, `#`).
    pub fn consumer_name(mut self, name: &str) -> Self {
        self.consumer_name = Some(name.to_string());
        self
    }

    fn validate_consumer_name(name: &str) -> ZResult<()> {
        if name.is_empty() {
            return Err(zenoh_result::zerror!("consumer_name must not be empty").into());
        }
        if name.contains(['/', '*', '?', '$', '#']) {
            return Err(zenoh_result::zerror!(
                "consumer_name must not contain key expression special characters (/, *, ?, $, #): '{name}'"
            )
            .into());
        }
        Ok(())
    }

    /// Set the flush interval for automatic cursor persistence.
    pub fn flush_interval(mut self, interval: Duration) -> Self {
        self.flush_interval = interval;
        self
    }
}

#[cfg(feature = "unstable")]
impl Resolvable for EventSubscriberBuilder<'_, '_> {
    type To = ZResult<EventSubscriber>;
}

#[cfg(feature = "unstable")]
impl Wait for EventSubscriberBuilder<'_, '_> {
    fn wait(self) -> <Self as Resolvable>::To {
        EventSubscriber::new(self)
    }
}

#[cfg(feature = "unstable")]
impl IntoFuture for EventSubscriberBuilder<'_, '_> {
    type Output = <Self as Resolvable>::To;
    type IntoFuture = Ready<<Self as Resolvable>::To>;

    fn into_future(self) -> Self::IntoFuture {
        std::future::ready(self.wait())
    }
}

// ---------------------------------------------------------------------------
// .event() on SubscriberBuilder
// ---------------------------------------------------------------------------

#[cfg(feature = "unstable")]
impl<'a, 'b> SubscriberBuilder<'a, 'b, DefaultHandler> {
    /// Create an event subscriber with client-side cursor tracking.
    ///
    /// Returns an [`EventSubscriberBuilder`] for further configuration.
    pub fn event(self) -> EventSubscriberBuilder<'a, 'b> {
        EventSubscriberBuilder {
            session: self.session,
            key_expr: self.key_expr,
            consumer_name: None,
            flush_interval: Duration::from_secs(5),
        }
    }
}

// ---------------------------------------------------------------------------
// EventSubscriberState
// ---------------------------------------------------------------------------

#[cfg(feature = "unstable")]
struct EventSubscriberState {
    bookmark: CursorBookmark,
    persistence_key: String,
    last_flushed: Option<Timestamp>,
    session: WeakSession,
}

// ---------------------------------------------------------------------------
// EventSubscriber
// ---------------------------------------------------------------------------

/// An event subscriber with client-side cursor tracking.
///
/// Provides `recv_async()`, `try_recv()`, `cursor_position()`,
/// `flush_cursor()`, and `flush_interval()` methods.
#[zenoh_macros::unstable]
pub struct EventSubscriber {
    receiver: flume::Receiver<Sample>,
    state: Arc<Mutex<EventSubscriberState>>,
    _subscriber: Subscriber<()>,
    _queryable: Queryable<()>,
    _flush_task: Option<tokio::task::JoinHandle<()>>,
    flush_interval: Duration,
}

#[zenoh_macros::unstable]
impl Drop for EventSubscriber {
    fn drop(&mut self) {
        if let Some(handle) = self._flush_task.take() {
            handle.abort();
        }
    }
}

#[zenoh_macros::unstable]
impl EventSubscriber {
    fn new(conf: EventSubscriberBuilder<'_, '_>) -> ZResult<Self> {
        let key_expr = conf.key_expr?.into_owned();
        let consumer_name = conf
            .consumer_name
            .unwrap_or_else(|| "default".to_string());
        EventSubscriberBuilder::validate_consumer_name(&consumer_name)?;
        let flush_interval = conf.flush_interval;
        let session = conf.session;

        let mut bookmark = CursorBookmark::new(&consumer_name, key_expr.as_str());
        let persistence_key = bookmark.persistence_key();

        Self::load_persisted_cursor(session, &persistence_key, &mut bookmark);

        let cursor_position = bookmark.cursor_position();

        let (sender, receiver) = flume::bounded::<Sample>(256);

        let live_buffer: Arc<Mutex<Vec<Sample>>> = Arc::new(Mutex::new(Vec::new()));
        let live_buffer_clone = live_buffer.clone();
        let live_ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let live_ready_clone = live_ready.clone();
        let sender_clone = sender.clone();

        let subscriber = session
            .declare_subscriber(key_expr.clone())
            .callback(move |sample: Sample| {
                if live_ready_clone.load(std::sync::atomic::Ordering::Acquire) {
                    let _ = sender_clone.send(sample);
                } else {
                    let mut buf = zlock!(live_buffer_clone);
                    buf.push(sample);
                }
            })
            .wait()?;

        let last_catchup_ts = Self::do_catchup(
            session,
            &key_expr,
            cursor_position,
            &sender,
            &mut bookmark,
        );

        Self::transition_to_live(&live_buffer, last_catchup_ts, &sender, &mut bookmark);
        live_ready.store(true, std::sync::atomic::Ordering::Release);

        let state = Arc::new(Mutex::new(EventSubscriberState {
            bookmark,
            persistence_key: persistence_key.clone(),
            last_flushed: None,
            session: session.downgrade(),
        }));

        let queryable = Self::declare_cursor_queryable(session, &persistence_key, &state)?;

        let flush_handle = Self::spawn_auto_flush(flush_interval, &state);

        Ok(EventSubscriber {
            receiver,
            state,
            _subscriber: subscriber,
            _queryable: queryable,
            _flush_task: flush_handle,
            flush_interval,
        })
    }

    /// Load a previously persisted cursor from the session.
    fn load_persisted_cursor(
        session: &Session,
        persistence_key: &str,
        bookmark: &mut CursorBookmark,
    ) {
        if let Ok(replies) = session
            .get(persistence_key)
            .timeout(Duration::from_secs(5))
            .wait()
        {
            for reply in replies {
                if let Ok(sample) = reply.into_result() {
                    if let Ok(persisted) = serde_json::from_slice::<CursorBookmark>(
                        &sample.payload().to_bytes(),
                    ) {
                        if let Some(ts) = persisted.cursor_position() {
                            bookmark.advance(ts);
                        }
                    }
                }
            }
        }
    }

    /// Perform catch-up query for samples newer than the cursor position.
    ///
    /// Returns the timestamp of the last catch-up sample, if any.
    fn do_catchup(
        session: &Session,
        key_expr: &KeyExpr<'_>,
        cursor_position: Option<Timestamp>,
        sender: &flume::Sender<Sample>,
        bookmark: &mut CursorBookmark,
    ) -> Option<Timestamp> {
        let mut last_catchup_ts: Option<Timestamp> = None;

        if cursor_position.is_some() {
            if let Ok(replies) = session
                .get(crate::query::Selector::from(key_expr))
                .timeout(Duration::from_secs(10))
                .wait()
            {
                let mut catchup_samples: Vec<Sample> = Vec::new();
                for reply in replies {
                    if let Ok(sample) = reply.into_result() {
                        if let Some(ts) = sample.timestamp() {
                            if cursor_position.map(|c| *ts > c).unwrap_or(true) {
                                catchup_samples.push(sample);
                            }
                        } else {
                            catchup_samples.push(sample);
                        }
                    }
                }

                catchup_samples.sort_by(|a, b| a.timestamp().cmp(&b.timestamp()));

                for sample in &catchup_samples {
                    if let Some(ts) = sample.timestamp() {
                        bookmark.advance(*ts);
                        last_catchup_ts = Some(*ts);
                    }
                }
                for sample in catchup_samples {
                    let _ = sender.send(sample);
                }
            }
        }

        last_catchup_ts
    }

    /// Drain the live buffer, deduplicating against catch-up samples, and send
    /// remaining samples to the receiver channel.
    fn transition_to_live(
        live_buffer: &Arc<Mutex<Vec<Sample>>>,
        last_catchup_ts: Option<Timestamp>,
        sender: &flume::Sender<Sample>,
        bookmark: &mut CursorBookmark,
    ) {
        let mut buf = zlock!(live_buffer);
        buf.sort_by(|a, b| a.timestamp().cmp(&b.timestamp()));

        for sample in buf.drain(..) {
            let dominated = match (sample.timestamp(), last_catchup_ts) {
                (Some(live_ts), Some(catchup_ts)) => *live_ts <= catchup_ts,
                _ => false,
            };
            if !dominated {
                if let Some(ts) = sample.timestamp() {
                    bookmark.advance(*ts);
                }
                let _ = sender.send(sample);
            }
        }
    }

    /// Declare a queryable on the persistence key to serve cursor data on demand.
    fn declare_cursor_queryable(
        session: &Session,
        persistence_key: &str,
        state: &Arc<Mutex<EventSubscriberState>>,
    ) -> ZResult<Queryable<()>> {
        let queryable_state = state.clone();
        session
            .declare_queryable(persistence_key)
            .callback(move |query| {
                let lock = zlock!(queryable_state);
                if lock.last_flushed.is_some() {
                    if let Ok(json_bytes) = serde_json::to_vec(&lock.bookmark) {
                        let _ = query.reply(query.key_expr(), json_bytes).wait();
                    }
                }
            })
            .wait()
    }

    /// Spawn the background auto-flush task, if a tokio runtime is available.
    fn spawn_auto_flush(
        flush_interval: Duration,
        state: &Arc<Mutex<EventSubscriberState>>,
    ) -> Option<tokio::task::JoinHandle<()>> {
        let flush_state = state.clone();
        let rt = tokio::runtime::Handle::try_current();
        match rt {
            Ok(handle) => Some(handle.spawn(async move {
                loop {
                    tokio::time::sleep(flush_interval).await;
                    if let Err(e) = Self::do_flush(&flush_state) {
                        warn!("EventSubscriber auto-flush failed: {e}");
                    }
                }
            })),
            Err(_) => None,
        }
    }

    /// Asynchronously receive the next sample.
    pub fn recv_async(&self) -> EventRecvFut<'_> {
        EventRecvFut {
            inner: self.receiver.recv_async(),
            state: &self.state,
        }
    }

    /// Non-blocking receive.
    pub fn try_recv(&self) -> ZResult<Option<Sample>> {
        match self.receiver.try_recv() {
            Ok(sample) => {
                if let Some(ts) = sample.timestamp() {
                    let mut st = zlock!(self.state);
                    st.bookmark.advance(*ts);
                }
                Ok(Some(sample))
            }
            Err(flume::TryRecvError::Empty) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    /// Returns the current cursor position.
    pub fn cursor_position(&self) -> Option<Timestamp> {
        let st = zlock!(self.state);
        st.bookmark.cursor_position()
    }

    /// Returns the configured flush interval.
    pub fn flush_interval(&self) -> Duration {
        self.flush_interval
    }

    /// Manually flush the cursor to persistent storage.
    pub async fn flush_cursor(&self) -> ZResult<()> {
        Self::do_flush(&self.state)
    }

    fn do_flush(state: &Arc<Mutex<EventSubscriberState>>) -> ZResult<()> {
        // Extract data needed for put under the lock, then drop before I/O
        let (json_bytes, persistence_key, current, session) = {
            let lock = zlock!(state);
            let current = lock.bookmark.cursor_position();

            // No-op if cursor hasn't advanced since last flush, or has never been set
            if current.is_none() || current == lock.last_flushed {
                return Ok(());
            }

            let json_bytes = serde_json::to_vec(&lock.bookmark)
                .map_err(|e| zenoh_result::zerror!("failed to serialize cursor bookmark: {e}"))?;
            let persistence_key = lock.persistence_key.clone();
            let session = lock.session.clone();
            (json_bytes, persistence_key, current, session)
            // lock dropped here
        };

        session.put(&persistence_key, json_bytes).wait()?;

        // Re-acquire to update last_flushed
        let mut lock = zlock!(state);
        lock.last_flushed = current;

        Ok(())
    }
}

/// Future returned by [`EventSubscriber::recv_async`].
///
/// Advances the cursor when a sample is successfully received.
#[zenoh_macros::unstable]
pub struct EventRecvFut<'a> {
    inner: flume::r#async::RecvFut<'a, Sample>,
    state: &'a Arc<Mutex<EventSubscriberState>>,
}

#[cfg(feature = "unstable")]
impl std::future::Future for EventRecvFut<'_> {
    type Output = ZResult<Sample>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let state = self.state;
        match std::pin::Pin::new(&mut self.inner).poll(cx) {
            std::task::Poll::Ready(Ok(sample)) => {
                if let Some(ts) = sample.timestamp() {
                    let mut lock = zlock!(state);
                    lock.bookmark.advance(*ts);
                }
                std::task::Poll::Ready(Ok(sample))
            }
            std::task::Poll::Ready(Err(err)) => {
                std::task::Poll::Ready(Err(err.into()))
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}
