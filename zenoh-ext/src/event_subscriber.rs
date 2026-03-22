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
    future::IntoFuture,
    sync::{Arc, Mutex},
    time::Duration,
};

use tracing::warn;
use zenoh::{
    bytes::ZBytes,
    handlers::DefaultHandler,
    internal::{zerror, zlock},
    key_expr::KeyExpr,
    pubsub::{Subscriber, SubscriberBuilder},
    query::{Parameters, Selector, TimeBound, TimeExpr, TimeRange, ZenohParameters},
    sample::Sample,
    session::WeakSession,
    time::Timestamp,
    Resolvable, Result as ZResult, Session, Wait,
};

use crate::cursor_persistence::{CursorPersister, PutPersister};
use crate::serialization::{Deserialize, Serialize, ZDeserializeError, ZDeserializer, ZSerializer};
use crate::{z_deserialize, z_serialize};

// ---------------------------------------------------------------------------
// FNV-1a hash (stable across Rust versions, unlike DefaultHasher)
// ---------------------------------------------------------------------------

fn fnv1a_hash(data: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

// ---------------------------------------------------------------------------
// CursorBookmark
// ---------------------------------------------------------------------------

/// A client-side cursor bookmark for event replay.
///
/// Tracks the consumer name, key expression, and last processed HLC timestamp.
#[zenoh_macros::unstable]
#[derive(Debug, Clone)]
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
    /// Uses FNV-1a for a stable hash that does not change across Rust versions.
    pub fn persistence_key(&self) -> String {
        let hash = fnv1a_hash(self.key_expr.as_bytes());
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
// zenoh-ext Serialize/Deserialize for CursorBookmark
// ---------------------------------------------------------------------------

#[zenoh_macros::unstable]
impl Serialize for CursorBookmark {
    fn serialize(&self, serializer: &mut ZSerializer) {
        self.consumer_name().serialize(serializer);
        self.key_expr_str().serialize(serializer);
        match self.cursor_position() {
            Some(ts) => {
                true.serialize(serializer);
                ts.to_string().serialize(serializer);
            }
            None => {
                false.serialize(serializer);
            }
        }
    }
}

#[zenoh_macros::unstable]
impl Deserialize for CursorBookmark {
    fn deserialize(deserializer: &mut ZDeserializer) -> Result<Self, ZDeserializeError> {
        let consumer_name: String = Deserialize::deserialize(deserializer)?;
        let key_expr: String = Deserialize::deserialize(deserializer)?;
        let has_ts: bool = Deserialize::deserialize(deserializer)?;

        let mut bookmark = CursorBookmark::new(&consumer_name, &key_expr);
        if has_ts {
            let ts_str: String = Deserialize::deserialize(deserializer)?;
            let ts: Timestamp = ts_str.parse().map_err(|_| ZDeserializeError)?;
            bookmark.advance(ts);
        }
        Ok(bookmark)
    }
}

// ---------------------------------------------------------------------------
// EventSubscriberBuilderExt
// ---------------------------------------------------------------------------

/// Extension trait that adds `.event()` to `SubscriberBuilder`.
#[zenoh_macros::unstable]
pub trait EventSubscriberBuilderExt<'a, 'b> {
    /// Create an event subscriber with client-side cursor tracking.
    ///
    /// Returns an [`EventSubscriberBuilder`] for further configuration.
    fn event(self) -> EventSubscriberBuilder<'a, 'b>;
}

#[zenoh_macros::unstable]
impl<'a, 'b> EventSubscriberBuilderExt<'a, 'b> for SubscriberBuilder<'a, 'b, DefaultHandler> {
    fn event(self) -> EventSubscriberBuilder<'a, 'b> {
        EventSubscriberBuilder {
            session: self.session,
            key_expr: self.key_expr,
            consumer_name: None,
            flush_interval: Duration::from_secs(5),
            buffer_size: 256,
            cursor_persister: None,
            cursor_load_timeout: Duration::from_secs(5),
            catch_up_timeout: Duration::from_secs(10),
            live_buffer_capacity: 10_000,
        }
    }
}

// ---------------------------------------------------------------------------
// EventSubscriberBuilder
// ---------------------------------------------------------------------------

/// Builder for an [`EventSubscriber`].
///
/// Created via the `.event()` method on a `SubscriberBuilder`.
///
/// **Note:** Resolving this builder (via `.await` or `.wait()`) may block for up
/// to `cursor_load_timeout + catch_up_timeout` (default 15s: 5s + 10s).
#[zenoh_macros::unstable]
pub struct EventSubscriberBuilder<'a, 'b> {
    pub(crate) session: &'a Session,
    pub(crate) key_expr: ZResult<KeyExpr<'b>>,
    pub(crate) consumer_name: Option<String>,
    pub(crate) flush_interval: Duration,
    pub(crate) buffer_size: usize,
    pub(crate) cursor_persister: Option<Arc<dyn CursorPersister>>,
    pub(crate) cursor_load_timeout: Duration,
    pub(crate) catch_up_timeout: Duration,
    pub(crate) live_buffer_capacity: usize,
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
            return Err(zerror!("consumer_name must not be empty").into());
        }
        if name.contains(['/', '*', '?', '$', '#']) {
            return Err(zerror!(
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

    /// Set the internal channel buffer size (default: 256).
    pub fn buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self
    }

    /// Set the timeout for loading a persisted cursor on startup (default: 5s).
    pub fn cursor_load_timeout(mut self, timeout: Duration) -> Self {
        self.cursor_load_timeout = timeout;
        self
    }

    /// Set the timeout for the catch-up query on startup (default: 10s).
    pub fn catch_up_timeout(mut self, timeout: Duration) -> Self {
        self.catch_up_timeout = timeout;
        self
    }

    /// Set the maximum number of live events buffered during catch-up (default: 10,000).
    ///
    /// During the catch-up phase, live events accumulate in a buffer. On
    /// high-throughput topics this buffer could grow unbounded. When the buffer
    /// reaches `capacity`, the oldest events are dropped and a warning is logged.
    ///
    /// Events dropped from the live buffer are still available via the catch-up
    /// query (they have earlier timestamps), so data loss only occurs if the
    /// catch-up window is shorter than the burst duration.
    pub fn live_buffer_capacity(mut self, capacity: usize) -> Self {
        self.live_buffer_capacity = capacity;
        self
    }

    /// Set a custom cursor persistence strategy.
    ///
    /// By default, [`PutPersister`] is used (fire-and-forget `session.put()`).
    /// Provide an alternative implementation to use confirmed writes or
    /// delegate to external storage.
    pub fn cursor_persister<P: CursorPersister + 'static>(mut self, persister: P) -> Self {
        self.cursor_persister = Some(Arc::new(persister));
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
    type IntoFuture = std::future::Ready<<Self as Resolvable>::To>;

    fn into_future(self) -> Self::IntoFuture {
        std::future::ready(self.wait())
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
    persister: Arc<dyn CursorPersister>,
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
    _flush_task: Option<tokio::task::JoinHandle<()>>,
    flush_interval: Duration,
}

#[zenoh_macros::unstable]
impl Drop for EventSubscriber {
    fn drop(&mut self) {
        // Abort the background flush task
        if let Some(handle) = self._flush_task.take() {
            handle.abort();
        }

        // Best-effort synchronous flush so cursor progress isn't lost on clean shutdown
        Self::do_flush_sync(&self.state);
    }
}

#[zenoh_macros::unstable]
impl EventSubscriber {
    fn new(conf: EventSubscriberBuilder<'_, '_>) -> ZResult<Self> {
        let key_expr = conf.key_expr?.into_owned();
        let consumer_name = conf.consumer_name.unwrap_or_else(|| "default".to_string());
        EventSubscriberBuilder::validate_consumer_name(&consumer_name)?;
        let flush_interval = conf.flush_interval;
        let session = conf.session;
        let persister: Arc<dyn CursorPersister> = conf
            .cursor_persister
            .unwrap_or_else(|| Arc::new(PutPersister));

        let mut bookmark = CursorBookmark::new(&consumer_name, key_expr.as_str());
        let persistence_key = bookmark.persistence_key();

        let cursor_load_timeout = conf.cursor_load_timeout;
        let catch_up_timeout = conf.catch_up_timeout;

        Self::load_persisted_cursor(
            session,
            &persistence_key,
            &mut bookmark,
            cursor_load_timeout,
        );

        let cursor_position = bookmark.cursor_position();

        let (sender, receiver) = flume::bounded::<Sample>(conf.buffer_size);

        let live_buffer: Arc<Mutex<Vec<Sample>>> = Arc::new(Mutex::new(Vec::new()));
        let live_buffer_clone = live_buffer.clone();
        let live_ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let live_ready_clone = live_ready.clone();
        let sender_clone = sender.clone();
        let live_cap = conf.live_buffer_capacity;

        let subscriber = session
            .declare_subscriber(key_expr.clone())
            .callback(move |sample: Sample| {
                if live_ready_clone.load(std::sync::atomic::Ordering::Acquire) {
                    let _ = sender_clone.send(sample);
                } else {
                    let mut buf = zlock!(live_buffer_clone);
                    if buf.len() >= live_cap {
                        buf.remove(0);
                        warn!(
                            "EventSubscriber live buffer full ({live_cap}), \
                             dropping oldest event during catch-up"
                        );
                    }
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
            catch_up_timeout,
        );

        Self::transition_to_live(&live_buffer, last_catchup_ts, &sender, &mut bookmark);
        live_ready.store(true, std::sync::atomic::Ordering::Release);

        let state = Arc::new(Mutex::new(EventSubscriberState {
            bookmark,
            persistence_key: persistence_key.clone(),
            last_flushed: None,
            session: session.downgrade(),
            persister,
        }));

        let flush_handle = Self::spawn_auto_flush(flush_interval, &state);

        Ok(EventSubscriber {
            receiver,
            state,
            _subscriber: subscriber,
            _flush_task: flush_handle,
            flush_interval,
        })
    }

    /// Load a previously persisted cursor from the session.
    fn load_persisted_cursor(
        session: &Session,
        persistence_key: &str,
        bookmark: &mut CursorBookmark,
        timeout: Duration,
    ) {
        match session.get(persistence_key).timeout(timeout).wait() {
            Ok(replies) => {
                for reply in replies {
                    if let Ok(sample) = reply.into_result() {
                        let zbytes = sample.payload().clone();
                        if let Ok(persisted) = z_deserialize::<CursorBookmark>(&zbytes) {
                            if let Some(ts) = persisted.cursor_position() {
                                bookmark.advance(ts);
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!(
                    "EventSubscriber cursor load timed out after {timeout:?} \
                     for key '{persistence_key}': {e}. Starting without persisted cursor."
                );
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
        timeout: Duration,
    ) -> Option<Timestamp> {
        let mut last_catchup_ts: Option<Timestamp> = None;

        if let Some(cursor_ts) = cursor_position {
            let mut params = Parameters::empty();
            params.set_time_range(TimeRange {
                start: TimeBound::Exclusive(TimeExpr::Fixed(cursor_ts.get_time().to_system_time())),
                end: TimeBound::Unbounded,
            });

            match session
                .get(Selector::from((key_expr, params)))
                .timeout(timeout)
                .wait()
            {
                Ok(replies) => {
                    let mut catchup_samples: Vec<Sample> = Vec::new();
                    for reply in replies {
                        if let Ok(sample) = reply.into_result() {
                            catchup_samples.push(sample);
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
                Err(e) => {
                    warn!(
                        "EventSubscriber catch-up query timed out after {timeout:?} \
                         for '{key_expr}': {e}. Some events may be missed."
                    );
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
                    if let Err(e) = Self::do_flush_async(&flush_state).await {
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
        Self::do_flush_async(&self.state).await
    }

    async fn do_flush_async(state: &Arc<Mutex<EventSubscriberState>>) -> ZResult<()> {
        // Extract data needed for persist under the lock, then drop before await
        let (zbytes, persistence_key, current, session, persister) = {
            let lock = zlock!(state);
            let current = lock.bookmark.cursor_position();

            // No-op if cursor hasn't advanced since last flush, or has never been set
            if current.is_none() || current == lock.last_flushed {
                return Ok(());
            }

            let zbytes: ZBytes = z_serialize(&lock.bookmark);
            let persistence_key = lock.persistence_key.clone();
            let session = lock.session.clone();
            let persister = lock.persister.clone();
            (zbytes, persistence_key, current, session, persister)
            // lock dropped here
        };

        // Session may be closed during shutdown — treat as success
        match persister.persist(&session, &persistence_key, zbytes).await {
            Ok(()) => {}
            Err(e) => {
                tracing::debug!("EventSubscriber flush skipped (session closed?): {e}");
                return Ok(());
            }
        }

        // Re-acquire to update last_flushed. A concurrent flush may have set a
        // newer value; overwriting with our `current` is safe because it only
        // causes one extra no-op persist on the next cycle.
        let mut lock = zlock!(state);
        lock.last_flushed = current;

        Ok(())
    }

    /// Synchronous best-effort flush for use in Drop. Never panics.
    fn do_flush_sync(state: &Arc<Mutex<EventSubscriberState>>) {
        let (zbytes, persistence_key, current, session, persister) = {
            let Ok(lock) = state.lock() else { return };
            let current = lock.bookmark.cursor_position();

            if current.is_none() || current == lock.last_flushed {
                return;
            }

            let zbytes: ZBytes = z_serialize(&lock.bookmark);
            let persistence_key = lock.persistence_key.clone();
            let session = lock.session.clone();
            let persister = lock.persister.clone();
            (zbytes, persistence_key, current, session, persister)
        };

        // Best-effort: ignore errors (session may already be closed)
        let _ = persister.persist_sync(&session, &persistence_key, zbytes);

        if let Ok(mut lock) = state.lock() {
            lock.last_flushed = current;
        }
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
            std::task::Poll::Ready(Err(err)) => std::task::Poll::Ready(Err(err.into())),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}
