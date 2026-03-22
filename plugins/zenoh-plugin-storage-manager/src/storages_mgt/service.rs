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

use std::{
    collections::HashSet,
    str::{self},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use tokio::sync::{broadcast::Receiver, Mutex, RwLock, RwLockWriteGuard};
use zenoh::{
    bytes::{Encoding, ZBytes},
    internal::{bail, Timed, TimedEvent, Timer},
    key_expr::{
        keyexpr,
        keyexpr_tree::{
            IKeyExprTree, IKeyExprTreeMut, KeBoxTree, KeyedSetProvider, UnknownWildness,
        },
        OwnedKeyExpr,
    },
    sample::{Sample, SampleBuilder, SampleFields, SampleKind},
    session::Session,
    time::{Timestamp, NTP64},
    Result as ZResult,
};
use zenoh_backend_traits::{
    config::{GarbageCollectionConfig, StorageConfig},
    Capability, History, Storage, StorageInsertionResult, StoredData,
};

use super::LatestUpdates;
use crate::{
    replication::{Action, Event},
    storages_mgt::{CacheLatest, StorageMessage},
};

#[derive(Clone)]
pub(crate) struct Update {
    kind: SampleKind,
    data: StoredData,
}

impl Update {
    pub(crate) fn timestamp(&self) -> &Timestamp {
        &self.data.timestamp
    }

    pub(crate) fn kind(&self) -> SampleKind {
        self.kind
    }

    pub(crate) fn payload(&self) -> &ZBytes {
        &self.data.payload
    }

    pub(crate) fn encoding(&self) -> &Encoding {
        &self.data.encoding
    }
}

impl From<Update> for StoredData {
    fn from(update: Update) -> Self {
        update.data
    }
}

#[derive(Clone)]
pub struct StorageService {
    session: Arc<Session>,
    pub(crate) configuration: StorageConfig,
    name: String,
    pub(crate) storage: Arc<Mutex<Box<dyn zenoh_backend_traits::Storage>>>,
    capability: Capability,
    pub(crate) wildcard_deletes: Arc<RwLock<KeBoxTree<Update, UnknownWildness, KeyedSetProvider>>>,
    pub(crate) wildcard_puts: Arc<RwLock<KeBoxTree<Update, UnknownWildness, KeyedSetProvider>>>,
    cache_latest: CacheLatest,
}

impl StorageService {
    pub async fn new(
        session: Arc<Session>,
        config: StorageConfig,
        name: &str,
        storage: Arc<Mutex<Box<dyn zenoh_backend_traits::Storage>>>,
        capability: Capability,
        cache_latest: CacheLatest,
    ) -> Self {
        StorageService {
            session,
            configuration: config,
            name: name.to_string(),
            storage,
            capability,
            wildcard_deletes: Arc::new(RwLock::new(KeBoxTree::default())),
            wildcard_puts: Arc::new(RwLock::new(KeBoxTree::default())),
            cache_latest,
        }
    }

    pub(crate) async fn start_storage_queryable_subscriber(
        self: Arc<Self>,
        mut rx: Receiver<StorageMessage>,
    ) {
        // start periodic GC event
        let t = Timer::default();

        let gc_config = self.configuration.garbage_collection_config.clone();

        let latest_updates = if self.cache_latest.replication_log.is_none() {
            Some(self.cache_latest.latest_updates.clone())
        } else {
            None
        };

        let gc = TimedEvent::periodic(
            gc_config.period,
            GarbageCollectionEvent {
                config: gc_config,
                storage: self.storage.clone(),
                wildcard_deletes: self.wildcard_deletes.clone(),
                wildcard_puts: self.wildcard_puts.clone(),
                latest_updates,
            },
        );
        t.add_async(gc).await;

        let storage_key_expr = &self.configuration.key_expr;

        // subscribe on key_expr
        let storage_sub = match self.session.declare_subscriber(storage_key_expr).await {
            Ok(storage_sub) => storage_sub,
            Err(e) => {
                tracing::error!("Error starting storage '{}': {}", self.name, e);
                return;
            }
        };

        // answer to queries on key_expr
        let storage_queryable = match self
            .session
            .declare_queryable(storage_key_expr)
            .complete(self.configuration.complete)
            .await
        {
            Ok(storage_queryable) => storage_queryable,
            Err(e) => {
                tracing::error!("Error starting storage '{}': {}", self.name, e);
                return;
            }
        };

        tracing::debug!(
            "Starting storage '{}' on keyexpr '{}'",
            self.name,
            storage_key_expr
        );

        tokio::task::spawn(async move {
            // Keep the Timer alive for the duration of the event loop so GC
            // periodic events continue to fire.
            let _gc_timer = t;
            loop {
                tokio::select!(
                    // on sample for key_expr
                    sample = storage_sub.recv_async() => {
                        let sample = match sample {
                            Ok(sample) => sample,
                            Err(e) => {
                                tracing::error!("Error in sample: {}", e);
                                continue;
                            }
                        };
                        let timestamp = sample.timestamp().cloned().unwrap_or(self.session.new_timestamp());
                        let sample = SampleBuilder::from(sample).timestamp(timestamp).into();
                        if let Err(e) = self.process_sample(sample).await {
                            tracing::error!("{e:?}");
                        }
                    },
                    // on query on key_expr
                    query = storage_queryable.recv_async() => {
                        self.reply_query(query).await;
                    },
                    // on storage handle drop
                    Ok(message) = rx.recv() => {
                        match message {
                            StorageMessage::Stop => {
                                tracing::trace!("Dropping storage '{}'", self.name);
                                return
                            },
                            StorageMessage::GetStatus(tx) => {
                                let storage = self.storage.lock().await;
                                std::mem::drop(tx.send(storage.get_admin_status().into()).await);
                            }
                        };
                    },
                );
            }
        });
    }

    // The storage should only simply save the key, sample pair while put and retrieve the same
    // during get the trimming during PUT and GET should be handled by the plugin
    pub(crate) async fn process_sample(&self, sample: Sample) -> ZResult<()> {
        tracing::trace!("[STORAGE] Processing sample: {:?}", sample.key_expr());
        let SampleFields {
            key_expr,
            timestamp,
            payload,
            encoding,
            kind,
            ..
        } = sample.clone().into();

        // A Sample, in theory, will not arrive to a Storage without a Timestamp. This check (which,
        // again, should never enter the `None` branch) ensures that the Storage Manager
        // does not panic even if it ever happens.
        let Some(timestamp) = timestamp else {
            bail!("Discarding Sample without a Timestamp: {:?}", sample);
        };

        let mut action: Action = kind.into();
        // if wildcard, update wildcard_updates
        if key_expr.is_wild() {
            self.register_wildcard_update(
                key_expr.clone().into(),
                kind,
                timestamp,
                payload,
                encoding,
            )
            .await;

            action = match kind {
                SampleKind::Put => Action::WildcardPut(key_expr.clone().into()),
                SampleKind::Delete => Action::WildcardDelete(key_expr.clone().into()),
            };

            let event = Event::new(Some(key_expr.clone().into()), timestamp, &action);

            self.cache_latest
                .latest_updates
                .write()
                .await
                .insert(event.log_key(), event);
        }

        let matching_keys = if key_expr.is_wild() {
            self.get_matching_keys(&key_expr).await
        } else {
            vec![key_expr.clone().into()]
        };
        tracing::trace!(
            "The list of keys matching `{}` is : {:?}",
            &key_expr,
            matching_keys
        );

        let prefix = self.configuration.strip_prefix.as_ref();

        for k in matching_keys {
            // there might be the case that the actual update was outdated due to a wild card
            // update, but not stored yet in the storage. get the relevant wild
            // card entry and use that value and timestamp to update the storage
            let sample_to_store: Sample = if let Some((_, update)) = self
                .overriding_wild_update(&k, &timestamp, &None, &kind.into())
                .await
            {
                match update.kind {
                    SampleKind::Put => SampleBuilder::put(k.clone(), update.data.payload.clone())
                        .encoding(update.data.encoding.clone())
                        .timestamp(update.data.timestamp)
                        .into(),
                    SampleKind::Delete => SampleBuilder::delete(k.clone())
                        .timestamp(update.data.timestamp)
                        .into(),
                }
            } else {
                SampleBuilder::from(sample.clone())
                    .keyexpr(k.clone())
                    .into()
            };

            // A Sample that is to be stored **must** have a Timestamp. In theory, the Sample
            // generated should have a Timestamp and, in theory, this check is
            // unneeded.
            let sample_to_store_timestamp = match sample_to_store.timestamp() {
                Some(timestamp) => *timestamp,
                None => {
                    tracing::error!(
                        "Discarding `Sample` generated through `SampleBuilder` that has no \
                         Timestamp: {:?}",
                        sample_to_store
                    );
                    continue;
                }
            };

            let stripped_key = match crate::strip_prefix(prefix, sample_to_store.key_expr()) {
                Ok(stripped) => stripped,
                Err(e) => {
                    bail!("{e:?}");
                }
            };

            // If the Storage was declared as only keeping the Latest value, we ensure that, for
            // each received Sample, it is indeed the Latest value that is processed.
            let new_event = Event::new(stripped_key.clone(), sample_to_store_timestamp, &action);
            let mut cache_guard = None;
            if self.capability.history == History::Latest {
                match self.guard_cache_if_latest(&new_event).await {
                    Some(guard) => {
                        cache_guard = Some(guard);
                    }
                    None => {
                        tracing::trace!("Skipping outdated Sample < {} >", k);
                        continue;
                    }
                }
            }

            let mut storage = self.storage.lock().await;
            let storage_result = match sample.kind() {
                SampleKind::Put => {
                    storage
                        .put(
                            stripped_key.clone(),
                            sample_to_store.payload().clone(),
                            sample_to_store.encoding().clone(),
                            sample_to_store_timestamp,
                        )
                        .await
                }
                SampleKind::Delete => {
                    storage
                        .delete(stripped_key.clone(), sample_to_store_timestamp)
                        .await
                }
            };

            drop(storage);

            match storage_result {
                Ok(StorageInsertionResult::Outdated) => {
                    tracing::trace!("Ignoring `Outdated` sample < {} >", k);
                }
                Ok(_) => {
                    if let Some(mut cache_guard) = cache_guard {
                        cache_guard.insert(new_event.log_key(), new_event);
                    }
                }
                Err(e) => {
                    // TODO In case of a wildcard update, multiple keys can be updated. What should
                    //      be the behaviour if one or more of these updates fail?
                    tracing::error!("`{}` on < {} > failed with: {e:?}", sample.kind(), k);
                }
            }
        }

        Ok(())
    }

    /// Registers a Wildcard Update, storing it in a dedicated in-memory structure and on disk if
    /// the Storage persistence capability is set to `Durable`.
    ///
    /// The `key_expr` and `timestamp` cannot be extracted from the received Sample when aligning
    /// and hence must be manually passed.
    ///
    /// # ⚠️ Cache with Replication
    ///
    /// It is the *responsibility of the caller* to insert a Wildcard Update event in the Cache. If
    /// the Replication is enabled, depending on where this method is called, the event should
    /// either be inserted in the Cache (to be later added in the Replication Log) or in the
    /// Replication Log.
    pub(crate) async fn register_wildcard_update(
        &self,
        key_expr: OwnedKeyExpr,
        kind: SampleKind,
        timestamp: Timestamp,
        payload: ZBytes,
        encoding: Encoding,
    ) {
        let update = Update {
            kind,
            data: StoredData {
                payload,
                encoding,
                timestamp,
            },
        };

        match kind {
            SampleKind::Put => {
                self.wildcard_puts.write().await.insert(&key_expr, update);
            }
            SampleKind::Delete => {
                self.wildcard_deletes
                    .write()
                    .await
                    .insert(&key_expr, update);
            }
        }
    }

    /// Returns an [Update] if the provided key expression is overridden by a Wildcard Update.
    pub(crate) async fn overriding_wild_update(
        &self,
        key_expr: &OwnedKeyExpr,
        timestamp: &Timestamp,
        timestamp_last_non_wildcard_update: &Option<Timestamp>,
        action: &Action,
    ) -> Option<(OwnedKeyExpr, Update)> {
        // First, check for a delete *if and only if the action is not a Wildcard Put*: if there are
        // Wildcard Delete that match this key expression, we want to keep the lowest delete
        // (i.e. the first that applies) as a Delete does not override another Delete -- except if
        // it's a Wildcard Delete that overrides another Wildcard Delete but that's another story.
        if matches!(
            action,
            Action::Put | Action::Delete | Action::WildcardDelete(_)
        ) {
            let mut wildcard_ke = None;
            let wildcard_deletes_guard = self.wildcard_deletes.read().await;
            let lowest_event_ts = timestamp_last_non_wildcard_update.unwrap_or(*timestamp);
            let mut lowest_wildcard_delete_ts = None;

            for wildcard_delete_ke in wildcard_deletes_guard.intersecting_keys(key_expr) {
                if let Some(wildcard_delete_update) =
                    wildcard_deletes_guard.weight_at(&wildcard_delete_ke)
                {
                    // Wildcard Delete with a greater timestamp than the lowest timestamp of the
                    // Event are the only one that should apply.
                    if wildcard_delete_update.data.timestamp >= lowest_event_ts {
                        match lowest_wildcard_delete_ts {
                            None => {
                                lowest_wildcard_delete_ts =
                                    Some(*wildcard_delete_update.timestamp());
                                wildcard_ke = Some(wildcard_delete_ke);
                            }
                            Some(current_lowest_ts) => {
                                if current_lowest_ts > wildcard_delete_update.data.timestamp {
                                    lowest_wildcard_delete_ts =
                                        Some(*wildcard_delete_update.timestamp());
                                    wildcard_ke = Some(wildcard_delete_ke);
                                }
                            }
                        }
                    }
                }
            }

            if let Some(wildcard_delete_ke) = wildcard_ke {
                if let Some(wildcard_delete_update) =
                    wildcard_deletes_guard.weight_at(&wildcard_delete_ke)
                {
                    return Some((wildcard_delete_ke, wildcard_delete_update.clone()));
                }
            }
        }

        // A Wildcard Put can only override a Put or another Wildcard Put. If several match, this
        // time we want to keep the Update with the latest timestamp.
        if matches!(action, Action::Put | Action::WildcardPut(_)) {
            let mut wildcard_ke = None;

            let wildcards = self.wildcard_puts.read().await;
            let mut latest_wildcard_ts = *timestamp;

            for node in wildcards.intersecting_keys(key_expr) {
                if let Some(wildcard_update) = wildcards.weight_at(&node) {
                    if wildcard_update.data.timestamp >= latest_wildcard_ts {
                        latest_wildcard_ts = wildcard_update.data.timestamp;
                        wildcard_ke = Some(node);
                    }
                }
            }

            if let Some(wildcard_ke) = wildcard_ke {
                if let Some(wildcard_update) = wildcards.weight_at(&wildcard_ke) {
                    return Some((wildcard_ke, wildcard_update.clone()));
                }
            }
        }

        None
    }

    /// Returns a guard over the cache if the provided [Timestamp] is more recent than what is kept
    /// in the Storage for the `stripped_key`. Otherwise returns `None`.
    ///
    /// This method will first look up any cached value and if none is found, it will request the
    /// Storage.
    ///
    /// # ⚠️ Race-condition
    ///
    /// Returning a guard over the cache is not an "innocent" choice: in order to avoid
    /// race-condition, the guard over the cache must be kept until the Storage has processed the
    /// Sample and the Cache has been updated accordingly.
    ///
    /// If the lock is released before both operations are performed, the Cache and Storage could
    /// end up in an inconsistent state (think two updates being processed at the same time).
    async fn guard_cache_if_latest(
        &self,
        new_event: &Event,
    ) -> Option<RwLockWriteGuard<'_, LatestUpdates>> {
        let cache_guard = self.cache_latest.latest_updates.write().await;
        if let Some(event) = cache_guard.get(&new_event.log_key()) {
            if new_event.timestamp > event.timestamp {
                return Some(cache_guard);
            }
        }

        if let Some(replication_log) = &self.cache_latest.replication_log {
            if replication_log
                .read()
                .await
                .lookup_newer(new_event)
                .is_some()
            {
                return None;
            }
        } else {
            let mut storage = self.storage.lock().await;
            // FIXME: An actual error from the underlying Storage cannot be distinguished from a
            //        missing entry.
            if let Ok(stored_data) = storage.get(new_event.stripped_key.clone(), "").await {
                for data in stored_data {
                    if data.timestamp > new_event.timestamp {
                        return None;
                    }
                }
            }
        }

        Some(cache_guard)
    }

    async fn reply_query(&self, query: ZResult<zenoh::query::Query>) {
        let q = match query {
            Ok(q) => q,
            Err(e) => {
                tracing::error!("Error in query: {}", e);
                return;
            }
        };
        tracing::trace!("[STORAGE] Processing query on key_expr: {}", q.key_expr());

        let prefix = self.configuration.strip_prefix.as_ref();

        if q.key_expr().is_wild() {
            // resolve key expr into individual keys
            let matching_keys = self.get_matching_keys(q.key_expr()).await;
            let mut storage = self.storage.lock().await;
            for key in matching_keys {
                let stripped_key = match crate::strip_prefix(prefix, &key.clone().into()) {
                    Ok(k) => k,
                    Err(e) => {
                        tracing::error!("{}", e);
                        // @TODO: return error when it is supported
                        return;
                    }
                };
                match storage.get(stripped_key, q.parameters().as_str()).await {
                    Ok(stored_data) => {
                        for entry in stored_data {
                            if let Err(e) = q
                                .reply(key.clone(), entry.payload.clone())
                                .encoding(entry.encoding.clone())
                                .timestamp(entry.timestamp)
                                .await
                            {
                                tracing::warn!(
                                    "Storage '{}' raised an error replying a query: {}",
                                    self.name,
                                    e
                                )
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Storage'{}' raised an error on query: {}", self.name, e)
                    }
                };
            }
            drop(storage);
        } else {
            let stripped_key = match crate::strip_prefix(prefix, q.key_expr()) {
                Ok(k) => k,
                Err(e) => {
                    tracing::error!("{}", e);
                    // @TODO: return error when it is supported
                    return;
                }
            };
            let mut storage = self.storage.lock().await;
            match storage.get(stripped_key, q.parameters().as_str()).await {
                Ok(stored_data) => {
                    for entry in stored_data {
                        if let Err(e) = q
                            .reply(q.key_expr().clone(), entry.payload.clone())
                            .encoding(entry.encoding.clone())
                            .timestamp(entry.timestamp)
                            .await
                        {
                            tracing::warn!(
                                "Storage '{}' raised an error replying a query: {}",
                                self.name,
                                e
                            )
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Storage '{}' raised an error on query: {e}", self.name);
                }
            };
        }
    }

    async fn get_matching_keys(&self, key_expr: &keyexpr) -> Vec<OwnedKeyExpr> {
        let mut result = Vec::new();
        // @TODO: if cache exists, use that to get the list
        let storage = self.storage.lock().await;

        let prefix = self.configuration.strip_prefix.as_ref();

        match storage.get_all_entries().await {
            Ok(entries) => {
                for (k, _ts) in entries {
                    // @TODO: optimize adding back the prefix (possible inspiration from https://github.com/eclipse-zenoh/zenoh/blob/0.5.0-beta.9/backends/traits/src/utils.rs#L79)
                    let Ok(full_key) = crate::prefix(prefix, k.as_ref()) else {
                        tracing::error!(
                            "Internal error: empty key with no `strip_prefix` configured"
                        );
                        continue;
                    };

                    if key_expr.intersects(&full_key.clone()) {
                        result.push(full_key);
                    }
                }
            }
            Err(e) => tracing::warn!(
                "Storage '{}' raised an error while retrieving keys: {}",
                self.name,
                e
            ),
        }
        result
    }
}

// Periodic event cleaning-up data info for old metadata
struct GarbageCollectionEvent {
    config: GarbageCollectionConfig,
    storage: Arc<Mutex<Box<dyn Storage>>>,
    wildcard_deletes: Arc<RwLock<KeBoxTree<Update, UnknownWildness, KeyedSetProvider>>>,
    wildcard_puts: Arc<RwLock<KeBoxTree<Update, UnknownWildness, KeyedSetProvider>>>,
    latest_updates: Option<Arc<RwLock<LatestUpdates>>>,
}

#[async_trait]
impl Timed for GarbageCollectionEvent {
    async fn run(&mut self) {
        tracing::trace!("Start garbage collection");
        let time_limit = NTP64::from(SystemTime::now().duration_since(UNIX_EPOCH).expect("system clock before UNIX epoch"))
            - NTP64::from(self.config.lifespan);

        // Get lock on fields
        let mut wildcard_deletes_guard = self.wildcard_deletes.write().await;
        let mut wildcard_updates_guard = self.wildcard_puts.write().await;

        let mut to_be_removed = HashSet::new();
        for (k, update) in wildcard_deletes_guard.key_value_pairs() {
            let ts = update.data.timestamp;
            if ts.get_time() < &time_limit {
                // mark key to be removed
                to_be_removed.insert(k);
            }
        }
        for k in to_be_removed {
            wildcard_deletes_guard.remove(&k);
        }

        let mut to_be_removed = HashSet::new();
        for (k, update) in wildcard_updates_guard.key_value_pairs() {
            let ts = update.data.timestamp;
            if ts.get_time() < &time_limit {
                // mark key to be removed
                to_be_removed.insert(k);
            }
        }
        for k in to_be_removed {
            wildcard_updates_guard.remove(&k);
        }

        if let Some(latest_updates) = &self.latest_updates {
            if let Some(prefix_lifespans) = &self.config.prefix_lifespans {
                // Pre-compute time limits per prefix (Finding 3: avoid per-key SystemTime::now)
                let now = NTP64::from(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap(),
                );
                let prefix_limits: Vec<_> = prefix_lifespans
                    .iter()
                    .map(|pl| (pl, now - NTP64::from(pl.lifespan)))
                    .collect();

                let mut latest = latest_updates.write().await;
                let mut keys_to_remove = Vec::new();
                // Collect storage deletes to batch after releasing cache lock (Finding 2)
                let mut storage_deletes: Vec<(OwnedKeyExpr, Timestamp)> = Vec::new();

                for (log_key, event) in latest.iter() {
                    let mut matched = false;
                    if let Some(stripped_key) = event.key_expr() {
                        for (pl, prefix_time_limit) in &prefix_limits {
                            if pl.key_expr.intersects(stripped_key) {
                                matched = true;
                                if event.timestamp().get_time() < prefix_time_limit {
                                    if pl.delete_data {
                                        storage_deletes.push((
                                            stripped_key.clone(),
                                            *event.timestamp(),
                                        ));
                                    }
                                    keys_to_remove.push(log_key.clone());
                                }
                                break; // first matching prefix wins
                            }
                        }
                    }
                    // Finding 1: non-matching keys (and None-keyed) still get default
                    // tombstone cleanup
                    if !matched && event.timestamp().get_time() < &time_limit {
                        keys_to_remove.push(log_key.clone());
                    }
                }

                for key in &keys_to_remove {
                    latest.remove(key);
                }
                drop(latest);

                // Batch-delete from storage without holding cache write lock
                if !storage_deletes.is_empty() {
                    let mut storage = self.storage.lock().await;
                    for (key, ts) in storage_deletes {
                        if let Err(e) = storage.delete(Some(key.clone()), ts).await {
                            tracing::warn!(
                                "Prefix GC: failed to delete key '{}': {e}",
                                key
                            );
                        }
                    }
                }
            } else {
                // Default behavior: clean up old metadata based on global lifespan
                latest_updates
                    .write()
                    .await
                    .retain(|_, event| event.timestamp().get_time() >= &time_limit);
            }
        }

        tracing::trace!("End garbage collection of obsolete data-infos");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use uhlc::{HLC, NTP64};
    use zenoh::key_expr::OwnedKeyExpr;
    use zenoh_backend_traits::config::PrefixLifespan;
    use zenoh_plugin_trait::Plugin;

    use crate::memory_backend::MemoryBackend;
    use crate::replication::{Action, Event};
    use crate::storages_mgt::LatestUpdates;

    fn old_timestamp(age: Duration) -> zenoh::time::Timestamp {
        let hlc = HLC::default();
        let past = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            - age;
        zenoh::time::Timestamp::new(NTP64::from(past), *hlc.get_id())
    }

    async fn make_memory_storage() -> Arc<Mutex<Box<dyn zenoh_backend_traits::Storage>>> {
        let volume_config = zenoh_backend_traits::config::VolumeConfig {
            name: "memory".into(),
            backend: None,
            paths: None,
            required: false,
            rest: Default::default(),
        };
        let volume = MemoryBackend::start("memory", &volume_config).unwrap();
        let storage_config = zenoh_backend_traits::config::StorageConfig {
            name: "test-storage".into(),
            key_expr: "test/**".try_into().unwrap(),
            complete: false,
            strip_prefix: None,
            volume_id: "memory".into(),
            volume_cfg: Default::default(),
            garbage_collection_config: GarbageCollectionConfig::default(),
            replication: None,
        };
        let storage = volume.create_storage(storage_config).await.unwrap();
        Arc::new(Mutex::new(storage))
    }

    #[tokio::test]
    async fn test_gc_deletes_matching_prefix_keys() {
        let storage = make_memory_storage().await;

        // Put an event key with a 3-day-old timestamp
        let event_key: OwnedKeyExpr = "devices/42/events/abc123".try_into().unwrap();
        let old_ts = old_timestamp(Duration::from_secs(3 * 86400));
        {
            let mut s = storage.lock().await;
            s.put(
                Some(event_key.clone()),
                zenoh::bytes::ZBytes::default(),
                zenoh::bytes::Encoding::default(),
                old_ts,
            )
            .await
            .unwrap();
        }

        // Create latest_updates cache with the event
        let latest_updates: Arc<RwLock<LatestUpdates>> =
            Arc::new(RwLock::new(HashMap::new()));
        let event = Event::new(Some(event_key.clone()), old_ts, &Action::Put);
        latest_updates
            .write()
            .await
            .insert(event.log_key(), event);

        // GC config with 48h prefix lifespan for events (key is 3 days old, should be GC'd)
        let gc_config = GarbageCollectionConfig {
            period: Duration::from_secs(30),
            lifespan: Duration::from_secs(86400),
            prefix_lifespans: Some(vec![PrefixLifespan {
                key_expr: "**/events/**".try_into().unwrap(),
                lifespan: Duration::from_secs(48 * 3600),
                delete_data: true,
            }]),
        };

        let mut gc_event = GarbageCollectionEvent {
            config: gc_config,
            storage: storage.clone(),
            wildcard_deletes: Arc::new(RwLock::new(KeBoxTree::default())),
            wildcard_puts: Arc::new(RwLock::new(KeBoxTree::default())),
            latest_updates: Some(latest_updates.clone()),
        };

        gc_event.run().await;

        // Verify: event key deleted from storage
        let mut s = storage.lock().await;
        let result = s.get(Some(event_key), "").await;
        assert!(
            result.is_err(),
            "Event key should have been deleted from storage"
        );

        // Verify: cache should not contain the event
        assert!(
            latest_updates.read().await.is_empty(),
            "Event should have been removed from cache"
        );
    }

    #[tokio::test]
    async fn test_gc_preserves_non_matching_keys() {
        let storage = make_memory_storage().await;

        // Put an entity state key with a 3-day-old timestamp
        let state_key: OwnedKeyExpr = "devices/42/status".try_into().unwrap();
        let old_ts = old_timestamp(Duration::from_secs(3 * 86400));
        {
            let mut s = storage.lock().await;
            s.put(
                Some(state_key.clone()),
                zenoh::bytes::ZBytes::from(b"online".to_vec()),
                zenoh::bytes::Encoding::default(),
                old_ts,
            )
            .await
            .unwrap();
        }

        // Create latest_updates cache with the entity state event
        let latest_updates: Arc<RwLock<LatestUpdates>> =
            Arc::new(RwLock::new(HashMap::new()));
        let event = Event::new(Some(state_key.clone()), old_ts, &Action::Put);
        latest_updates
            .write()
            .await
            .insert(event.log_key(), event);

        // GC config with prefix lifespan ONLY for events — status keys should NOT match
        let gc_config = GarbageCollectionConfig {
            period: Duration::from_secs(30),
            lifespan: Duration::from_secs(86400),
            prefix_lifespans: Some(vec![PrefixLifespan {
                key_expr: "**/events/**".try_into().unwrap(),
                lifespan: Duration::from_secs(48 * 3600),
                delete_data: true,
            }]),
        };

        let mut gc_event = GarbageCollectionEvent {
            config: gc_config,
            storage: storage.clone(),
            wildcard_deletes: Arc::new(RwLock::new(KeBoxTree::default())),
            wildcard_puts: Arc::new(RwLock::new(KeBoxTree::default())),
            latest_updates: Some(latest_updates.clone()),
        };

        gc_event.run().await;

        // Verify: state key still exists in storage (NOT deleted by prefix GC)
        let mut s = storage.lock().await;
        let result = s.get(Some(state_key), "").await;
        assert!(
            result.is_ok(),
            "Entity state key should NOT have been deleted from storage"
        );

        // Note: the cache entry IS cleaned by default tombstone GC (the entry is older
        // than the default lifespan). This is correct — only STORAGE data is preserved
        // for non-matching keys. Metadata cleanup still applies.
    }

    #[tokio::test]
    async fn test_gc_delete_data_false_removes_cache_preserves_storage() {
        let storage = make_memory_storage().await;

        // Put an event key with a 3-day-old timestamp
        let event_key: OwnedKeyExpr = "devices/42/events/abc123".try_into().unwrap();
        let old_ts = old_timestamp(Duration::from_secs(3 * 86400));
        {
            let mut s = storage.lock().await;
            s.put(
                Some(event_key.clone()),
                zenoh::bytes::ZBytes::from(b"payload".to_vec()),
                zenoh::bytes::Encoding::default(),
                old_ts,
            )
            .await
            .unwrap();
        }

        // Create latest_updates cache with the event
        let latest_updates: Arc<RwLock<LatestUpdates>> =
            Arc::new(RwLock::new(HashMap::new()));
        let event = Event::new(Some(event_key.clone()), old_ts, &Action::Put);
        latest_updates
            .write()
            .await
            .insert(event.log_key(), event);

        // GC config with delete_data: false — should remove cache entry but NOT storage data
        let gc_config = GarbageCollectionConfig {
            period: Duration::from_secs(30),
            lifespan: Duration::from_secs(86400),
            prefix_lifespans: Some(vec![PrefixLifespan {
                key_expr: "**/events/**".try_into().unwrap(),
                lifespan: Duration::from_secs(48 * 3600),
                delete_data: false,
            }]),
        };

        let mut gc_event = GarbageCollectionEvent {
            config: gc_config,
            storage: storage.clone(),
            wildcard_deletes: Arc::new(RwLock::new(KeBoxTree::default())),
            wildcard_puts: Arc::new(RwLock::new(KeBoxTree::default())),
            latest_updates: Some(latest_updates.clone()),
        };

        gc_event.run().await;

        // Verify: cache entry removed
        assert!(
            latest_updates.read().await.is_empty(),
            "Cache entry should have been removed"
        );

        // Verify: storage data preserved (delete_data is false)
        let mut s = storage.lock().await;
        let result = s.get(Some(event_key), "").await;
        assert!(
            result.is_ok(),
            "Storage data should be preserved when delete_data is false"
        );
    }

    #[tokio::test]
    async fn test_gc_first_matching_prefix_wins() {
        let storage = make_memory_storage().await;

        // Put an event key with a 2-day-old timestamp
        let event_key: OwnedKeyExpr = "devices/42/events/abc123".try_into().unwrap();
        let old_ts = old_timestamp(Duration::from_secs(2 * 86400));
        {
            let mut s = storage.lock().await;
            s.put(
                Some(event_key.clone()),
                zenoh::bytes::ZBytes::from(b"payload".to_vec()),
                zenoh::bytes::Encoding::default(),
                old_ts,
            )
            .await
            .unwrap();
        }

        let latest_updates: Arc<RwLock<LatestUpdates>> =
            Arc::new(RwLock::new(HashMap::new()));
        let event = Event::new(Some(event_key.clone()), old_ts, &Action::Put);
        latest_updates
            .write()
            .await
            .insert(event.log_key(), event);

        // Two overlapping prefixes: first has 3-day lifespan (key is 2 days old, NOT expired),
        // second has 1-day lifespan (key IS expired). First match wins → key should survive.
        let gc_config = GarbageCollectionConfig {
            period: Duration::from_secs(30),
            lifespan: Duration::from_secs(86400),
            prefix_lifespans: Some(vec![
                PrefixLifespan {
                    key_expr: "**/events/**".try_into().unwrap(),
                    lifespan: Duration::from_secs(3 * 86400),
                    delete_data: true,
                },
                PrefixLifespan {
                    key_expr: "devices/**".try_into().unwrap(),
                    lifespan: Duration::from_secs(86400),
                    delete_data: true,
                },
            ]),
        };

        let mut gc_event = GarbageCollectionEvent {
            config: gc_config,
            storage: storage.clone(),
            wildcard_deletes: Arc::new(RwLock::new(KeBoxTree::default())),
            wildcard_puts: Arc::new(RwLock::new(KeBoxTree::default())),
            latest_updates: Some(latest_updates.clone()),
        };

        gc_event.run().await;

        // Verify: first prefix matched (**/events/**) with 3-day lifespan,
        // key is only 2 days old, so it should NOT be GC'd
        assert_eq!(
            latest_updates.read().await.len(),
            1,
            "Key should survive — first matching prefix has 3-day lifespan"
        );

        let mut s = storage.lock().await;
        let result = s.get(Some(event_key), "").await;
        assert!(
            result.is_ok(),
            "Storage data should be preserved — first matching prefix wins"
        );
    }

    #[tokio::test]
    async fn test_gc_default_behavior_no_prefix_lifespans() {
        let storage = make_memory_storage().await;

        // Put a key in storage
        let key: OwnedKeyExpr = "devices/42/status".try_into().unwrap();
        let old_ts = old_timestamp(Duration::from_secs(3 * 86400));
        {
            let mut s = storage.lock().await;
            s.put(
                Some(key.clone()),
                zenoh::bytes::ZBytes::default(),
                zenoh::bytes::Encoding::default(),
                old_ts,
            )
            .await
            .unwrap();
        }

        // GC config WITHOUT prefix_lifespans (default)
        let gc_config = GarbageCollectionConfig::default();

        let mut gc_event = GarbageCollectionEvent {
            config: gc_config,
            storage: storage.clone(),
            wildcard_deletes: Arc::new(RwLock::new(KeBoxTree::default())),
            wildcard_puts: Arc::new(RwLock::new(KeBoxTree::default())),
            latest_updates: Some(Arc::new(RwLock::new(HashMap::new()))),
        };

        gc_event.run().await;

        // Verify: storage data is NOT deleted (default GC only cleans metadata)
        let mut s = storage.lock().await;
        let result = s.get(Some(key), "").await;
        assert!(
            result.is_ok(),
            "Default GC should NOT delete storage data"
        );
    }
}
