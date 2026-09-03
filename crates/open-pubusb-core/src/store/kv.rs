//! Keyspace-scoped key/value storage abstraction, backing every domain
//! module (`topic`, `subscription`, `delivery::engine`) uniformly whether
//! the concrete store is the in-memory [`MemKv`] (tests, `--ephemeral`) or
//! the persistent [`crate::store::fjall::FjallKv`] (User Story 3).
//!
//! `put`/`delete` return [`crate::error::Result`] (not `()`, as in this
//! module's original MVP-era placeholder version) because a real disk-backed
//! implementation can genuinely fail a write (I/O error, ENOSPC) and
//! callers up the stack (`PubSubService::publish` et al.) must be able to
//! surface that as an error to the client rather than silently reporting
//! success on a write that never became durable.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::error::{Error, Result};

/// A keyspace-scoped byte-oriented key/value store.
///
/// `keyspace` mirrors the logical keyspaces used throughout this crate
/// (`meta`, `msg`, `sub`, `dlv`, `okey`, `snap`, `idx`, ...): implementations
/// may map each
/// keyspace to a separate physical partition, but callers only need to
/// pass the keyspace name consistently.
pub trait KvStore: Send + Sync {
    /// Reads the value stored at `key` in `keyspace`, or `None` if absent.
    fn get(&self, keyspace: &str, key: &[u8]) -> Option<Vec<u8>>;

    /// Writes `value` at `key` in `keyspace`, overwriting any existing
    /// value.
    fn put(&self, keyspace: &str, key: Vec<u8>, value: Vec<u8>) -> Result<()>;

    /// Removes `key` from `keyspace`, if present.
    fn delete(&self, keyspace: &str, key: &[u8]) -> Result<()>;

    /// Returns every `(key, value)` pair in `keyspace` whose key starts
    /// with `prefix`.
    fn scan_prefix(&self, keyspace: &str, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)>;

    /// Approximate total on-disk size in bytes, or `0` if this store
    /// doesn't track disk usage (e.g. [`MemKv`], which has none). Backs
    /// `storage.max_disk_bytes` (`0` there means "unlimited") and the
    /// `open_pubusb_storage_disk_bytes` gauge.
    fn approx_disk_bytes(&self) -> u64 {
        0
    }
}

/// `(keyspace, key) -> value` map backing [`MemKv`].
type MemKvMap = HashMap<(&'static str, Vec<u8>), Vec<u8>>;

/// An in-memory [`KvStore`] backed by a single `Mutex<HashMap<..>>`, for use
/// in tests (and anywhere else an ephemeral store suffices).
pub struct MemKv(Mutex<MemKvMap>);

impl MemKv {
    /// Creates an empty in-memory store.
    pub fn new() -> Self {
        Self(Mutex::new(HashMap::new()))
    }
}

impl Default for MemKv {
    fn default() -> Self {
        Self::new()
    }
}

impl KvStore for MemKv {
    fn get(&self, keyspace: &str, key: &[u8]) -> Option<Vec<u8>> {
        let map = self.0.lock().ok()?;
        // `keyspace` is always one of a small set of static string literals
        // used consistently by callers, so this lookup relies on matching
        // one of the `&'static str` keys already stored in the map.
        map.iter()
            .find(|((ks, k), _)| *ks == keyspace && k.as_slice() == key)
            .map(|(_, v)| v.clone())
    }

    fn put(&self, keyspace: &str, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let mut map = self.0.lock().map_err(|_| Error::Internal {
            message: "MemKv mutex poisoned".to_string(),
        })?;
        let static_ks = intern_keyspace(keyspace);
        map.insert((static_ks, key), value);
        Ok(())
    }

    fn delete(&self, keyspace: &str, key: &[u8]) -> Result<()> {
        let mut map = self.0.lock().map_err(|_| Error::Internal {
            message: "MemKv mutex poisoned".to_string(),
        })?;
        map.retain(|(ks, k), _| !(*ks == keyspace && k.as_slice() == key));
        Ok(())
    }

    fn scan_prefix(&self, keyspace: &str, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let Ok(map) = self.0.lock() else {
            return Vec::new();
        };
        let mut results: Vec<(Vec<u8>, Vec<u8>)> = map
            .iter()
            .filter(|((ks, k), _)| *ks == keyspace && k.starts_with(prefix))
            .map(|((_, k), v)| (k.clone(), v.clone()))
            .collect();
        results.sort_by(|(a, _), (b, _)| a.cmp(b));
        results
    }
}

/// A [`KvStore`] that is one of exactly two concrete backends, chosen at
/// startup: [`MemKv`] (`--ephemeral` / `storage.ephemeral = true`) or
/// [`crate::store::fjall::FjallKv`] (persistent, the default).
///
/// `PubSubService<K>` (and every domain module it composes) stays generic
/// over `K: KvStore` — tests keep instantiating it directly with `MemKv`,
/// unaffected by this type's existence. `AnyKv` exists purely so
/// `crates/open-pubusb/src/main.rs` can pick a backend at runtime from one
/// config flag while still handing a *single* concrete `K` to every
/// generic type downstream (the gRPC/REST layers, the tonic health
/// reporter's `set_serving::<PublisherServer<PublisherService<K>>>()`
/// calls, ...) — Rust generics require one concrete type per
/// instantiation, so a runtime choice between two backends has to be
/// resolved into one type somewhere, and an enum is simpler and cheaper
/// than `Arc<dyn KvStore>` (which `KvStore`'s object-safe shape would
/// technically also allow, but at the cost of dynamic dispatch on every
/// call).
pub enum AnyKv {
    /// In-memory backend (`--ephemeral`).
    Mem(MemKv),
    /// Disk-backed backend (the default, persistent mode).
    Fjall(crate::store::fjall::FjallKv),
}

impl KvStore for AnyKv {
    fn get(&self, keyspace: &str, key: &[u8]) -> Option<Vec<u8>> {
        match self {
            Self::Mem(kv) => kv.get(keyspace, key),
            Self::Fjall(kv) => kv.get(keyspace, key),
        }
    }

    fn put(&self, keyspace: &str, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        match self {
            Self::Mem(kv) => kv.put(keyspace, key, value),
            Self::Fjall(kv) => kv.put(keyspace, key, value),
        }
    }

    fn delete(&self, keyspace: &str, key: &[u8]) -> Result<()> {
        match self {
            Self::Mem(kv) => kv.delete(keyspace, key),
            Self::Fjall(kv) => kv.delete(keyspace, key),
        }
    }

    fn scan_prefix(&self, keyspace: &str, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        match self {
            Self::Mem(kv) => kv.scan_prefix(keyspace, prefix),
            Self::Fjall(kv) => kv.scan_prefix(keyspace, prefix),
        }
    }

    fn approx_disk_bytes(&self) -> u64 {
        match self {
            Self::Mem(kv) => kv.approx_disk_bytes(),
            Self::Fjall(kv) => kv.approx_disk_bytes(),
        }
    }
}

impl AnyKv {
    /// Flushes accumulated writes per `mode` (FR-016 group sync / final
    /// shutdown sync). A no-op returning `Ok(())` for the [`Self::Mem`]
    /// backend, which has nothing to flush.
    pub fn persist(&self, mode: fjall::PersistMode) -> Result<()> {
        match self {
            Self::Mem(_) => Ok(()),
            Self::Fjall(kv) => kv.persist(mode),
        }
    }
}

/// Maps a caller-supplied keyspace name to one of a fixed set of
/// `&'static str` values, so [`MemKv`]'s map key doesn't need an owned
/// `String` per entry. Falls back to a generic bucket for unrecognized
/// names (in practice callers only ever use the fixed keyspace names).
fn intern_keyspace(keyspace: &str) -> &'static str {
    match keyspace {
        "meta" => "meta",
        "msg" => "msg",
        "sub" => "sub",
        "dlv" => "dlv",
        "okey" => "okey",
        "snap" => "snap",
        "idx" => "idx",
        _ => "other",
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn put_then_get_round_trip() {
        let kv = MemKv::new();
        kv.put("meta", b"k1".to_vec(), b"v1".to_vec()).unwrap();
        assert_eq!(kv.get("meta", b"k1"), Some(b"v1".to_vec()));
    }

    #[test]
    fn get_missing_is_none() {
        let kv = MemKv::new();
        assert_eq!(kv.get("meta", b"missing"), None);
    }

    #[test]
    fn keyspaces_are_isolated() {
        let kv = MemKv::new();
        kv.put("meta", b"k".to_vec(), b"meta-v".to_vec()).unwrap();
        kv.put("msg", b"k".to_vec(), b"msg-v".to_vec()).unwrap();
        assert_eq!(kv.get("meta", b"k"), Some(b"meta-v".to_vec()));
        assert_eq!(kv.get("msg", b"k"), Some(b"msg-v".to_vec()));
    }

    #[test]
    fn delete_removes_key() {
        let kv = MemKv::new();
        kv.put("meta", b"k".to_vec(), b"v".to_vec()).unwrap();
        kv.delete("meta", b"k").unwrap();
        assert_eq!(kv.get("meta", b"k"), None);
    }

    #[test]
    fn scan_prefix_returns_matching_sorted() {
        let kv = MemKv::new();
        kv.put("meta", b"name/t/b".to_vec(), b"2".to_vec()).unwrap();
        kv.put("meta", b"name/t/a".to_vec(), b"1".to_vec()).unwrap();
        kv.put("meta", b"name/s/a".to_vec(), b"other".to_vec())
            .unwrap();
        let results = kv.scan_prefix("meta", b"name/t/");
        assert_eq!(
            results,
            vec![
                (b"name/t/a".to_vec(), b"1".to_vec()),
                (b"name/t/b".to_vec(), b"2".to_vec()),
            ]
        );
    }
}
