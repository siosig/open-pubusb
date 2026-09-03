//! Persistent [`KvStore`] implementation backed by `fjall` 3.x.
//!
//! One [`fjall::Database`] per data directory, with one `fjall::Keyspace`
//! (physical LSM-tree) per logical keyspace name (`meta`, `msg`, `sub`,
//! `dlv`, `okey`, `snap`, `idx`, ...) — created lazily on first use via
//! `Database::keyspace`, which is itself create-or-get and thread-safe, so
//! this type does not need its own keyspace-handle cache.
//!
//! ## Write durability (FR-016 "group sync")
//!
//! The `Database` is opened with fjall's *default* `manual_journal_persist
//! = false`: every `insert`/`remove` internally calls
//! `persist(PersistMode::Buffer)` before returning, i.e. the write reaches
//! the OS (survives this *process* crashing or being killed, `kill -9`
//! included — verified by `crates/open-pubusb-core/tests` / `tests/integration`)
//! but is not yet `fsync`ed (does not survive a power loss / OS crash).
//! That matches the contract that insert/batch return once written to the
//! OS journal, with fsync decoupled, exactly. A caller
//! (`crates/open-pubusb/src/main.rs`'s group-sync timer) is responsible for
//! calling [`FjallKv::persist`] with `PersistMode::SyncData` every
//! `sync_interval_ms` to `fdatasync` accumulated writes as one batch —
//! that is the "group sync": batching the *expensive* fsync-class flush
//! across many writes, not deferring the (cheap) OS-buffer flush itself.
//!
//! Deliberately **not** `manual_journal_persist(true)`: that flag defers
//! even the OS-buffer flush to the next explicit `persist()` call, which
//! would leave every write vulnerable to a `kill -9` for up to
//! `sync_interval_ms` — a materially weaker guarantee than FR-016
//! specifies, and worth calling out explicitly since it is easy to
//! misread the two `PersistMode`/`manual_journal_persist` knobs as
//! interchangeable.
//!
//! ## Format version
//!
//! [`FjallKv::open`] writes a `format_version` marker into the `meta`
//! keyspace the first time a data directory is used, and refuses to open
//! (returning [`OpenError::UnsupportedFormatVersion`]) if an existing
//! marker is *newer* than [`FORMAT_VERSION`] — an older or matching marker
//! opens normally (this binary is expected to read its own past formats).
//!
//! ## Fallibility
//!
//! [`KvStore::put`]/[`KvStore::delete`] return [`crate::error::Result`] —
//! unlike the in-memory [`crate::store::kv::MemKv`], a disk-backed write
//! can genuinely fail (I/O error, ENOSPC, a prior poisoned journal), and
//! callers must be able to surface that as an error rather than silently
//! reporting success on a write that never became durable. An
//! `Error::Io` whose `ErrorKind` is `StorageFull` maps to
//! [`crate::error::Error::ResourceExhausted`]; every other fjall error
//! maps to [`crate::error::Error::Internal`].

use std::path::{Path, PathBuf};

use fjall::{Database, KeyspaceCreateOptions, PersistMode};

use crate::error::{Error, Result};
use crate::store::kv::KvStore;

/// The current on-disk format version this binary writes and reads.
/// Bump only alongside a documented, intentional key/value-encoding
/// change; existing data directories at this version (or older) still
/// open normally.
pub const FORMAT_VERSION: u32 = 1;

const FORMAT_VERSION_KEYSPACE: &str = "meta";
const FORMAT_VERSION_KEY: &[u8] = b"__open_pubusb_format_version";

/// Failure modes specific to opening a [`FjallKv`] data directory — kept
/// separate from [`crate::error::Error`] because these are startup-time
/// (pre-request) failures, reported by `crates/open-pubusb/src/main.rs` as a
/// process exit code and log line rather than surfaced to any client.
#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    /// The underlying storage engine failed to open `path` (I/O error,
    /// locked by another process, corrupt on-disk state, ...).
    #[error("failed to open open-pubusb data directory {path}: {source}")]
    Storage {
        /// The data directory that failed to open.
        path: PathBuf,
        #[source]
        /// The underlying `fjall` error.
        source: fjall::Error,
    },
    /// `path` already contains a data directory written by a *newer*
    /// version of this binary than [`FORMAT_VERSION`] supports.
    #[error(
        "open-pubusb data directory {path} was written with format version {found}, but this binary only supports up to {supported}; upgrade open-pubusb, or point storage.data_dir at an empty directory"
    )]
    UnsupportedFormatVersion {
        /// The data directory whose format is too new.
        path: PathBuf,
        /// The format version found on disk.
        found: u32,
        /// The newest format version this binary supports ([`FORMAT_VERSION`]).
        supported: u32,
    },
    /// The persisted format-version marker itself could not be decoded
    /// (corrupt or truncated — 4 bytes expected).
    #[error("open-pubusb data directory {path} has a corrupt format-version marker")]
    CorruptFormatVersionMarker {
        /// The data directory with the corrupt marker.
        path: PathBuf,
    },
}

/// A persistent, disk-backed [`KvStore`], one `fjall::Database` per data
/// directory.
pub struct FjallKv {
    db: Database,
}

impl FjallKv {
    /// Opens (creating if necessary) a `FjallKv` rooted at `path`.
    ///
    /// `cache_size_bytes` is fjall's shared block cache capacity across
    /// every keyspace (`storage.cache_size_bytes`, recommended default
    /// 32 MiB).
    pub fn open(path: &Path, cache_size_bytes: u64) -> std::result::Result<Self, OpenError> {
        std::fs::create_dir_all(path).map_err(|e| OpenError::Storage {
            path: path.to_path_buf(),
            source: fjall::Error::Io(e),
        })?;
        let db = Database::builder(path)
            .cache_size(cache_size_bytes)
            .open()
            .map_err(|source| OpenError::Storage {
                path: path.to_path_buf(),
                source,
            })?;
        let store = Self { db };
        store.check_and_write_format_version(path)?;
        Ok(store)
    }

    fn check_and_write_format_version(&self, path: &Path) -> std::result::Result<(), OpenError> {
        let meta = self
            .db
            .keyspace(FORMAT_VERSION_KEYSPACE, KeyspaceCreateOptions::default)
            .map_err(|source| OpenError::Storage {
                path: path.to_path_buf(),
                source,
            })?;
        match meta
            .get(FORMAT_VERSION_KEY)
            .map_err(|source| OpenError::Storage {
                path: path.to_path_buf(),
                source,
            })? {
            Some(bytes) => {
                let arr: [u8; 4] = bytes.as_ref().try_into().map_err(|_| {
                    OpenError::CorruptFormatVersionMarker {
                        path: path.to_path_buf(),
                    }
                })?;
                let found = u32::from_be_bytes(arr);
                if found > FORMAT_VERSION {
                    return Err(OpenError::UnsupportedFormatVersion {
                        path: path.to_path_buf(),
                        found,
                        supported: FORMAT_VERSION,
                    });
                }
                Ok(())
            }
            None => meta
                .insert(FORMAT_VERSION_KEY, FORMAT_VERSION.to_be_bytes())
                .map_err(|source| OpenError::Storage {
                    path: path.to_path_buf(),
                    source,
                }),
        }
    }

    /// Flushes the journal per `mode` (the group-sync timer calls this with
    /// `PersistMode::SyncData` every `sync_interval_ms`; graceful shutdown
    /// calls it once more with `PersistMode::SyncAll` before exiting).
    pub fn persist(&self, mode: PersistMode) -> Result<()> {
        self.db.persist(mode).map_err(map_fjall_error)
    }

    /// Total on-disk size in bytes (journal + every keyspace), for
    /// `open_pubusb_storage_disk_bytes` and the `storage.max_disk_bytes` guard.
    pub fn disk_space_bytes(&self) -> Result<u64> {
        self.db.disk_space().map_err(map_fjall_error)
    }

    fn keyspace(&self, name: &str) -> Result<fjall::Keyspace> {
        self.db
            .keyspace(name, KeyspaceCreateOptions::default)
            .map_err(map_fjall_error)
    }
}

/// Maps a `fjall::Error` to the domain [`Error`] type. An I/O error whose
/// `ErrorKind` is `StorageFull` (ENOSPC) becomes `ResourceExhausted`, so
/// callers can surface it distinctly (`RESOURCE_EXHAUSTED` / 429) instead
/// of a generic internal failure; everything else — a poisoned journal
/// (fatal, hardware-related per fjall's own docs), a locked database, a
/// corrupt on-disk structure — becomes `Internal`.
fn map_fjall_error(e: fjall::Error) -> Error {
    if let fjall::Error::Io(io_err) = &e {
        if io_err.kind() == std::io::ErrorKind::StorageFull {
            return Error::ResourceExhausted {
                message: format!("storage engine reported ENOSPC: {e}"),
            };
        }
    }
    Error::Internal {
        message: format!("storage engine error: {e}"),
    }
}

impl KvStore for FjallKv {
    fn get(&self, keyspace: &str, key: &[u8]) -> Option<Vec<u8>> {
        // Infallible per the `KvStore::get` signature (`Option`, not
        // `Result`) — a read failure here (rather than a write failure) is
        // rare enough, and inconsequential enough (the caller simply sees
        // "not found" instead of a hard error), that logging and treating
        // it as absent is an acceptable simplification; every fallible
        // operation that actually matters for durability (put/delete) does
        // propagate its error.
        match self.keyspace(keyspace) {
            Ok(ks) => match ks.get(key) {
                Ok(v) => v.map(|slice| slice.to_vec()),
                Err(e) => {
                    tracing::error!(%keyspace, error = %e, "FjallKv::get failed");
                    None
                }
            },
            Err(e) => {
                tracing::error!(%keyspace, error = ?e, "FjallKv::get: failed to open keyspace");
                None
            }
        }
    }

    fn put(&self, keyspace: &str, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.keyspace(keyspace)?
            .insert(key, value)
            .map_err(map_fjall_error)
    }

    fn delete(&self, keyspace: &str, key: &[u8]) -> Result<()> {
        self.keyspace(keyspace)?
            .remove(key)
            .map_err(map_fjall_error)
    }

    fn scan_prefix(&self, keyspace: &str, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let Ok(ks) = self.keyspace(keyspace) else {
            return Vec::new();
        };
        ks.prefix(prefix)
            .filter_map(|guard| guard.into_inner().ok())
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect()
    }

    fn approx_disk_bytes(&self) -> u64 {
        self.disk_space_bytes().unwrap_or_else(|e| {
            tracing::error!(error = ?e, "FjallKv::approx_disk_bytes failed");
            0
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn open_temp() -> (tempfile::TempDir, FjallKv) {
        let dir = tempfile::tempdir().unwrap();
        let kv = FjallKv::open(dir.path(), 8 * 1024 * 1024).unwrap();
        (dir, kv)
    }

    #[test]
    fn put_then_get_round_trip() {
        let (_dir, kv) = open_temp();
        kv.put("meta", b"k1".to_vec(), b"v1".to_vec()).unwrap();
        assert_eq!(kv.get("meta", b"k1"), Some(b"v1".to_vec()));
    }

    #[test]
    fn delete_removes_key() {
        let (_dir, kv) = open_temp();
        kv.put("meta", b"k".to_vec(), b"v".to_vec()).unwrap();
        kv.delete("meta", b"k").unwrap();
        assert_eq!(kv.get("meta", b"k"), None);
    }

    #[test]
    fn keyspaces_are_isolated() {
        let (_dir, kv) = open_temp();
        kv.put("meta", b"k".to_vec(), b"meta-v".to_vec()).unwrap();
        kv.put("msg", b"k".to_vec(), b"msg-v".to_vec()).unwrap();
        assert_eq!(kv.get("meta", b"k"), Some(b"meta-v".to_vec()));
        assert_eq!(kv.get("msg", b"k"), Some(b"msg-v".to_vec()));
    }

    #[test]
    fn scan_prefix_returns_matching_sorted() {
        let (_dir, kv) = open_temp();
        kv.put("meta", b"name/t/b".to_vec(), b"2".to_vec()).unwrap();
        kv.put("meta", b"name/t/a".to_vec(), b"1".to_vec()).unwrap();
        kv.put("meta", b"name/s/a".to_vec(), b"other".to_vec())
            .unwrap();
        let mut results = kv.scan_prefix("meta", b"name/t/");
        results.sort_by(|(a, _), (b, _)| a.cmp(b));
        assert_eq!(
            results,
            vec![
                (b"name/t/a".to_vec(), b"1".to_vec()),
                (b"name/t/b".to_vec(), b"2".to_vec()),
            ]
        );
    }

    #[test]
    fn data_survives_reopen_at_the_same_path() {
        let dir = tempfile::tempdir().unwrap();
        {
            let kv = FjallKv::open(dir.path(), 8 * 1024 * 1024).unwrap();
            kv.put("meta", b"k".to_vec(), b"v".to_vec()).unwrap();
            kv.persist(PersistMode::SyncAll).unwrap();
        }
        let kv = FjallKv::open(dir.path(), 8 * 1024 * 1024).unwrap();
        assert_eq!(kv.get("meta", b"k"), Some(b"v".to_vec()));
    }

    /// A write survives a plain `drop()` with no explicit `persist()` call
    /// — proving the per-write `PersistMode::Buffer` flush this module's
    /// doc comment describes (via the default `manual_journal_persist =
    /// false`) actually happens inside `put` itself, not only as a side
    /// effect of fjall's own `Drop` impl (which also attempts a final
    /// `SyncAll`, muddying what this test would otherwise prove) doing
    /// its own cleanup. A **process kill** (as opposed to an in-process
    /// `drop()`, which cannot be prevented from running fjall's `Drop`
    /// impl without leaking the file lock and breaking the very reopen
    /// this test needs) is proven for real, at the OS level, by
    /// `scripts/qa/durability.sh` — killing a real,
    /// separate `open-pubusb` process with `SIGKILL` immediately after a
    /// publish, well inside `sync_interval_ms`.
    #[test]
    fn put_survives_a_plain_drop_with_no_persist_call() {
        let dir = tempfile::tempdir().unwrap();
        {
            let kv = FjallKv::open(dir.path(), 8 * 1024 * 1024).unwrap();
            kv.put("meta", b"k".to_vec(), b"v".to_vec()).unwrap();
        }
        let kv = FjallKv::open(dir.path(), 8 * 1024 * 1024).unwrap();
        assert_eq!(kv.get("meta", b"k"), Some(b"v".to_vec()));
    }

    #[test]
    fn first_open_writes_current_format_version() {
        let (_dir, kv) = open_temp();
        let stored = kv.get(FORMAT_VERSION_KEYSPACE, FORMAT_VERSION_KEY).unwrap();
        let arr: [u8; 4] = stored.try_into().unwrap();
        assert_eq!(u32::from_be_bytes(arr), FORMAT_VERSION);
    }

    #[test]
    fn newer_format_version_refuses_to_open() {
        let dir = tempfile::tempdir().unwrap();
        {
            let kv = FjallKv::open(dir.path(), 8 * 1024 * 1024).unwrap();
            kv.put(
                FORMAT_VERSION_KEYSPACE,
                FORMAT_VERSION_KEY.to_vec(),
                (FORMAT_VERSION + 1).to_be_bytes().to_vec(),
            )
            .unwrap();
            kv.persist(PersistMode::SyncAll).unwrap();
        }
        match FjallKv::open(dir.path(), 8 * 1024 * 1024) {
            Err(OpenError::UnsupportedFormatVersion { .. }) => {}
            other => panic!("expected UnsupportedFormatVersion, got {}", other.is_ok()),
        }
    }

    #[test]
    fn disk_space_bytes_is_nonzero_after_a_write() {
        let (_dir, kv) = open_temp();
        kv.put("meta", b"k".to_vec(), vec![0u8; 4096]).unwrap();
        kv.persist(PersistMode::SyncAll).unwrap();
        assert!(kv.disk_space_bytes().unwrap() > 0);
    }
}
