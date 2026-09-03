//! The persistence layer: keyspace value encodings ([`codec`]), binary key
//! builders/parsers ([`keys`]), the storage-engine-agnostic [`kv::KvStore`]
//! trait, and its two implementations (in-memory [`kv::MemKv`], disk-backed
//! [`fjall::FjallKv`]).

pub mod codec;
pub mod fjall;
pub mod keys;
pub mod kv;
