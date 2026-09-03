//! Layered configuration for `open-pubusb`.
//!
//! Layering (lowest → highest precedence):
//!
//! 1. hardcoded defaults (this module, via `#[serde(default = ...)]`)
//! 2. optional TOML file (`--config` / `OPEN_PUBUSB_CONFIG` / `/etc/open-pubusb/config.toml`,
//!    resolved by the caller — this module only knows the resolved path)
//! 3. environment variables, prefix `OPEN_PUBUSB`, section separator `__`
//!    (e.g. `OPEN_PUBUSB__SERVER__LISTEN=0.0.0.0:9000`)
//!
//! CLI flag overrides (layer 4) and `RUST_LOG` precedence over `log.level`
//! are intentionally NOT handled here — the former is `clap`'s job, and the
//! latter is exposed via [`effective_log_filter`] for the telemetry module
//! to call.
//!
//! Every struct is `#[serde(deny_unknown_fields)]` so a typo'd key in the
//! TOML file or an unrecognized env var segment is a hard startup failure:
//! an unknown key always causes startup to fail.

use std::path::Path;

use serde::{Deserialize, Serialize};

/// Upper bound for `storage.sync_interval_ms`.
///
/// The default is 50ms ("group fsync interval"), but there is no explicit
/// upper bound the way there is for `shutdown_grace_secs` (600s). 5000ms
/// (5s) is chosen as a generous but still-safe ceiling: beyond that the
/// group-commit window would make the FR-016 durability guarantee (bounded
/// data loss on crash) practically meaningless, and it is far outside
/// "tens of ms" the design targets.
const SYNC_INTERVAL_MS_MAX: u64 = 5_000;

/// Upper bound for `server.shutdown_grace_secs`; `shutdown_grace_secs > 600`
/// fails validation and startup.
const SHUTDOWN_GRACE_SECS_MAX: u64 = 600;

/// Ack deadline bounds (FR-005): 10-600 s. Mirrors the range
/// `open-pubusb-core`'s `limits` module will also enforce at the RPC layer;
/// duplicated here as plain constants because `open_pubusb_core::limits`
/// does not exist yet — keeping `config.rs` self-contained means it compiles
/// and its unit tests run today instead of depending on unlanded code. Once
/// that module lands, these should be replaced with
/// `open_pubusb_core::limits::ACK_DEADLINE_*` to avoid the two constant sets
/// drifting apart.
const ACK_DEADLINE_SECS_MIN: u32 = 10;
const ACK_DEADLINE_SECS_MAX: u32 = 600;

const VALID_LOG_FORMATS: &[&str] = &["json", "text", "journald"];

/// Top-level configuration; field names match the TOML keys exactly.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub delivery: DeliveryConfig,
    #[serde(default)]
    pub log: LogConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(default = "default_server_listen")]
    pub listen: String,
    #[serde(default = "default_server_admin_listen")]
    pub admin_listen: String,
    #[serde(default = "default_max_message_size_bytes")]
    pub max_message_size_bytes: u64,
    #[serde(default = "default_shutdown_grace_secs")]
    pub shutdown_grace_secs: u64,
    #[serde(default = "default_true")]
    pub enable_reflection: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: default_server_listen(),
            admin_listen: default_server_admin_listen(),
            max_message_size_bytes: default_max_message_size_bytes(),
            shutdown_grace_secs: default_shutdown_grace_secs(),
            enable_reflection: default_true(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
    #[serde(default = "default_false")]
    pub ephemeral: bool,
    #[serde(default = "default_sync_interval_ms")]
    pub sync_interval_ms: u64,
    #[serde(default = "default_cache_size_bytes")]
    pub cache_size_bytes: u64,
    #[serde(default = "default_max_disk_bytes")]
    pub max_disk_bytes: u64,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: default_data_dir(),
            ephemeral: default_false(),
            sync_interval_ms: default_sync_interval_ms(),
            cache_size_bytes: default_cache_size_bytes(),
            max_disk_bytes: default_max_disk_bytes(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryConfig {
    #[serde(default = "default_pull_max_wait_secs")]
    pub pull_max_wait_secs: u64,
    #[serde(default = "default_streaming_pull_max_lifetime_secs")]
    pub streaming_pull_max_lifetime_secs: u64,
    #[serde(default = "default_push_timeout_secs")]
    pub push_timeout_secs: u64,
    #[serde(default = "default_push_max_concurrency_per_sub")]
    pub push_max_concurrency_per_sub: u32,
    #[serde(default = "default_ack_deadline_secs_value")]
    pub default_ack_deadline_secs: u32,
    #[serde(default = "default_lease_scan_interval_ms")]
    pub lease_scan_interval_ms: u64,
    #[serde(default = "default_retention_sweep_interval_secs")]
    pub retention_sweep_interval_secs: u64,
}

impl Default for DeliveryConfig {
    fn default() -> Self {
        Self {
            pull_max_wait_secs: default_pull_max_wait_secs(),
            streaming_pull_max_lifetime_secs: default_streaming_pull_max_lifetime_secs(),
            push_timeout_secs: default_push_timeout_secs(),
            push_max_concurrency_per_sub: default_push_max_concurrency_per_sub(),
            default_ack_deadline_secs: default_ack_deadline_secs_value(),
            lease_scan_interval_ms: default_lease_scan_interval_ms(),
            retention_sweep_interval_secs: default_retention_sweep_interval_secs(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogConfig {
    #[serde(default = "default_log_format")]
    pub format: String,
    #[serde(default = "default_log_level")]
    pub level: String,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            format: default_log_format(),
            level: default_log_level(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
        }
    }
}

// --- default-value functions ---

fn default_true() -> bool {
    true
}

fn default_false() -> bool {
    false
}

fn default_server_listen() -> String {
    "0.0.0.0:8085".to_string()
}

fn default_server_admin_listen() -> String {
    "0.0.0.0:8086".to_string()
}

fn default_max_message_size_bytes() -> u64 {
    10_485_760
}

fn default_shutdown_grace_secs() -> u64 {
    30
}

fn default_data_dir() -> String {
    "/var/lib/open-pubusb".to_string()
}

fn default_sync_interval_ms() -> u64 {
    50
}

fn default_cache_size_bytes() -> u64 {
    33_554_432
}

fn default_max_disk_bytes() -> u64 {
    0
}

fn default_pull_max_wait_secs() -> u64 {
    90
}

fn default_streaming_pull_max_lifetime_secs() -> u64 {
    1_800
}

fn default_push_timeout_secs() -> u64 {
    10
}

fn default_push_max_concurrency_per_sub() -> u32 {
    16
}

fn default_ack_deadline_secs_value() -> u32 {
    10
}

fn default_lease_scan_interval_ms() -> u64 {
    100
}

fn default_retention_sweep_interval_secs() -> u64 {
    60
}

fn default_log_format() -> String {
    "json".to_string()
}

fn default_log_level() -> String {
    "info".to_string()
}

/// Errors surfaced while loading or validating configuration.
///
/// [`ConfigError::Invalid`] always names the offending key (dotted TOML
/// path, e.g. `"storage.sync_interval_ms"`) so callers can log a
/// `config_key` field: the startup-failure log is a single `level=error`
/// line that includes a `config_key` field.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("invalid config key '{key}': {message}")]
    Invalid { key: String, message: String },
    #[error(transparent)]
    Load(#[from] config::ConfigError),
}

impl ConfigError {
    fn invalid(key: &str, message: impl Into<String>) -> Self {
        ConfigError::Invalid {
            key: key.to_string(),
            message: message.into(),
        }
    }

    /// The `config_key` value to attach to the startup-failure log line,
    /// when available. Always available for [`ConfigError::Invalid`]
    /// (produced by [`Config::validate`], which always names its key).
    /// For [`ConfigError::Load`] (produced by [`Config::load`]), the
    /// `config` crate structurally exposes a key only for some of its
    /// variants (e.g. a type-mismatch on a specific field); an unknown-key
    /// error (`FileParse`) has no structured key field — the offending
    /// key name is still present, but only inside the error's message
    /// text, which callers get from this error's `Display`/`Error` impl.
    pub fn config_key(&self) -> Option<&str> {
        match self {
            ConfigError::Invalid { key, .. } => Some(key.as_str()),
            ConfigError::Load(config::ConfigError::Type { key: Some(k), .. }) => Some(k.as_str()),
            ConfigError::Load(config::ConfigError::At { key: Some(k), .. }) => Some(k.as_str()),
            ConfigError::Load(_) => None,
        }
    }
}

impl Config {
    /// Load configuration by layering defaults < optional TOML file <
    /// environment variables (`OPEN_PUBUSB__SECTION__KEY`).
    ///
    /// `config_file` is the already-resolved path (caller applies the
    /// `--config` / `OPEN_PUBUSB_CONFIG` / `/etc/open-pubusb/config.toml` precedence
    /// — that resolution is a CLI concern).
    /// A missing file is not an error (`required(false)`): defaults still
    /// apply, and env vars can still override them.
    ///
    /// This does NOT call [`Config::validate`] — callers must do so
    /// explicitly so `check-config` and `serve` can share this loader
    /// while producing distinct exit-code behavior around it.
    pub fn load(config_file: Option<&Path>) -> Result<Self, ConfigError> {
        // `config::File`'s TOML backend deserializes through a real serde
        // `Deserializer`, so `#[serde(deny_unknown_fields)]` on every
        // struct here is enforced directly by `try_deserialize` below —
        // no separate pre-parse pass is needed to catch a typo'd key
        // (verified: an unknown top-level or nested TOML key surfaces as
        // `ConfigError::Load(config::ConfigError::FileParse { .. })` whose
        // message names the offending field), satisfying the requirement
        // that an unknown key always causes startup to fail.
        let mut builder = config::Config::builder();
        if let Some(path) = config_file {
            builder = builder.add_source(config::File::from(path).required(false));
        }
        builder = builder.add_source(
            config::Environment::with_prefix("OPEN_PUBUSB")
                .separator("__")
                .try_parsing(true),
        );
        let raw = builder.build()?;
        let cfg: Config = raw.try_deserialize()?;
        Ok(cfg)
    }

    /// Validate every numerically/enum-checkable configuration rule.
    /// Returns the first violation found, naming the offending key.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.storage.sync_interval_ms == 0
            || self.storage.sync_interval_ms > SYNC_INTERVAL_MS_MAX
        {
            return Err(ConfigError::invalid(
                "storage.sync_interval_ms",
                format!(
                    "must be in 1..={SYNC_INTERVAL_MS_MAX} (got {})",
                    self.storage.sync_interval_ms
                ),
            ));
        }

        if self.server.shutdown_grace_secs == 0
            || self.server.shutdown_grace_secs > SHUTDOWN_GRACE_SECS_MAX
        {
            return Err(ConfigError::invalid(
                "server.shutdown_grace_secs",
                format!(
                    "must be in 1..={SHUTDOWN_GRACE_SECS_MAX} (got {})",
                    self.server.shutdown_grace_secs
                ),
            ));
        }

        if self.delivery.pull_max_wait_secs == 0 {
            return Err(ConfigError::invalid(
                "delivery.pull_max_wait_secs",
                "must be > 0",
            ));
        }

        if self.delivery.push_timeout_secs == 0 {
            return Err(ConfigError::invalid(
                "delivery.push_timeout_secs",
                "must be > 0",
            ));
        }

        if self.delivery.default_ack_deadline_secs < ACK_DEADLINE_SECS_MIN
            || self.delivery.default_ack_deadline_secs > ACK_DEADLINE_SECS_MAX
        {
            return Err(ConfigError::invalid(
                "delivery.default_ack_deadline_secs",
                format!(
                    "must be in {ACK_DEADLINE_SECS_MIN}..={ACK_DEADLINE_SECS_MAX} (got {})",
                    self.delivery.default_ack_deadline_secs
                ),
            ));
        }

        if !VALID_LOG_FORMATS.contains(&self.log.format.as_str()) {
            return Err(ConfigError::invalid(
                "log.format",
                format!(
                    "must be one of {VALID_LOG_FORMATS:?} (got '{}')",
                    self.log.format
                ),
            ));
        }

        if self.log.level.trim().is_empty() {
            return Err(ConfigError::invalid("log.level", "must not be empty"));
        }

        if !self.storage.ephemeral && self.storage.data_dir.trim().is_empty() {
            return Err(ConfigError::invalid(
                "storage.data_dir",
                "must not be empty when storage.ephemeral = false",
            ));
        }

        Ok(())
    }
}

/// The log filter string that should actually be used: `RUST_LOG` takes
/// precedence over `log.level` when set.
pub fn effective_log_filter(cfg: &LogConfig) -> String {
    std::env::var("RUST_LOG").unwrap_or_else(|_| cfg.level.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `cargo test` runs tests in multiple threads within one process, and
    /// `std::env::set_var`/`remove_var` are process-wide. Every test that
    /// touches `OPEN_PUBUSB__...`/`RUST_LOG` must hold this lock for its whole
    /// duration (including the final `remove_var` cleanup) so it can't
    /// interleave with another such test and leak a value into it.
    static ENV_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn default_config_is_valid() {
        let cfg = Config::default();
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.server.listen, "0.0.0.0:8085");
        assert_eq!(cfg.server.admin_listen, "0.0.0.0:8086");
        assert_eq!(cfg.server.max_message_size_bytes, 10_485_760);
        assert_eq!(cfg.server.shutdown_grace_secs, 30);
        assert!(cfg.server.enable_reflection);
        assert_eq!(cfg.storage.data_dir, "/var/lib/open-pubusb");
        assert!(!cfg.storage.ephemeral);
        assert_eq!(cfg.storage.sync_interval_ms, 50);
        assert_eq!(cfg.storage.cache_size_bytes, 33_554_432);
        assert_eq!(cfg.storage.max_disk_bytes, 0);
        assert_eq!(cfg.delivery.pull_max_wait_secs, 90);
        assert_eq!(cfg.delivery.streaming_pull_max_lifetime_secs, 1_800);
        assert_eq!(cfg.delivery.push_timeout_secs, 10);
        assert_eq!(cfg.delivery.push_max_concurrency_per_sub, 16);
        assert_eq!(cfg.delivery.default_ack_deadline_secs, 10);
        assert_eq!(cfg.delivery.lease_scan_interval_ms, 100);
        assert_eq!(cfg.delivery.retention_sweep_interval_secs, 60);
        assert_eq!(cfg.log.format, "json");
        assert_eq!(cfg.log.level, "info");
        assert!(cfg.metrics.enabled);
    }

    #[test]
    fn load_with_no_file_and_no_env_matches_default() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        // Use a path that cannot possibly exist; `required(false)` must
        // make this a no-op rather than an error.
        let cfg = Config::load(Some(Path::new(
            "/nonexistent/open-pubusb-config-test/does-not-exist.toml",
        )))
        .expect("load with missing optional file must succeed");
        assert_eq!(cfg.server.listen, Config::default().server.listen);
    }

    #[test]
    fn load_with_none_path_matches_default() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let cfg = Config::load(None).expect("load with no file must succeed");
        assert_eq!(cfg.server.listen, Config::default().server.listen);
    }

    #[test]
    fn sync_interval_ms_zero_is_invalid() {
        let mut cfg = Config::default();
        cfg.storage.sync_interval_ms = 0;
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.config_key(), Some("storage.sync_interval_ms"));
    }

    #[test]
    fn sync_interval_ms_over_max_is_invalid() {
        let mut cfg = Config::default();
        cfg.storage.sync_interval_ms = SYNC_INTERVAL_MS_MAX + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn shutdown_grace_secs_zero_is_invalid() {
        let mut cfg = Config::default();
        cfg.server.shutdown_grace_secs = 0;
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.config_key(), Some("server.shutdown_grace_secs"));
    }

    #[test]
    fn shutdown_grace_secs_over_600_is_invalid() {
        let mut cfg = Config::default();
        cfg.server.shutdown_grace_secs = 601;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn pull_max_wait_secs_zero_is_invalid() {
        let mut cfg = Config::default();
        cfg.delivery.pull_max_wait_secs = 0;
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.config_key(), Some("delivery.pull_max_wait_secs"));
    }

    #[test]
    fn push_timeout_secs_zero_is_invalid() {
        let mut cfg = Config::default();
        cfg.delivery.push_timeout_secs = 0;
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.config_key(), Some("delivery.push_timeout_secs"));
    }

    #[test]
    fn ack_deadline_below_min_is_invalid() {
        let mut cfg = Config::default();
        cfg.delivery.default_ack_deadline_secs = ACK_DEADLINE_SECS_MIN - 1;
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.config_key(), Some("delivery.default_ack_deadline_secs"));
    }

    #[test]
    fn ack_deadline_above_max_is_invalid() {
        let mut cfg = Config::default();
        cfg.delivery.default_ack_deadline_secs = ACK_DEADLINE_SECS_MAX + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn ack_deadline_bounds_are_inclusive() {
        let mut cfg = Config::default();
        cfg.delivery.default_ack_deadline_secs = ACK_DEADLINE_SECS_MIN;
        assert!(cfg.validate().is_ok());
        cfg.delivery.default_ack_deadline_secs = ACK_DEADLINE_SECS_MAX;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn log_format_invalid_value_is_invalid() {
        let mut cfg = Config::default();
        cfg.log.format = "xml".to_string();
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.config_key(), Some("log.format"));
    }

    #[test]
    fn log_format_accepts_all_documented_values() {
        for fmt in VALID_LOG_FORMATS {
            let mut cfg = Config::default();
            cfg.log.format = fmt.to_string();
            assert!(cfg.validate().is_ok(), "format '{fmt}' should be valid");
        }
    }

    #[test]
    fn log_level_empty_is_invalid() {
        let mut cfg = Config::default();
        cfg.log.level = "  ".to_string();
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.config_key(), Some("log.level"));
    }

    #[test]
    fn data_dir_empty_with_non_ephemeral_is_invalid() {
        let mut cfg = Config::default();
        cfg.storage.ephemeral = false;
        cfg.storage.data_dir = "".to_string();
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.config_key(), Some("storage.data_dir"));
    }

    #[test]
    fn data_dir_empty_with_ephemeral_is_valid() {
        let mut cfg = Config::default();
        cfg.storage.ephemeral = true;
        cfg.storage.data_dir = "".to_string();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn unknown_top_level_key_fails_to_load() {
        // An unknown key must always cause startup to fail.
        // Exercised through the real `Config::load` (TOML file source),
        // since that is the path production code actually takes.
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "open-pubusb-config-test-unknown-key-{}-{}.toml",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or_default()
        ));
        std::fs::write(&path, "[not_a_real_section]\nx = 1\n")
            .expect("writing the temp config file must succeed");

        let result = Config::load(Some(&path));
        let _ = std::fs::remove_file(&path);

        let err = result.expect_err("unknown top-level key must fail to load");
        assert!(matches!(err, ConfigError::Load(_)));
        assert!(
            err.to_string().contains("not_a_real_section"),
            "error message should name the offending key, got: {err}"
        );
    }

    #[test]
    fn unknown_nested_key_fails_to_load() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "open-pubusb-config-test-unknown-nested-key-{}-{}.toml",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or_default()
        ));
        std::fs::write(&path, "[server]\nnot_a_real_field = 1\n")
            .expect("writing the temp config file must succeed");

        let result = Config::load(Some(&path));
        let _ = std::fs::remove_file(&path);

        let err = result.expect_err("unknown nested key must fail to load");
        assert!(matches!(err, ConfigError::Load(_)));
        assert!(
            err.to_string().contains("not_a_real_field"),
            "error message should name the offending key, got: {err}"
        );
    }

    #[test]
    fn env_var_overrides_default() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        // SAFETY: this test mutates process-wide environment state. It
        // uses a variable name unlikely to collide with any other test in
        // this crate, and removes it again before returning (including on
        // the assertion path, since a `Config::default()` comparison
        // happens before the fallible calls) to avoid leaking state into
        // other tests running in the same process.
        const VAR: &str = "OPEN_PUBUSB__SERVER__LISTEN";
        unsafe {
            std::env::set_var(VAR, "127.0.0.1:19999");
        }
        let result = Config::load(None);
        unsafe {
            std::env::remove_var(VAR);
        }
        let cfg = result.expect("load with env override must succeed");
        assert_eq!(cfg.server.listen, "127.0.0.1:19999");
        // Everything else should remain at its default.
        assert_eq!(cfg.server.admin_listen, default_server_admin_listen());
    }

    #[test]
    fn env_var_bool_override_works() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        const VAR: &str = "OPEN_PUBUSB__STORAGE__EPHEMERAL";
        unsafe {
            std::env::set_var(VAR, "true");
        }
        let result = Config::load(None);
        unsafe {
            std::env::remove_var(VAR);
        }
        let cfg = result.expect("load with bool env override must succeed");
        assert!(cfg.storage.ephemeral);
    }

    #[test]
    fn file_source_overrides_default() {
        // Sensitive to other tests' env var mutations (env overrides file
        // per `Config::load`'s precedence), even though this test doesn't
        // set any env vars itself — see `ENV_MUTEX`'s doc comment.
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "open-pubusb-config-test-{}-{}.toml",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or_default()
        ));
        std::fs::write(&path, "[server]\nlisten = \"127.0.0.1:7777\"\n")
            .expect("writing the temp config file must succeed");

        let result = Config::load(Some(&path));
        let _ = std::fs::remove_file(&path);

        let cfg = result.expect("load with a valid file source must succeed");
        assert_eq!(cfg.server.listen, "127.0.0.1:7777");
        // Untouched keys keep their defaults.
        assert_eq!(cfg.server.admin_listen, default_server_admin_listen());
    }

    #[test]
    fn effective_log_filter_prefers_rust_log() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        const VAR: &str = "RUST_LOG";
        let previous = std::env::var(VAR).ok();
        unsafe {
            std::env::set_var(VAR, "debug,open_pubusb::delivery=trace");
        }
        let cfg = LogConfig {
            format: default_log_format(),
            level: "info".to_string(),
        };
        let filter = effective_log_filter(&cfg);
        unsafe {
            match &previous {
                Some(v) => std::env::set_var(VAR, v),
                None => std::env::remove_var(VAR),
            }
        }
        assert_eq!(filter, "debug,open_pubusb::delivery=trace");
    }

    #[test]
    fn effective_log_filter_falls_back_to_log_level() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        const VAR: &str = "RUST_LOG";
        let previous = std::env::var(VAR).ok();
        unsafe {
            std::env::remove_var(VAR);
        }
        let cfg = LogConfig {
            format: default_log_format(),
            level: "warn".to_string(),
        };
        let filter = effective_log_filter(&cfg);
        unsafe {
            if let Some(v) = &previous {
                std::env::set_var(VAR, v);
            }
        }
        assert_eq!(filter, "warn");
    }
}
