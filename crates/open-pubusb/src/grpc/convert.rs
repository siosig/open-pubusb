//! `Timestamp`/`Duration` conversions between the millisecond/second
//! integers `open-pubusb-core` works in and the proto3 well-known types
//! (`::pbjson_types::{Timestamp,Duration}`) the generated gRPC/REST types
//! use.

/// Milliseconds since the Unix epoch -> proto `Timestamp`.
pub fn ms_to_timestamp(ms: i64) -> pbjson_types::Timestamp {
    pbjson_types::Timestamp {
        seconds: ms.div_euclid(1000),
        nanos: (ms.rem_euclid(1000) * 1_000_000) as i32,
    }
}

/// Proto `Timestamp` -> milliseconds since the Unix epoch. Not yet called
/// from production code (every current caller only builds
/// `Timestamp`s, never parses one back) but kept as the natural
/// counterpart to `ms_to_timestamp` for when a future task (Push,
/// StreamingPull) needs to round-trip one.
#[allow(dead_code)]
pub fn timestamp_to_ms(ts: &pbjson_types::Timestamp) -> i64 {
    ts.seconds.saturating_mul(1000) + i64::from(ts.nanos) / 1_000_000
}

/// Seconds -> proto `Duration`.
pub fn secs_to_duration(secs: i64) -> pbjson_types::Duration {
    pbjson_types::Duration {
        seconds: secs,
        nanos: 0,
    }
}

/// Proto `Duration` -> whole seconds (sub-second precision is not used
/// anywhere in this server's domain model, so it's truncated, matching
/// every duration field this server exposes — ack deadlines, retention,
/// backoff — which are always specified as whole seconds).
pub fn duration_to_secs(d: &pbjson_types::Duration) -> i64 {
    d.seconds
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_round_trips_millisecond_precision() {
        let ms = 1_700_000_123_456;
        let ts = ms_to_timestamp(ms);
        assert_eq!(ts.seconds, 1_700_000_123);
        assert_eq!(ts.nanos, 456_000_000);
        assert_eq!(timestamp_to_ms(&ts), ms);
    }

    #[test]
    fn timestamp_handles_zero() {
        assert_eq!(timestamp_to_ms(&ms_to_timestamp(0)), 0);
    }

    #[test]
    fn duration_round_trips_whole_seconds() {
        let d = secs_to_duration(600);
        assert_eq!(duration_to_secs(&d), 600);
    }
}
