//! The REST (HTTP/JSON) subset of the API, multiplexed onto the same port
//! as gRPC by `crates/open-pubusb/src/server.rs`.

pub mod error;
pub mod handlers;
pub mod router;

use axum::response::IntoResponse;

use crate::rest::error::RestError;

/// Any `/v1/...` path/method combination not covered by
/// [`router::router`] — everything outside the 12-endpoint subset this
/// server implements over REST. Any other `/v1/...` path is
/// `501 Not Implemented`.
pub async fn fallback() -> impl IntoResponse {
    RestError::Unimplemented {
        message: "this REST method is not implemented by this server".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;

    #[tokio::test]
    async fn fallback_returns_501() {
        let resp = fallback().await.into_response();
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    }
}
