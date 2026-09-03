//! Maps [`open_pubusb_core::Error`] to the REST error body
//! `{"error":{"code":<http>,"message":...,"status":"<CANONICAL>"}}`.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use open_pubusb_core::Error;
use serde::Serialize;

#[derive(Serialize)]
struct ErrorBody {
    error: ErrorDetail,
}

#[derive(Serialize)]
struct ErrorDetail {
    code: u16,
    message: String,
    status: &'static str,
}

/// A REST-layer error: either a domain error or "this path/method is not
/// implemented" (used by the router fallback and by any handler covering
/// a method this server doesn't expose over REST yet).
pub enum RestError {
    Domain(Error),
    Unimplemented { message: String },
}

impl From<Error> for RestError {
    fn from(e: Error) -> Self {
        RestError::Domain(e)
    }
}

impl IntoResponse for RestError {
    fn into_response(self) -> Response {
        let (status, code, message, status_name) = match self {
            RestError::Domain(err) => {
                let (status_name, http_status) = err.code();
                let message = match &err {
                    Error::Internal { message } => {
                        tracing::error!(error = %message, "internal error");
                        "internal error".to_string()
                    }
                    Error::NotFound { resource } => format!("{resource} not found"),
                    Error::AlreadyExists { resource } => format!("{resource} already exists"),
                    Error::InvalidArgument { field, message } => format!("{field}: {message}"),
                    Error::FailedPrecondition { message }
                    | Error::ResourceExhausted { message }
                    | Error::Unavailable { message } => message.clone(),
                    Error::Unimplemented { method } => format!("{method} is not implemented"),
                };
                let status =
                    StatusCode::from_u16(http_status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                (status, http_status, message, status_name)
            }
            RestError::Unimplemented { message } => {
                (StatusCode::NOT_IMPLEMENTED, 501, message, "UNIMPLEMENTED")
            }
        };

        let body = ErrorBody {
            error: ErrorDetail {
                code,
                message,
                status: status_name,
            },
        };
        (status, Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_found_maps_to_404() {
        let err = RestError::Domain(Error::NotFound {
            resource: "x".into(),
        });
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn unimplemented_maps_to_501() {
        let err = RestError::Unimplemented {
            message: "not done".into(),
        };
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    }
}
