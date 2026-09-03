//! Maps [`open_pubusb_core::Error`] to [`tonic::Status`], using the same
//! status mapping as the REST layer's `rest::error`.

use open_pubusb_core::Error;

/// Converts a core domain error into the gRPC status a client should see.
/// `Error::Internal`'s diagnostic message is deliberately not included in
/// the returned `Status` message (only logged by the caller) — see
/// `Error`'s own doc comment on why that field is log-only.
pub fn to_status(err: Error) -> tonic::Status {
    match err {
        Error::NotFound { resource } => tonic::Status::not_found(format!("{resource} not found")),
        Error::AlreadyExists { resource } => {
            tonic::Status::already_exists(format!("{resource} already exists"))
        }
        Error::InvalidArgument { field, message } => {
            tonic::Status::invalid_argument(format!("{field}: {message}"))
        }
        Error::FailedPrecondition { message } => tonic::Status::failed_precondition(message),
        Error::ResourceExhausted { message } => tonic::Status::resource_exhausted(message),
        Error::Unavailable { message } => tonic::Status::unavailable(message),
        Error::Unimplemented { method } => {
            tonic::Status::unimplemented(format!("{method} is not implemented"))
        }
        Error::Internal { message } => {
            tracing::error!(error = %message, "internal error");
            tonic::Status::internal("internal error")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_every_variant_to_the_documented_code() {
        let cases = [
            (
                Error::NotFound {
                    resource: "x".into(),
                },
                tonic::Code::NotFound,
            ),
            (
                Error::AlreadyExists {
                    resource: "x".into(),
                },
                tonic::Code::AlreadyExists,
            ),
            (
                Error::InvalidArgument {
                    field: "f".into(),
                    message: "m".into(),
                },
                tonic::Code::InvalidArgument,
            ),
            (
                Error::FailedPrecondition {
                    message: "m".into(),
                },
                tonic::Code::FailedPrecondition,
            ),
            (
                Error::ResourceExhausted {
                    message: "m".into(),
                },
                tonic::Code::ResourceExhausted,
            ),
            (
                Error::Unavailable {
                    message: "m".into(),
                },
                tonic::Code::Unavailable,
            ),
            (
                Error::Unimplemented { method: "m".into() },
                tonic::Code::Unimplemented,
            ),
            (
                Error::Internal {
                    message: "m".into(),
                },
                tonic::Code::Internal,
            ),
        ];
        for (err, expected) in cases {
            assert_eq!(to_status(err).code(), expected);
        }
    }

    #[test]
    fn internal_error_message_is_not_leaked_to_the_client() {
        let status = to_status(Error::Internal {
            message: "sensitive detail".into(),
        });
        assert!(!status.message().contains("sensitive detail"));
    }
}
