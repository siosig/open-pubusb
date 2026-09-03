//! The core-layer error type and its mapping to gRPC status codes / HTTP
//! status codes, matching the Cloud Pub/Sub REST/gRPC error-code mapping.
//!
//! `Error` is transport-agnostic: the gRPC and REST layers (in the `open-pubusb`
//! binary crate) translate it into `tonic::Status` / `google.rpc.Status`
//! JSON respectively, using [`Error::code`] as the canonical source of
//! truth for which gRPC code / HTTP status a given error maps to.

use thiserror::Error as ThisError;

/// The result type used throughout the core crate.
pub type Result<T> = std::result::Result<T, Error>;

/// A core-layer error, transport-agnostic.
///
/// Every variant carries enough context to produce a useful message; the
/// mapping to gRPC codes and HTTP statuses is centralized in
/// [`Error::code`] rather than duplicated at each call site.
#[derive(Debug, Clone, PartialEq, Eq, ThisError)]
pub enum Error {
    /// The referenced resource (Topic / Subscription / Snapshot / ...)
    /// does not exist.
    #[error("not found: {resource}")]
    NotFound {
        /// A human-readable description of the missing resource, e.g. its
        /// full name.
        resource: String,
    },

    /// A resource with the same identity already exists.
    #[error("already exists: {resource}")]
    AlreadyExists {
        /// A human-readable description of the conflicting resource, e.g.
        /// its full name.
        resource: String,
    },

    /// A request field failed validation.
    #[error("invalid argument ({field}): {message}")]
    InvalidArgument {
        /// The offending field's name (request-relative, e.g.
        /// `"message_retention_duration"`).
        field: String,
        /// A human-readable explanation of why the value was rejected.
        message: String,
    },

    /// The request is well-formed but the system is not in a state that
    /// allows it (e.g. Pull on a detached subscription).
    #[error("failed precondition: {message}")]
    FailedPrecondition {
        /// A human-readable explanation.
        message: String,
    },

    /// The server is out of a needed resource (e.g. disk space).
    #[error("resource exhausted: {message}")]
    ResourceExhausted {
        /// A human-readable explanation.
        message: String,
    },

    /// The server is temporarily unable to serve the request.
    #[error("unavailable: {message}")]
    Unavailable {
        /// A human-readable explanation.
        message: String,
    },

    /// The requested method is not implemented by this server.
    #[error("unimplemented: {method}")]
    Unimplemented {
        /// The unimplemented method's name.
        method: String,
    },

    /// An unexpected internal error. Details are for logs only — never
    /// echoed back to the caller.
    #[error("internal error")]
    Internal {
        /// Internal-only details (logged, not returned to the caller).
        message: String,
    },
}

impl Error {
    /// The canonical gRPC status code name (e.g. `"NOT_FOUND"`) and the
    /// matching HTTP status code, per the contract's error-mapping table.
    pub fn code(&self) -> (&'static str, u16) {
        match self {
            Error::NotFound { .. } => ("NOT_FOUND", 404),
            Error::AlreadyExists { .. } => ("ALREADY_EXISTS", 409),
            Error::InvalidArgument { .. } => ("INVALID_ARGUMENT", 400),
            Error::FailedPrecondition { .. } => ("FAILED_PRECONDITION", 400),
            Error::ResourceExhausted { .. } => ("RESOURCE_EXHAUSTED", 429),
            Error::Unavailable { .. } => ("UNAVAILABLE", 503),
            Error::Unimplemented { .. } => ("UNIMPLEMENTED", 501),
            Error::Internal { .. } => ("INTERNAL", 500),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_found_maps_to_404() {
        let e = Error::NotFound {
            resource: "projects/p/topics/t".into(),
        };
        assert_eq!(e.code(), ("NOT_FOUND", 404));
    }

    #[test]
    fn already_exists_maps_to_409() {
        let e = Error::AlreadyExists {
            resource: "projects/p/topics/t".into(),
        };
        assert_eq!(e.code(), ("ALREADY_EXISTS", 409));
    }

    #[test]
    fn invalid_argument_maps_to_400() {
        let e = Error::InvalidArgument {
            field: "name".into(),
            message: "bad".into(),
        };
        assert_eq!(e.code(), ("INVALID_ARGUMENT", 400));
    }

    #[test]
    fn failed_precondition_maps_to_400() {
        let e = Error::FailedPrecondition {
            message: "bad state".into(),
        };
        assert_eq!(e.code(), ("FAILED_PRECONDITION", 400));
    }

    #[test]
    fn resource_exhausted_maps_to_429() {
        let e = Error::ResourceExhausted {
            message: "disk full".into(),
        };
        assert_eq!(e.code(), ("RESOURCE_EXHAUSTED", 429));
    }

    #[test]
    fn unavailable_maps_to_503() {
        let e = Error::Unavailable {
            message: "shutting down".into(),
        };
        assert_eq!(e.code(), ("UNAVAILABLE", 503));
    }

    #[test]
    fn unimplemented_maps_to_501() {
        let e = Error::Unimplemented {
            method: "SetIamPolicy".into(),
        };
        assert_eq!(e.code(), ("UNIMPLEMENTED", 501));
    }

    #[test]
    fn internal_maps_to_500() {
        let e = Error::Internal {
            message: "boom".into(),
        };
        assert_eq!(e.code(), ("INTERNAL", 500));
    }

    #[test]
    fn display_does_not_panic() {
        let e = Error::Internal {
            message: "boom".into(),
        };
        assert_eq!(e.to_string(), "internal error");
    }
}
