use thiserror::Error;

use crate::state::string_enum;

string_enum! {
    /// Error code of a Home Assistant WebSocket error (`websocket_api/const.py`).
    pub enum HaErrorCode {
        HomeAssistantError => "home_assistant_error",
        InvalidFormat => "invalid_format",
        IdReuse => "id_reuse",
        NotFound => "not_found",
        NotSupported => "not_supported",
        NotAllowed => "not_allowed",
        UnknownCommand => "unknown_command",
        UnknownError => "unknown_error",
        Unauthorized => "unauthorized",
        Timeout => "timeout",
        TemplateError => "template_error",
        ServiceValidationError => "service_validation_error",
    }
}

/// Errors returned by [`HAClient`](crate::HAClient) methods.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// Home Assistant rejected the access token (`auth_invalid`).
    /// The client never retries after this error.
    #[error("authentication failed: {0}")]
    Auth(String),

    /// The WebSocket connection could not be established.
    #[error("connection error: {0}")]
    Connection(String),

    /// The client is currently reconnecting; the request was not sent.
    #[error("not connected to Home Assistant")]
    NotConnected,

    /// The connection dropped after the request was sent but before Home Assistant answered.
    #[error("connection lost before the request completed")]
    ConnectionLost,

    /// Home Assistant did not answer within the configured request timeout.
    #[error("request timed out")]
    Timeout,

    /// The client has been stopped (or its authentication failed permanently).
    #[error("client is stopped")]
    Stopped,

    /// Home Assistant answered the request with an error.
    #[error("Home Assistant error `{code}`: {message}")]
    Ha { code: HaErrorCode, message: String },

    /// Home Assistant sent something the client did not expect.
    #[error("unexpected response: {0}")]
    Protocol(String),

    /// A response could not be decoded into the expected type.
    #[error("failed to decode response: {0}")]
    Decode(#[from] serde_json::Error),

    /// An argument passed to the client was invalid (e.g. a malformed entity id).
    #[error("invalid input: {0}")]
    InvalidInput(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
