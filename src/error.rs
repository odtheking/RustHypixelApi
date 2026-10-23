use std::{error::Error, fmt::Display};

use actix_web::{HttpResponse, ResponseError, http::{StatusCode as ActixStatusCode, header::ContentType}};
use reqwest::{Error as ReqwestError, StatusCode};
use simd_json::Error as SimdError;
use serde_json::Error as SerdeError;


#[derive(Clone, Debug)]
pub enum ProcessError {
    InternalServer(&'static str),
    /// Hypixel answered with an error status.
    Request(StatusCode),
    /// Hypixel did not answer in time.
    UpstreamTimeout,
    /// Hypixel could not be reached.
    UpstreamUnavailable,
    /// Hypixel answered with a body that can't be read, which it sometimes does when it cuts one short.
    UpstreamInvalid(String),
    Serialization(String),
    Database(String),
    /// Hypixel is rate limiting us.
    HypixelRateLimit,
    /// The client has made too many requests to this proxy.
    ProxyRateLimit,
}

/// The message is what clients show to the user as is, so it starts with who the error is from (`Hypixel error` or `Proxy error`)
/// and then says briefly what it is.
impl Display for ProcessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HypixelRateLimit => f.write_str("Hypixel error: rate limited, try again in a moment"),
            Self::Request(code) => write!(f, "Hypixel error: responded with {code}"),
            Self::UpstreamTimeout => f.write_str("Hypixel error: did not respond in time"),
            Self::UpstreamUnavailable => f.write_str("Hypixel error: could not be reached"),
            Self::UpstreamInvalid(msg) => write!(f, "Hypixel error: sent an unreadable response ({msg})"),
            Self::ProxyRateLimit => f.write_str("Proxy error: too many requests, slow down"),
            Self::InternalServer(msg) => write!(f, "Proxy error: {msg}"),
            Self::Serialization(msg) => write!(f, "Proxy error: could not read the response ({msg})"),
            Self::Database(msg) => write!(f, "Proxy error: database failed ({msg})"),
        }
    }
}

impl Error for ProcessError {}

impl ProcessError {
    /// Hypixel's body couldn't be read, because of `err`.
    pub fn upstream_invalid(err: impl Display) -> Self {
        Self::UpstreamInvalid(err.to_string())
    }

    /// Whether hypixel is why the request failed, as opposed to the client being rate limited or something being wrong here.
    pub const fn is_upstream(&self) -> bool {
        matches!(self, Self::HypixelRateLimit | Self::Request(_) | Self::UpstreamTimeout | Self::UpstreamUnavailable | Self::UpstreamInvalid(_))
    }
}

impl From<ReqwestError> for ProcessError {
    fn from(value: ReqwestError) -> Self {
        match value.status() {
            Some(StatusCode::TOO_MANY_REQUESTS) => Self::HypixelRateLimit,
            Some(status) => Self::Request(status),
            None if value.is_timeout() => Self::UpstreamTimeout,
            None => Self::UpstreamUnavailable,
        }
    }
}

impl From<SimdError> for ProcessError {
    fn from(value: SimdError) -> Self {
        Self::Serialization(value.to_string())
    }
}

impl From<SerdeError> for ProcessError {
    fn from(value: SerdeError) -> Self {
        Self::Serialization(value.to_string())
    }
}

impl From<ltmdb::Error> for ProcessError {
    fn from(value: ltmdb::Error) -> Self {
        Self::Database(value.to_string())
    }
}

impl ResponseError for ProcessError {
    fn status_code(&self) -> ActixStatusCode {
        match self {
            Self::Request(_) | Self::UpstreamUnavailable | Self::UpstreamInvalid(_) => ActixStatusCode::BAD_GATEWAY,
            Self::UpstreamTimeout => ActixStatusCode::GATEWAY_TIMEOUT,
            Self::HypixelRateLimit | Self::ProxyRateLimit => ActixStatusCode::TOO_MANY_REQUESTS,
            _ => ActixStatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Plain text, so a client can show the body as the error as is.
    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code()).insert_header(ContentType::plaintext()).body(self.to_string())
    }
}
