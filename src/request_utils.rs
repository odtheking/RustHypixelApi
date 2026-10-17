use std::{sync::LazyLock, time::Duration};

use actix_web::http::header::ContentType;
use actix_web::web::Bytes;
use actix_web::{mime, HttpResponse};
use reqwest::{Response, StatusCode};
use reqwest::{header:: HeaderMap, Client};
use tokio::time::Instant;

use crate::{API_KEY, env_var};
use crate::cache::{UuidKey, cache_key::MemoryPool, cache_router::Waiting};
use crate::error::ProcessError;
use crate::logging::{LogMessage, log};
use crate::routes::stats::RateLimit;

/// How long to wait for hypixel to connect.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

pub static CLIENT: LazyLock<Client> = LazyLock::new(|| {
    let api_key = API_KEY.get().expect("Api key should have been set already!");

    let mut headers = HeaderMap::new();
    headers.insert("API-Key", api_key.parse().unwrap());
    
    // without a timeout a hung connection would hold up everyone waiting on that request forever.
    Client::builder()
        .default_headers(headers.clone())
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(Duration::from_secs(env_var("UPSTREAM_TIMEOUT_SECONDS", 10)))
        .build()
        .unwrap()
});

/// Errors are never cached, so while hypixel is rate limiting us we fail fast instead of letting every client retry go upstream.
/// The request is paid for by one of the clients `waiting` for it, who are rate limited separately for each `pool` to protect hypixel from a single client.
pub async fn request(key: UuidKey, url: String, rate_limit: &RateLimit, waiting: &Waiting<'_>, pool: MemoryPool) -> Result<Response, ProcessError> {
    if rate_limit.is_blocked() {
        log(LogMessage::MessageAndUser { key, message: "Rate limited, not hitting upstream" });
        return Err(ProcessError::HypixelRateLimit);
    }
    // checked after hypixel's limit so a client doesn't spend its allowance on a request that is never sent.
    rate_limit.check_upstream(&waiting.clients(), pool).inspect_err(|_| log(LogMessage::MessageAndUser { key, message: "Every waiting client is rate limited, not hitting upstream" }))?;

    let now = Instant::now();
    let res = CLIENT.get(url).send().await
        .inspect_err(|err| log(LogMessage::KeyError { key, message: "Upstream request failed", error: err.to_string() }))?;
    log(LogMessage::ElapsedUserStatus { key, elapsed: now.elapsed(), message: "Upstream hit", code: res.status().as_u16() });
    // observed before error_for_status, which would otherwise discard the headers of a 429.
    rate_limit.observe(res.status(), res.headers());
    if res.status() == StatusCode::TOO_MANY_REQUESTS {
        return Err(ProcessError::HypixelRateLimit);
    }
    res.error_for_status().map_err(Into::into)
}

pub fn json_response(data: Bytes) -> HttpResponse {
    HttpResponse::Ok()
        .append_header(ContentType(mime::APPLICATION_JSON))
        .body(data)
}