use std::sync::LazyLock;

use actix_web::http::header::ContentType;
use actix_web::web::Bytes;
use actix_web::{mime, HttpResponse};
use reqwest::{Response, StatusCode};
use reqwest::{header:: HeaderMap, Client};
use tokio::time::Instant;

use crate::API_KEY;
use crate::cache::UuidKey;
use crate::error::ProcessError;
use crate::logging::{LogMessage, log};
use crate::routes::stats::RateLimit;

static CLIENT: LazyLock<Client> = LazyLock::new(|| {
    let api_key = API_KEY.get().expect("Api key should have been set already!");

    let mut headers = HeaderMap::new();
    headers.insert("API-Key", api_key.parse().unwrap());
    
    Client::builder()
        .default_headers(headers.clone())
        .build()
        .unwrap()
});

/// Errors are never cached, so while hypixel is rate limiting us we fail fast instead of letting every client retry go upstream.
pub async fn request(key: UuidKey, url: String, rate_limit: &RateLimit) -> Result<Response, ProcessError> {
    if rate_limit.is_blocked() {
        log(LogMessage::MessageAndUser { key, message: "Rate limited, not hitting upstream" });
        return Err(ProcessError::Request(StatusCode::TOO_MANY_REQUESTS));
    }

    let now = Instant::now();
    let res = CLIENT.get(url).send().await?;
    log(LogMessage::ElapsedUserStatus { key, elapsed: now.elapsed(), message: "Upstream hit", code: res.status().as_u16() });
    // observed before error_for_status, which would otherwise discard the headers of a 429.
    rate_limit.observe(res.status(), res.headers());
    res.error_for_status().map_err(Into::into)
}

pub fn json_response(data: Bytes) -> HttpResponse {
    HttpResponse::Ok()
        .append_header(ContentType(mime::APPLICATION_JSON))
        .body(data)
}