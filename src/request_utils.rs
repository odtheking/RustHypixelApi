use std::{sync::LazyLock, time::Duration};

use actix_web::http::header::{ACCEPT_ENCODING as CLIENT_ACCEPT_ENCODING, ContentEncoding, ContentType, VARY};
use actix_web::web::Bytes;
use actix_web::{mime, HttpRequest, HttpResponse};
use reqwest::StatusCode;
use reqwest::{header::{ACCEPT_ENCODING, CONTENT_ENCODING, HeaderMap, HeaderValue}, Client};
use tokio::time::{Instant, timeout};

use crate::{API_KEY, env_var};
use crate::cache::{UuidKey, cache_key::MemoryPool, cache_router::Waiting, compression::{gunzip, gunzip_blocking}};
use crate::error::ProcessError;
use crate::logging::{LogMessage, log};
use crate::routes::stats::RateLimit;

/// How long to wait for hypixel to connect.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long to wait for hypixel to start answering.
pub static UPSTREAM_TIMEOUT: LazyLock<Duration> = LazyLock::new(|| Duration::from_secs(env_var("UPSTREAM_TIMEOUT_SECONDS", 10)));
/// How long to wait for the rest of the answer once hypixel has started it. This is separate because hypixel often answers the headers
/// right away and then stalls on the body. That request is already paid for, so giving up on it early only makes the next caller send it again.
pub static UPSTREAM_BODY_TIMEOUT: LazyLock<Duration> = LazyLock::new(|| Duration::from_secs(env_var("UPSTREAM_BODY_TIMEOUT_SECONDS", 45)));

pub static CLIENT: LazyLock<Client> = LazyLock::new(|| {
    let api_key = API_KEY.get().expect("Api key should have been set already!");

    let mut headers = HeaderMap::new();
    headers.insert("API-Key", api_key.parse().unwrap());
    // asked for by hand, as reqwest would unzip it. The gzipped body is kept as is, see `compression`.
    headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip"));

    // without a timeout a hung connection would hold up everyone waiting on that request forever. These are applied in `request`, as
    // reqwest has no timeout that is longer for the body than for the headers.
    Client::builder()
        .default_headers(headers.clone())
        .connect_timeout(CONNECT_TIMEOUT)
        .build()
        .unwrap()
});

/// A body from hypixel, as it was sent.
pub struct Upstream {
    pub body: Bytes,
    /// whether hypixel gzipped it, which it usually does as we ask for it.
    pub gzipped: bool,
}

impl Upstream {
    /// The body as plain json, which is a copy that can be parsed in place. One that can't be unzipped is hypixel's fault, as it
    /// sometimes answers 200 with a body cut short. Megabytes for profiles, so callers run this off the thread answering requests.
    pub fn json(&self) -> Result<Vec<u8>, ProcessError> {
        if self.gzipped {
            gunzip_blocking(&self.body).map_err(ProcessError::upstream_invalid)
        } else {
            Ok(self.body.to_vec())
        }
    }
}

/// Errors are never cached, so while hypixel is rate limiting us we fail fast instead of letting every client retry go upstream.
/// The request is paid for by one of the clients `waiting` for it, who are rate limited separately for each `pool` to protect hypixel from a single client.
pub async fn request(key: UuidKey, url: String, rate_limit: &RateLimit, waiting: &Waiting<'_>, pool: MemoryPool) -> Result<Upstream, ProcessError> {
    if rate_limit.is_blocked() {
        log(LogMessage::MessageAndUser { key, message: "Rate limited, not hitting upstream" });
        return Err(ProcessError::HypixelRateLimit);
    }
    // checked after hypixel's limit so a client doesn't spend its allowance on a request that is never sent.
    rate_limit.check_upstream(&waiting.clients(), pool).inspect_err(|_| log(LogMessage::MessageAndUser { key, message: "Every waiting client is rate limited, not hitting upstream" }))?;

    let now = Instant::now();
    let res = timeout(*UPSTREAM_TIMEOUT, CLIENT.get(url).send()).await
        .map_err(|_| ProcessError::UpstreamTimeout)
        .and_then(|res| res.map_err(ProcessError::from))
        .inspect_err(|err| log(LogMessage::KeyError { key, message: "Upstream request failed", error: err.to_string() }))?;
    log(LogMessage::ElapsedUserStatus { key, elapsed: now.elapsed(), message: "Upstream hit", code: res.status().as_u16() });
    // observed before error_for_status, which would otherwise discard the headers of a 429.
    rate_limit.observe(res.status(), res.headers());
    if res.status() == StatusCode::TOO_MANY_REQUESTS {
        return Err(ProcessError::HypixelRateLimit);
    }
    let res = res.error_for_status()?;
    // gzip is the only encoding asked for, anything else would be passed on as if it were plain json. x-gzip is another name for it.
    let gzipped = match res.headers().get(CONTENT_ENCODING).map(|encoding| encoding.to_str().map(str::trim)) {
        None => false,
        Some(Ok(encoding)) if encoding.eq_ignore_ascii_case("gzip") || encoding.eq_ignore_ascii_case("x-gzip") => true,
        Some(Ok(encoding)) if encoding.is_empty() || encoding.eq_ignore_ascii_case("identity") => false,
        Some(_) => return Err(ProcessError::upstream_invalid("unexpected encoding")),
    };

    // a failure here is logged since "Upstream hit" above only means hypixel started answering, and nothing is cached when the body is lost.
    let now = Instant::now();
    let body = timeout(*UPSTREAM_BODY_TIMEOUT, res.bytes()).await
        .map_err(|_| ProcessError::UpstreamTimeout)
        .and_then(|bytes| bytes.map_err(ProcessError::from))
        .inspect_err(|err| log(LogMessage::KeyError { key, message: "Upstream response body failed", error: err.to_string() }))?;
    log(LogMessage::ElapsedUserSize { key, elapsed: now.elapsed(), message: if gzipped { "Upstream body (gzip)" } else { "Upstream body (plain)" }, size: body.len() });
    Ok(Upstream { body, gzipped })
}

pub fn json_response(data: Bytes) -> HttpResponse {
    HttpResponse::Ok()
        .append_header(ContentType(mime::APPLICATION_JSON))
        .body(data)
}

/// Answers with gzipped json as is to clients that accept gzip, and unzips it for ones that don't (older clients).
pub async fn gzip_json_response(req: &HttpRequest, gzipped: Bytes) -> Result<HttpResponse, ProcessError> {
    let mut res = HttpResponse::Ok();
    // the body depends on the header, so caches in between must not hand one client's answer to the other.
    res.insert_header(ContentType(mime::APPLICATION_JSON)).insert_header((VARY, "Accept-Encoding"));
    if accepts_gzip(req) {
        Ok(res.insert_header(ContentEncoding::Gzip).body(gzipped))
    } else {
        Ok(res.body(gunzip(gzipped).await?))
    }
}

/// Whether the client accepts gzip. Naming gzip decides it, otherwise `*` does, and either is turned off with `q=0`.
fn accepts_gzip(req: &HttpRequest) -> bool {
    let (mut gzip, mut any) = (None, None);
    let codings = req.headers().get_all(CLIENT_ACCEPT_ENCODING).filter_map(|value| value.to_str().ok()).flat_map(|value| value.split(','));
    for coding in codings {
        let mut parts = coding.split(';');
        let name = parts.next().unwrap_or_default().trim();
        let refused = parts.any(|param| param.split_once('=').is_some_and(|(key, q)| {
            key.trim().eq_ignore_ascii_case("q") && q.trim().parse::<f32>().is_ok_and(|q| q <= 0.0)
        }));
        if name.eq_ignore_ascii_case("gzip") || name.eq_ignore_ascii_case("x-gzip") {
            gzip = Some(!refused);
        } else if name == "*" {
            any = Some(!refused);
        }
    }
    gzip.or(any).unwrap_or(false)
}
