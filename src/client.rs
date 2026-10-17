use std::net::IpAddr;

use actix_web::{HttpMessage, HttpRequest, body::MessageBody, dev::{ServiceRequest, ServiceResponse}, error::ErrorInternalServerError, middleware::Next, web::Data};

use std::time::Instant;

use crate::{logging::{LogMessage, log}, routes::stats::RateLimit};

/// Who a request is rate limited as, which is their ip.
pub type ClientId = IpAddr;

/// Whether `ip` is where our reverse proxy connects from, so the headers it sets can be trusted.
fn is_trusted_proxy(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => ip.is_loopback() || ip.is_private(),
        IpAddr::V6(ip) => ip.is_loopback() || ip.is_unique_local(),
    }
}

/// The address of the real client, used to rate limit them.
///
/// Prefers `X-Real-IP`, which nginx overwrites with the address it saw. `X-Forwarded-For` is the fallback, but nginx appends to
/// what the client sent so it can be set by the client. Both are only trusted when the connection comes from a proxy, so they can't be
/// spoofed by connecting directly. ipv6 addresses are limited by their /64, as that is what a single client controls.
fn client_ip(req: &HttpRequest) -> Option<IpAddr> {
    let peer = req.peer_addr()?.ip().to_canonical();
    let real = req.headers().get("X-Real-IP")
        .and_then(|ip| ip.to_str().ok()?.parse::<IpAddr>().ok())
        .or_else(|| req.connection_info().realip_remote_addr()?.parse().ok());

    Some(match real.filter(|_| is_trusted_proxy(peer)).unwrap_or(peer).to_canonical() {
        IpAddr::V6(ip) => IpAddr::V6((u128::from(ip) & (!0 << 64)).into()),
        ip => ip,
    })
}

/// The client a request is on behalf of, as worked out by `limit_requests`.
///
/// # Errors
/// if called for a request that didn't go through `limit_requests`.
pub fn client(req: &HttpRequest) -> actix_web::Result<ClientId> {
    req.extensions().get::<ClientId>().copied().ok_or_else(|| ErrorInternalServerError("Proxy error: no client"))
}

/// Works out who the client is, and limits how many requests they can make in total, including ones answered from the cache.
pub async fn limit_requests(
    req: ServiceRequest,
    next: Next<impl MessageBody>,
) -> Result<ServiceResponse<impl MessageBody>, actix_web::Error> {
    let ip = client_ip(req.request()).ok_or_else(|| ErrorInternalServerError("Proxy error: no remote address"))?;
    let rate_limit = req.app_data::<Data<RateLimit>>().expect("RateLimit should be app data!").clone();
    let (method, path, now) = (req.method().to_string(), req.path().to_owned(), Instant::now());

    // inserted first so a request that is rate limited is still known to be theirs.
    req.extensions_mut().insert(ip);
    let res = match rate_limit.check_request(ip) {
        Ok(()) => next.call(req).await,
        Err(err) => Err(err.into()),
    };

    // not in its own middleware: holding a clone of the request there across routing makes actix panic.
    let status = match &res {
        Ok(res) => res.status(),
        Err(err) => err.as_response_error().status_code(),
    };
    log(LogMessage::Request { client: ip, method, path, status: status.as_u16(), elapsed: now.elapsed() });
    res
}
