use std::{num::NonZeroU32, sync::{LazyLock, atomic::{AtomicU64, Ordering}}, time::{Duration, Instant}};

use actix_web::{HttpResponse, Responder, get, web::Data};
use governor::{DefaultKeyedRateLimiter, Quota, RateLimiter};
use portable_atomic::AtomicU128;
use reqwest::{StatusCode, header::HeaderMap};
use simd_json::json;

use crate::{cache::cache_key::MemoryPool, client::ClientId, error::ProcessError};

/// Used when hypixel rate limits us but doesn't tell us for how long.
const DEFAULT_BLOCK: Duration = Duration::from_secs(10);
/// Upper bound on a single block, so a bogus header can't lock the proxy out for long.
const MAX_BLOCK: Duration = Duration::from_secs(300);

pub struct RateLimit {
    inner: AtomicU128,
    /// `now_millis()` until which hypixel is known to be rate limiting us.
    blocked_until: AtomicU64,
    /// limits all requests from a client, including ones answered from the cache.
    requests: DefaultKeyedRateLimiter<ClientId>,
    /// limits secrets requests from a client that have to be sent to hypixel.
    secrets: DefaultKeyedRateLimiter<ClientId>,
    /// limits profile requests from a client that have to be sent to hypixel.
    profiles: DefaultKeyedRateLimiter<ClientId>,
}

/// `requests` per `period_seconds`, with up to `burst` allowed at once.
///
/// # Panics
/// panics if any of the values are zero.
pub fn quota(requests: u32, period_seconds: u64, burst: u32) -> Quota {
    let requests = NonZeroU32::new(requests).expect("rate limit requests should be above zero!");
    let burst = NonZeroU32::new(burst).expect("rate limit burst should be above zero!");
    Quota::with_period(Duration::from_secs(period_seconds) / requests.get())
        .expect("rate limit period should be above zero!")
        .allow_burst(burst)
}

/// monotonic millis since startup, so wall clock jumps can't stretch or cut a block.
#[allow(clippy::cast_possible_truncation)]
fn now_millis() -> u64 {
    static START: LazyLock<Instant> = LazyLock::new(Instant::now);
    START.elapsed().as_millis() as u64
}

impl RateLimit {
    pub fn new(requests: Quota, secrets: Quota, profiles: Quota) -> Self {
        Self {
            inner: AtomicU128::new(0),
            blocked_until: AtomicU64::new(0),
            requests: RateLimiter::keyed(requests),
            secrets: RateLimiter::keyed(secrets),
            profiles: RateLimiter::keyed(profiles),
        }
    }

    /// # Errors
    /// if the client has made too many requests.
    pub fn check_request(&self, client: ClientId) -> Result<(), ProcessError> {
        self.requests.check_key(&client).map_err(|_| ProcessError::ProxyRateLimit)
    }

    /// Spends a request to hypixel of the first of `clients` that is within their limit. Everyone waiting on the same request gets its
    /// result, so it only takes one of them.
    ///
    /// # Errors
    /// if every one of the clients has made too many requests that had to be sent to hypixel.
    pub fn check_upstream(&self, clients: &[ClientId], pool: MemoryPool) -> Result<(), ProcessError> {
        let limiter = match pool {
            MemoryPool::Profiles => &self.profiles,
            MemoryPool::Secrets => &self.secrets,
        };
        if clients.iter().any(|client| limiter.check_key(client).is_ok()) {
            Ok(())
        } else {
            Err(ProcessError::ProxyRateLimit)
        }
    }

    /// Forgets clients that have their full allowance back, so they don't build up forever.
    /// Giving the freed memory back needs every shard locked, so that is only done when asked to.
    pub fn cleanup(&self, shrink: bool) {
        for limiter in [&self.requests, &self.secrets, &self.profiles] {
            limiter.retain_recent();
            if shrink {
                limiter.shrink_to_fit();
            }
        }
    }

    /// True while hypixel has told us we are rate limited. Requests made in this window are guaranteed 429s,
    /// and uncached errors mean every client retry would otherwise go upstream again.
    pub fn is_blocked(&self) -> bool {
        now_millis() < self.blocked_until.load(Ordering::Relaxed)
    }

    /// Records the headers and status of an upstream response, starting a block if it was a 429 or the quota is spent.
    #[allow(clippy::cast_possible_truncation)]
    pub fn observe(&self, status: StatusCode, headers: &HeaderMap) {
        let stats = stats_from_headers(headers);
        if let Some((remaining, reset)) = stats {
            self.store(remaining, reset, Ordering::Relaxed);
        }
        // a spent quota guarantees the next request is a 429, so block before sending it.
        let exhausted = stats.filter(|&(remaining, reset)| remaining == 0 && reset > 0);
        if status == StatusCode::TOO_MANY_REQUESTS || exhausted.is_some() {
            let retry_after = headers.get("Retry-After").and_then(|v| v.to_str().ok()).and_then(|v| v.parse().ok());
            let wait = retry_after.or(stats.map(|(_, reset)| reset)).map_or(DEFAULT_BLOCK, Duration::from_secs);
            let until = now_millis().saturating_add(wait.clamp(Duration::from_secs(1), MAX_BLOCK).as_millis() as u64);
            self.blocked_until.fetch_max(until, Ordering::Relaxed);
        }
    }

    pub fn store(&self, remaining: u64, reset: u64, order: Ordering) {
        let value = u128::from(remaining) << 64 | u128::from(reset);
        self.inner.store(value, order);
    }

    pub fn load(&self, order: Ordering) -> (u64, u64) {
        let value = self.inner.load(order);
        let remaining = (value >> 64) as u64;
        #[allow(clippy::cast_possible_truncation)]
        let reset = value as u64;
        (remaining, reset)
    }
}

pub fn stats_from_headers(headers: &HeaderMap) -> Option<(u64, u64)> {
    let remaining = headers.get("RateLimit-Remaining")?.to_str().ok()?.parse().ok()?;
    let reset = headers.get("RateLimit-Reset")?.to_str().ok()?.parse().ok()?;
    Some((remaining, reset))
}

#[get("/stats")]
async fn statistics(
    rate_limit: Data<RateLimit>,
) -> actix_web::Result<impl Responder> {
    let (remaining, reset) = rate_limit.load(Ordering::Relaxed);
    let json = json!({
        "RateLimit-Remaining": remaining,
        "RateLimit-Reset": reset
    });
    Ok(HttpResponse::Ok().json(json))
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use super::*;

    fn client(last: u8) -> ClientId {
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, last))
    }

    #[test]
    fn upstream_is_paid_for_by_any_waiting_client_within_their_limit() {
        let limit = RateLimit::new(quota(30, 60, 30), quota(1, 60, 1), quota(1, 60, 1));
        let (a, b) = (client(1), client(2));

        assert!(limit.check_upstream(&[a], MemoryPool::Secrets).is_ok());
        // a has spent its allowance, but b is waiting too, so the request is still sent (and paid for by b).
        assert!(limit.check_upstream(&[a, b], MemoryPool::Secrets).is_ok());
        // everyone waiting is out of allowance.
        assert!(limit.check_upstream(&[a, b], MemoryPool::Secrets).is_err());
        // pools are limited separately.
        assert!(limit.check_upstream(&[a], MemoryPool::Profiles).is_ok());
    }
}
