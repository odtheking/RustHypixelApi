use std::{sync::{LazyLock, atomic::{AtomicU64, Ordering}}, time::{Duration, Instant}};

use actix_web::{HttpResponse, Responder, get, web::Data};
use portable_atomic::AtomicU128;
use reqwest::{StatusCode, header::HeaderMap};
use simd_json::json;

/// Used when hypixel rate limits us but doesn't tell us for how long.
const DEFAULT_BLOCK: Duration = Duration::from_secs(10);
/// Upper bound on a single block, so a bogus header can't lock the proxy out for long.
const MAX_BLOCK: Duration = Duration::from_secs(300);

pub struct RateLimit {
    inner: AtomicU128,
    /// `now_millis()` until which hypixel is known to be rate limiting us.
    blocked_until: AtomicU64,
}

/// monotonic millis since startup, so wall clock jumps can't stretch or cut a block.
#[allow(clippy::cast_possible_truncation)]
fn now_millis() -> u64 {
    static START: LazyLock<Instant> = LazyLock::new(Instant::now);
    START.elapsed().as_millis() as u64
}

impl RateLimit {
    pub fn new() -> Self {
        Self {
            inner: AtomicU128::new(0),
            blocked_until: AtomicU64::new(0),
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
