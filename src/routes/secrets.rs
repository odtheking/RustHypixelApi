use std::{str::FromStr, sync::LazyLock, time::Duration};

use actix_web::{HttpRequest, Responder, error::ErrorBadRequest, get, web::{Bytes, Data, Path}};
use serde_json::to_vec;
use tokio::task::spawn_blocking;
use simd_json::{BorrowedValue, derived::ValueObjectAccess, to_borrowed_value};
use uuid::Uuid;

use crate::{client::client, cache::{cache_key::{CacheKey, MemoryPool}, cache_router::{CacheRouter, Database, Waiting}}, env_var, error::ProcessError, request_utils::{Upstream, json_response, request}, routes::stats::RateLimit};

/// Cache time to live for secret queries in seconds. Secret queries do not query the database.
pub static SECRETS_TTL_SECONDS: LazyLock<Duration> = LazyLock::new(|| Duration::from_secs(env_var("SECRETS_TTL_SECONDS", 120)));

struct SecretsKey(Uuid);

impl CacheKey for SecretsKey {
    const KEYFLAG: u8 = 1;
    const POOL: MemoryPool = MemoryPool::Secrets;

    fn uuid(&self) -> Uuid {
        self.0
    }
    
    async fn get_or_insert(&self, _: &Database, rate_limit: &RateLimit, waiting: &Waiting<'_>) -> Result<(Bytes, Duration), ProcessError> {
        let upstream = request(self.key(), format!("https://api.hypixel.net/v2/player?uuid={}", self.uuid()), rate_limit, waiting, Self::POOL).await?;

        // unzipping and parsing the whole player is done off the thread that is answering requests. Only the count is kept, so
        // unlike profiles, secrets are kept and answered plain.
        let count = spawn_blocking(move || count_secrets(&upstream)).await.map_err(|_| ProcessError::InternalServer("Secrets task failed!"))??;
        Ok((count.into(), *SECRETS_TTL_SECONDS))
    }
}

/// The secrets count in a player from hypixel, as json. A player that can't be read is hypixel's fault, as it sometimes answers 200 with
/// a body cut short, so an old count can still be served instead.
fn count_secrets(upstream: &Upstream) -> Result<Vec<u8>, ProcessError> {
    let mut bytes = upstream.json()?;
    let json = to_borrowed_value(&mut bytes).map_err(ProcessError::upstream_invalid)?;
    // players that have never found a secret don't have the achievement at all. That is still a result worth caching, otherwise every request for them goes upstream.
    Ok(match find_secrets(&json) {
        Some(count) => to_vec(count)?,
        None => b"0".to_vec(),
    })
}

#[get("/secrets/{uuid}")]
async fn secrets(
    req: HttpRequest,
    path: Path<String>,
    cache: Data<CacheRouter>,
    rate_limit: Data<RateLimit>,
) -> actix_web::Result<impl Responder> {
    let uuid = Uuid::from_str(&path.into_inner()).map_err(|_| ErrorBadRequest("Proxy error: invalid uuid"))?;
    let client = client(&req)?;
    let data = cache.into_inner().get(SecretsKey(uuid), rate_limit.into_inner(), client).await?;

    Ok(json_response(data))
}

/// Extracts the secret field from hypixel's achievement data. The data in the profile fields is per-profile and takes longer to update.
fn find_secrets<'a>(data: &'a BorrowedValue<'a>) -> Option<&'a BorrowedValue<'a>> {
    data.get("player")
        .and_then(|player| player.get("achievements"))
        .and_then(|achievements| achievements.get("skyblock_treasure_hunter"))
}
