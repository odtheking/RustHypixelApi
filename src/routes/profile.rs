use std::{str::FromStr, sync::LazyLock, time::{Duration, Instant}};

use actix_web::{HttpRequest, Responder, error::ErrorBadRequest, get, web::{Bytes, Data, Path}};
use uuid::Uuid;

use crate::{client::client, cache::{cache_key::{CacheKey, MemoryPool}, cache_router::{CacheRouter, Database, Waiting}, compression::{compress, decompress}}, env_var, error::ProcessError, logging::{LogMessage, log}, request_utils::{json_response, request}, routes::stats::RateLimit};

/// Database time to live for profile queries in seconds.
pub static PROFILE_DB_TTL_SECONDS: LazyLock<Duration> = LazyLock::new(|| Duration::from_secs(env_var("PROFILE_DB_TTL_SECONDS", 3600)));
/// Cache time to live for profile queries in seconds.
pub static PROFILE_CACHE_TTL_SECONDS: LazyLock<Duration> = LazyLock::new(|| Duration::from_secs(env_var("PROFILE_CACHE_TTL_SECONDS", 120)));

struct ProfileKey(Uuid);

impl CacheKey for ProfileKey {
    const KEYFLAG: u8 = 0;
    const POOL: MemoryPool = MemoryPool::Profiles;

    fn uuid(&self) -> Uuid {
        self.0
    }
 
    async fn get_or_insert(&self, db: &Database, stats: &RateLimit, waiting: &Waiting<'_>) -> Result<(Bytes, Duration), ProcessError> {
        let uuid_key = self.key();
        let now = Instant::now();
        let bytes = db.read(uuid_key).await?;
        log(LogMessage::TimeElapsed { elapsed: now.elapsed(), name: "DB Read" });
        
        if let Some(db_data) = bytes {
            // an entry that can't be read is treated as missing, and is overwritten by the fetch below.
            match decompress(db_data).await {
                Ok(decompressed) => {
                    log(LogMessage::MessageAndUser { key: uuid_key, message: "DB Hit" });
                    return Ok((decompressed.into(), *PROFILE_CACHE_TTL_SECONDS))
                }
                Err(err) => log(LogMessage::KeyError { key: uuid_key, message: "DB entry unreadable, fetching again", error: err.to_string() }),
            }
        }

        let bytes = request(uuid_key, format!("https://api.hypixel.net/v2/skyblock/profiles?uuid={}", self.uuid()), stats, waiting, Self::POOL).await?;

        // the profile is already fetched, so failing to keep it for later is no reason to fail the request.
        let now = Instant::now();
        let stored = async {
            let compressed = compress(bytes.clone()).await?;
            db.insert(uuid_key, compressed, *PROFILE_DB_TTL_SECONDS).await?;
            Ok::<_, ProcessError>(())
        }.await;
        match stored {
            Ok(()) => log(LogMessage::TimeElapsed { elapsed: now.elapsed(), name: "DB write" }),
            Err(err) => log(LogMessage::KeyError { key: uuid_key, message: "DB write failed", error: err.to_string() }),
        }

        Ok((bytes, *PROFILE_CACHE_TTL_SECONDS))
    }
}

#[get("/get/{uuid}")]
async fn profile(
    req: HttpRequest,
    path: Path<String>,
    cache: Data<CacheRouter>,
    stats: Data<RateLimit>,
) -> actix_web::Result<impl Responder> {
    let uuid = Uuid::from_str(&path.into_inner()).map_err(|_| ErrorBadRequest("Proxy error: invalid uuid"))?;
    let client = client(&req)?;
    let data = cache.into_inner().get(ProfileKey(uuid), stats.into_inner(), client).await?;
    Ok(json_response(data))
}
