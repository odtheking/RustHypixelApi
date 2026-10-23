use std::{str::FromStr, sync::LazyLock, time::{Duration, Instant}};

use actix_web::{HttpRequest, Responder, error::ErrorBadRequest, get, web::{Bytes, Data, Path}};
use tokio::{spawn, task::spawn_blocking};
use uuid::Uuid;

use crate::{client::client, cache::{cache_key::{CacheKey, MemoryPool}, cache_router::{CacheRouter, Database, Waiting}, compression::{gzip_blocking, looks_like_gzip}}, env_var, error::ProcessError, logging::{LogMessage, log}, request_utils::{Upstream, gzip_json_response, request}, routes::stats::RateLimit};

/// Database time to live for profile queries in seconds.
pub static PROFILE_DB_TTL_SECONDS: LazyLock<Duration> = LazyLock::new(|| Duration::from_secs(env_var("PROFILE_DB_TTL_SECONDS", 3600)));
/// Cache time to live for profile queries in seconds.
pub static PROFILE_CACHE_TTL_SECONDS: LazyLock<Duration> = LazyLock::new(|| Duration::from_secs(env_var("PROFILE_CACHE_TTL_SECONDS", 120)));

/// Profiles are kept gzipped, in memory and in the database, and only unzipped for clients that don't accept gzip.
struct ProfileKey(Uuid);

impl CacheKey for ProfileKey {
    // was 0 while the database kept profiles as lz4. Those entries are never looked up, so they aren't sent as gzip, and expire on their own.
    const KEYFLAG: u8 = 2;
    const POOL: MemoryPool = MemoryPool::Profiles;

    fn uuid(&self) -> Uuid {
        self.0
    }

    async fn get_or_insert(&self, db: &Database, stats: &RateLimit, waiting: &Waiting<'_>) -> Result<(Bytes, Duration), ProcessError> {
        let uuid_key = self.key();
        let now = Instant::now();
        let bytes = db.read(uuid_key).await;
        log(LogMessage::TimeElapsed { elapsed: now.elapsed(), name: "DB Read" });

        // an entry that can't be read is treated as missing, and is overwritten by the fetch below. Otherwise it would be served,
        // and fail to unzip, for as long as it is kept.
        match bytes {
            Ok(Some(db_data)) if looks_like_gzip(&db_data) => {
                log(LogMessage::MessageAndUser { key: uuid_key, message: "DB Hit" });
                return Ok((db_data, *PROFILE_CACHE_TTL_SECONDS))
            }
            Ok(Some(_)) => log(LogMessage::MessageAndUser { key: uuid_key, message: "DB entry is not gzip, fetching again" }),
            Ok(None) => {}
            Err(err) => log(LogMessage::KeyError { key: uuid_key, message: "DB entry unreadable, fetching again", error: err.to_string() }),
        }

        let upstream = request(uuid_key, format!("https://api.hypixel.net/v2/skyblock/profiles?uuid={}", self.uuid()), stats, waiting, Self::POOL).await?;
        let body = spawn_blocking(move || checked_gzip(upstream)).await.map_err(|_| ProcessError::InternalServer("Profile check task failed!"))??;

        // the profile is kept for later in the background, so the client isn't waiting on the disk write. It is in the
        // memory cache by the time this would have finished, and failing to keep it is no reason to fail the request.
        let (db, data) = (db.clone(), body.clone());
        spawn(async move {
            let now = Instant::now();
            match db.insert(uuid_key, data, *PROFILE_DB_TTL_SECONDS).await {
                Ok(()) => log(LogMessage::TimeElapsed { elapsed: now.elapsed(), name: "DB write" }),
                Err(err) => log(LogMessage::KeyError { key: uuid_key, message: "DB write failed", error: err.to_string() }),
            }
        });

        Ok((body, *PROFILE_CACHE_TTL_SECONDS))
    }
}

/// Checks a profile is whole json before it is kept, and gives it gzipped (gzipping it if hypixel didn't). Hypixel sometimes answers 200
/// with a body cut short, which would otherwise be served for as long as the profile is kept. Megabytes, so run off the request threads.
fn checked_gzip(upstream: Upstream) -> Result<Bytes, ProcessError> {
    simd_json::to_tape(&mut upstream.json()?).map_err(ProcessError::upstream_invalid)?;
    if upstream.gzipped { Ok(upstream.body) } else { gzip_blocking(&upstream.body) }
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
    Ok(gzip_json_response(&req, data).await?)
}
