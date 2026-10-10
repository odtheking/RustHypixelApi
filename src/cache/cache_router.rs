use std::{sync::{Arc, LazyLock}, time::Duration};

use actix_web::web::Bytes;
use ltmdb::{ResultExt, Runtime};
use rapidhash_lite::RandomHash;
use reqwest::StatusCode;
use simple_defer::{Deferred, defer};
use single_flight::Group;
use tinyufo::TinyUfo;
use tokio::{spawn, task::spawn_blocking, time::{Instant, sleep}};

use crate::{cache::{UuidKey, cache_key::{CacheKey, MemoryPool}}, env_var, error::ProcessError, logging::{LogMessage, log}, routes::stats::RateLimit};

// The memory cache is sized by number of entries, not bytes. Profiles are large (potentially hundreds of KB) so that cache is kept small,
// a profile miss only costs a database read. Secrets are tiny but every miss is a hypixel request, so that cache is large.
static PROFILE_CACHE_SIZE: LazyLock<usize> = LazyLock::new(|| env_var("PROFILE_CACHE_SIZE", 512));
static SECRETS_CACHE_SIZE: LazyLock<usize> = LazyLock::new(|| env_var("SECRETS_CACHE_SIZE", 10_000));

pub type Database = ltmdb::Database<TokioRT, RandomHash>;

/// Routes cache requests to the memory cache and db cache.
/// behavior during insertion is handled via the `CacheKey` trait.
pub struct CacheRouter {
    profile_cache: TinyUfo<UuidKey, (Bytes, Instant)>,
    secrets_cache: TinyUfo<UuidKey, (Bytes, Instant)>,
    database: Database,
    group: Group<UuidKey, Bytes, ProcessError, RandomHash>,
}

impl CacheRouter {
    fn new(profile_size: usize, secrets_size: usize, database: Database) -> Self {
        Self {
            profile_cache: TinyUfo::new(profile_size, profile_size),
            secrets_cache: TinyUfo::new(secrets_size, secrets_size),
            database,
            group: Group::with_hasher(RandomHash::default()),
        }
    }

    pub async fn load() -> Result<Self, ProcessError> {
        let now = Instant::now();
        let database = Database::load(".db").await?;
        log(LogMessage::TimeElapsed { elapsed: now.elapsed(), name: "database load" });
        Ok(Self::new(*PROFILE_CACHE_SIZE, *SECRETS_CACHE_SIZE, database))
    }

    fn pool(&self, pool: MemoryPool) -> &TinyUfo<UuidKey, (Bytes, Instant)> {
        match pool {
            MemoryPool::Profiles => &self.profile_cache,
            MemoryPool::Secrets => &self.secrets_cache,
        }
    }

    /// Returns the entry if it is present and has not expired. TinyUfo has no notion of ttl, so we keep the expiry alongside the value.
    fn cached(&self, pool: MemoryPool, key: &UuidKey) -> Option<Bytes> {
        self.pool(pool).get(key).filter(|(_, expires)| *expires > Instant::now()).map(|(data, _)| data)
    }

    /// Stores an entry in the memory cache. This uses `force_put` since `put` runs TinyLFU admission and silently refuses new
    /// entries when the cache is full and the entry it would evict is more popular, which defeats the point of caching the result.
    fn store(&self, pool: MemoryPool, key: UuidKey, data: Bytes, ttl: Duration) {
        self.pool(pool).force_put(key, (data, Instant::now() + ttl), 1);
    }

    /// Attempts to get the cache entry from the cache or fetches an entry into the cache if there is none.
    pub async fn get<K: CacheKey + 'static>(self: Arc<Self>, key: K, rate_limit: Arc<RateLimit>) -> Result<Bytes, ProcessError> {
        let k = key.key();

        // we check the cache outside the singleflight group, since it's much more expensive to start that work 
        // just to check the cache if its already there and doesn't need suppression
        if let Some(entry) = self.cached(K::POOL, &k) {
            return Ok(entry);
        }

        let drop_logs = defer(|| log(LogMessage::MessageAndUser { key: k, message: "Caller dropped while waiting on single flight group (work continues)" }));
        
        // The work runs in its own task: if the client disconnects, actix drops this handler future, which would otherwise
        // cancel an in-flight upstream request (still counted by hypixel, never cached) and make the next caller send it again.
        let task_rate_limit = rate_limit.clone();
        let res = spawn(async move {
            // singleflight coelesces the key.get_or_insert requests so we dont duplicate work on quick duplicate requests
            let router = &self;
            self.group.work(&k, async move {
                // we check again here since it may have been added between the prior call and when the group started the work.
                if let Some(entry) = router.cached(K::POOL, &k) {
                    return Ok(entry);
                }

                let (data, ttl) = key.get_or_insert(&router.database, &task_rate_limit).await?;
                router.store(K::POOL, k, data.clone(), ttl); // store the result in the cache BEFORE the end of duplicate suppression
                Ok(data)
            }).await
        }).await.map_err(|_| ProcessError::InternalServer("Single Flight task failed!"))?;

        drop_logs.cancel();
        
        // followers only learn that the leader failed, not why. If we are rate limited that is almost certainly the reason.
        res.map_err(|err| err.unwrap_or_else(|| if rate_limit.is_blocked() {
            ProcessError::Request(StatusCode::TOO_MANY_REQUESTS)
        } else {
            ProcessError::InternalServer("Single Flight Leader failed!")
        }))
    }
}

pub struct TokioRT;
impl Runtime for TokioRT {
    fn spawn<T>(task: T)
        where
            T: Future + Send + 'static,
            T::Output: Send + 'static 
    {
        spawn(task);
    }

    fn spawn_blocking<T, R>(task: T) -> impl Future<Output = Result<R, ltmdb::Error>>
        where
            T: FnOnce() -> R + Send + 'static,
            R: Send + 'static 
    {
        spawn_blocking(task).task_err()
    }

    fn sleep(duration: std::time::Duration) -> impl Future<Output = ()> {
        sleep(duration)
    }
}