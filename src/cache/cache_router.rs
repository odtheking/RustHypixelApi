use std::{collections::HashMap, sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError}, time::Duration};

use actix_web::web::Bytes;
use ltmdb::{ResultExt, Runtime};
use rapidhash_lite::RandomHash;
use simple_defer::{Deferred, defer};
use single_flight::Group;
use tinyufo::TinyUfo;
use tokio::{spawn, task::spawn_blocking, time::{Instant, sleep}};

use crate::{cache::{UuidKey, cache_key::{CacheKey, MemoryPool}}, client::ClientId, env_var, error::ProcessError, logging::{LogMessage, log}, routes::stats::RateLimit};

/// How long past its expiry an entry can still be served when it can't be refreshed (hypixel is rate limiting us or failing).
const MAX_STALE: Duration = Duration::from_secs(3600);

// The memory cache is sized by number of entries, not bytes. Profiles are large (potentially hundreds of KB) so that cache is kept small,
// a profile miss only costs a database read. Secrets are tiny but every miss is a hypixel request, so that cache is large.
// Profiles are kept gzipped, about half the size of the plain json, so twice as many fit in the memory that used to hold 512.
static PROFILE_CACHE_SIZE: LazyLock<usize> = LazyLock::new(|| env_var("PROFILE_CACHE_SIZE", 1024));
static SECRETS_CACHE_SIZE: LazyLock<usize> = LazyLock::new(|| env_var("SECRETS_CACHE_SIZE", 10_000));

pub type Database = ltmdb::Database<TokioRT, RandomHash>;

/// Routes cache requests to the memory cache and db cache.
/// behavior during insertion is handled via the `CacheKey` trait.
pub struct CacheRouter {
    profile_cache: TinyUfo<UuidKey, (Bytes, Instant)>,
    secrets_cache: TinyUfo<UuidKey, (Bytes, Instant)>,
    database: Database,
    group: Group<UuidKey, Bytes, ProcessError, RandomHash>,
    flights: Mutex<HashMap<UuidKey, Flight, RandomHash>>,
}

/// What the callers waiting on one key (the leader and everyone coalesced into it) have in common.
#[derive(Default)]
struct Flight {
    /// the client of every caller waiting on the key.
    clients: Vec<ClientId>,
    /// why the leader failed. Followers only learn that it did, so they read it from here.
    failure: Option<ProcessError>,
}

/// The clients waiting on the key being fetched. The request to hypixel is paid for by any one of them that is within their rate limit,
/// so a client that is rate limited still gets the result when someone else waiting on the same key isn't.
pub struct Waiting<'a> {
    flights: &'a Mutex<HashMap<UuidKey, Flight, RandomHash>>,
    key: UuidKey,
}

impl Waiting<'_> {
    /// The clients waiting right now, including ones that joined after the fetch started.
    pub fn clients(&self) -> Vec<ClientId> {
        self.flights.lock().unwrap_or_else(PoisonError::into_inner).get(&self.key).map(|flight| flight.clients.clone()).unwrap_or_default()
    }
}

impl CacheRouter {
    fn new(profile_size: usize, secrets_size: usize, database: Database) -> Self {
        Self {
            profile_cache: TinyUfo::new(profile_size, profile_size),
            secrets_cache: TinyUfo::new(secrets_size, secrets_size),
            database,
            group: Group::with_hasher(RandomHash::default()),
            flights: Mutex::new(HashMap::with_hasher(RandomHash::default())),
        }
    }

    fn flights(&self) -> MutexGuard<'_, HashMap<UuidKey, Flight, RandomHash>> {
        self.flights.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn join(&self, key: UuidKey, client: ClientId) {
        self.flights().entry(key).or_default().clients.push(client);
    }

    fn leave(&self, key: UuidKey, client: ClientId) {
        let mut flights = self.flights();
        if let Some(flight) = flights.get_mut(&key) {
            if let Some(position) = flight.clients.iter().position(|waiting| *waiting == client) {
                flight.clients.swap_remove(position);
            }
            if flight.clients.is_empty() {
                flights.remove(&key);
            }
        }
    }

    /// Sets how the leader failed, or clears it when a new attempt starts.
    fn set_failure(&self, key: UuidKey, failure: Option<ProcessError>) {
        if let Some(flight) = self.flights().get_mut(&key) {
            flight.failure = failure;
        }
    }

    fn failure(&self, key: UuidKey) -> Option<ProcessError> {
        self.flights().get(&key).and_then(|flight| flight.failure.clone())
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

    /// Returns the entry even if it has expired (but not for longer than `MAX_STALE`), for when it can't be refreshed.
    fn stale(&self, pool: MemoryPool, key: &UuidKey) -> Option<Bytes> {
        self.pool(pool).get(key).filter(|(_, expires)| Instant::now() < *expires + MAX_STALE).map(|(data, _)| data)
    }

    /// Stores an entry in the memory cache. This uses `force_put` since `put` runs TinyLFU admission and silently refuses new
    /// entries when the cache is full and the entry it would evict is more popular, which defeats the point of caching the result.
    fn store(&self, pool: MemoryPool, key: UuidKey, data: Bytes, ttl: Duration) {
        self.pool(pool).force_put(key, (data, Instant::now() + ttl), 1);
    }

    /// Attempts to get the cache entry from the cache or fetches an entry into the cache if there is none.
    /// `client` is who asked for it, who may pay for the fetch from hypixel if they are within their rate limit.
    pub async fn get<K: CacheKey + 'static>(self: Arc<Self>, key: K, rate_limit: Arc<RateLimit>, client: ClientId) -> Result<Bytes, ProcessError> {
        let k = key.key();

        // we check the cache outside the singleflight group, since it's much more expensive to start that work
        // just to check the cache if its already there and doesn't need suppression
        if let Some(entry) = self.cached(K::POOL, &k) {
            log(LogMessage::ClientMessage { key: k, client, message: "Memory cache hit" });
            return Ok(entry);
        }

        let drop_logs = defer(|| log(LogMessage::ClientMessage { key: k, client, message: "Caller dropped while waiting on single flight group (work continues)" }));

        // The work runs in its own task: if the client disconnects, actix drops this handler future, which would otherwise
        // cancel an in-flight upstream request (still counted by hypixel, never cached) and make the next caller send it again.
        let task_rate_limit = rate_limit.clone();
        let router = self.clone();
        let res = spawn(async move {
            let router = &self;
            // everyone waiting on this key is registered, so the fetch can be paid for by whichever of them is within their rate limit.
            router.join(k, client);
            let _leave = defer(|| router.leave(k, client));

            // singleflight coelesces the key.get_or_insert requests so we dont duplicate work on quick duplicate requests
            let res = router.group.work(&k, async move {
                // we check again here since it may have been added between the prior call and when the group started the work.
                if let Some(entry) = router.cached(K::POOL, &k) {
                    return Ok(entry);
                }

                log(LogMessage::ClientMessage { key: k, client, message: "Memory cache miss, fetching" });
                router.set_failure(k, None);
                let waiting = Waiting { flights: &router.flights, key: k };
                match key.get_or_insert(&router.database, &task_rate_limit, &waiting).await {
                    Ok((data, ttl)) => {
                        router.store(K::POOL, k, data.clone(), ttl); // store the result in the cache BEFORE the end of duplicate suppression
                        Ok(data)
                    }
                    Err(err) => {
                        // rate limits and hypixel not answering are already logged where they happen.
                        if matches!(err, ProcessError::UpstreamInvalid(_) | ProcessError::Serialization(_) | ProcessError::Database(_) | ProcessError::InternalServer(_)) {
                            log(LogMessage::KeyError { key: k, message: "Fetch failed", error: err.to_string() });
                        }
                        router.set_failure(k, Some(err.clone()));
                        Err(err)
                    }
                }
            }).await;

            // followers only learn that the leader failed, so they get why from where the leader left it.
            res.map_err(|err| err.or_else(|| router.failure(k)))
        }).await.map_err(|_| ProcessError::InternalServer("Single Flight task failed!"))?;

        drop_logs.cancel();

        match res {
            Ok(data) => Ok(data),
            Err(err) => {
                let err = err.unwrap_or_else(|| if rate_limit.is_blocked() {
                    ProcessError::HypixelRateLimit
                } else {
                    ProcessError::InternalServer("Single Flight Leader failed!")
                });
                // an old answer is better than an error, and hypixel not answering is no reason to answer nothing.
                // A client over its own limit still hears that, though.
                if !err.is_upstream() {
                    return Err(err);
                }
                router.stale(K::POOL, &k)
                    .inspect(|_| log(LogMessage::ClientMessage { key: k, client, message: "Failed to refresh, served stale entry" }))
                    .ok_or(err)
            }
        }
    }
}

#[derive(Clone)]
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
