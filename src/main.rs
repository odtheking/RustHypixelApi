use std::{env, fmt::{Debug, Display}, str::FromStr, sync::{LazyLock, OnceLock}, time::Duration};

use actix_web::{App, HttpServer, middleware::from_fn, web::Data};
use mimalloc::MiMalloc;
use tokio::{spawn, time::sleep};

use crate::{cache::cache_router::CacheRouter, client::limit_requests, request_utils::{CLIENT, UPSTREAM_BODY_TIMEOUT, UPSTREAM_TIMEOUT}, routes::{profile::{PROFILE_CACHE_TTL_SECONDS, PROFILE_DB_TTL_SECONDS, profile}, secrets::{SECRETS_TTL_SECONDS, secrets}, stats::{RateLimit, quota, statistics}}};

mod cache;
mod client;
mod routes;
mod request_utils;
mod logging;
mod error;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

pub static API_KEY: OnceLock<String> = OnceLock::new();

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    logging::init();

    let api_key = std::env::var("API_KEY").expect("no api key env variable found");
    API_KEY.set(api_key).expect("API_KEY should be available to set!");
    let ip_addr: String = std::env::var("IP_ADDR").unwrap_or("127.0.0.1".to_string());
    // configuration is read lazily, so read it all now for a value that can't be parsed to fail on startup, not on a request.
    LazyLock::force(&PROFILE_DB_TTL_SECONDS);
    LazyLock::force(&PROFILE_CACHE_TTL_SECONDS);
    LazyLock::force(&SECRETS_TTL_SECONDS);
    LazyLock::force(&UPSTREAM_TIMEOUT);
    LazyLock::force(&UPSTREAM_BODY_TIMEOUT);
    LazyLock::force(&CLIENT);
    println!("Listening on {ip_addr}:8000!");

    let stats = Data::new(RateLimit::new(
        quota(env_var("RATELIMIT_REQUESTS", 30), env_var("RATELIMIT_PERIOD_SECONDS", 60), env_var("RATELIMIT_BURST", 30)),
        quota(env_var("SECRETS_RATELIMIT_REQUESTS", 4), env_var("SECRETS_RATELIMIT_PERIOD_SECONDS", 120), env_var("SECRETS_RATELIMIT_BURST", 4)),
        quota(env_var("PROFILE_RATELIMIT_REQUESTS", 5), env_var("PROFILE_RATELIMIT_PERIOD_SECONDS", 60), env_var("PROFILE_RATELIMIT_BURST", 5)),
    ));
    spawn(cleanup_rate_limits(stats.clone()));
    let cache = Data::new(CacheRouter::load().await.unwrap());

    HttpServer::new(move || {
        App::new()
            .app_data(stats.clone())
            .app_data(cache.clone())
            .wrap(from_fn(limit_requests))
            .service(secrets)
            .service(profile)
            .service(statistics)
    })
    .bind((ip_addr, 8000))?
    .run()
    .await
}

/// Clients that stopped making requests are no longer tracked by the rate limits, so they don't build up forever.
async fn cleanup_rate_limits(rate_limit: Data<RateLimit>) {
    for tick in 1_u64.. {
        sleep(Duration::from_secs(60)).await;
        rate_limit.cleanup(tick % 10 == 0);
    }
}

/// # Panics
/// panics if the environment variable is not parsable as `T`.
pub fn env_var<T>(key: &'static str, default: T) -> T
where
    T: FromStr + Display,
    T::Err: Debug
{
    match env::var(key) {
        Ok(str) => str.parse::<T>().unwrap_or_else(|e| panic!("{} should be a {}!: {e:?}", key, std::any::type_name::<T>())),
        Err(e) => {
            eprintln!("{e}: {key}, using {default} default.");
            default
        }
    }
}