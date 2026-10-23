# Hypixel Api Proxy

Rust web server to proxy the hypixel api with caching and rate limiting.

Currently only has 2 paths, full skyblock profile via `/get/<uuid>` and accross profile secrets via achievement data at `/secrets/<uuid>`

Api key should be an environment variable. Per session can be set with `export API_KEY="<apikeyhere>"`
There are various other environment variables to control caching which will be seen when they defualt and log how they are using defaults, but they arent important and should be fine left as is.

While initially made for [HateCheaters](https://github.com/SubAt0m1c/HateCheaters), and now [Odin](https://github.com/odtheking/Odin), it should work just fine for any other projects as long as the expected paths are the same.

By default this is expected to be run through a reverse proxy with port 8000. You will need to pass client ip through.
The installation script will automatically handle installing nginx and setting this, however.

Installation and usage: (These instructions only apply to a fresh server. The script may not work as intended otherwise)

1. git clone this repo
2. move to the repo using `cd RustHypixelApi`
3. source the script by running`source install.sh` and follow its instructions
4. build the repo with `cargo build --release`
5. set api key as an environment variable with key `API_KEY`
6. run using pm2 by running `pm2 start "cargo run --release"`
7. access via set url

To update the server:

1. move to the clone using `cd RustHypixelApi`
2. run `git pull`
3. rebuild the repo using `cargo build --release`
4. run `pm2 restart 0` to restart the server

## Configuration

Everything is configured with environment variables. Only `API_KEY` is required, anything else falls back to its default (and logs that it did on startup). A value that can't be parsed will panic on startup.

| Variable | Default | Description |
| --- | --- | --- |
| `API_KEY` | none, **required** | Hypixel api key used for upstream requests. |
| `IP_ADDR` | `127.0.0.1` | Address to bind to (port is always `8000`). Use `0.0.0.0` when running in docker. |
| `RATELIMIT_REQUESTS` | `30` | How many requests a client (by real ip) can make per `RATELIMIT_PERIOD_SECONDS`, counting every request including ones answered from the cache or database. |
| `RATELIMIT_PERIOD_SECONDS` | `60` | The period `RATELIMIT_REQUESTS` is measured over. |
| `RATELIMIT_BURST` | `30` | How many requests a client can make at once before being limited. |
| `SECRETS_RATELIMIT_REQUESTS` | `4` | How many secrets requests per `SECRETS_RATELIMIT_PERIOD_SECONDS` a client can make that have to be sent to hypixel (not in the cache). |
| `SECRETS_RATELIMIT_PERIOD_SECONDS` | `120` | The period `SECRETS_RATELIMIT_REQUESTS` is measured over. |
| `SECRETS_RATELIMIT_BURST` | `4` | How many of those requests a client can make at once before being limited. |
| `PROFILE_RATELIMIT_REQUESTS` | `5` | How many profile requests per `PROFILE_RATELIMIT_PERIOD_SECONDS` a client can make that have to be sent to hypixel (not in the cache or database). |
| `PROFILE_RATELIMIT_PERIOD_SECONDS` | `60` | The period `PROFILE_RATELIMIT_REQUESTS` is measured over. |
| `PROFILE_RATELIMIT_BURST` | `5` | How many of those requests a client can make at once before being limited. |
| `UPSTREAM_TIMEOUT_SECONDS` | `10` | How long to wait for hypixel to start answering a request before giving up with a `504`. |
| `UPSTREAM_BODY_TIMEOUT_SECONDS` | `45` | How long to wait for the rest of hypixel's answer once it has started. Longer than the above, as a body that is lost is never cached and the next request has to go upstream again. |
| `PROFILE_CACHE_TTL_SECONDS` | `120` | How long a profile (`/get/<uuid>`) stays in the memory cache. |
| `PROFILE_DB_TTL_SECONDS` | `3600` | How long a profile stays in the on-disk database. A profile found there is served without asking hypixel. |
| `SECRETS_TTL_SECONDS` | `120` | How long secrets (`/secrets/<uuid>`) stay in the memory cache. Secrets are not stored in the database. |
| `PROFILE_CACHE_SIZE` | `512` | Max number of profiles in the memory cache. Counted in entries, not bytes, and a profile can be hundreds of KB, so this is the one that costs memory. A miss only costs a database read. |
| `SECRETS_CACHE_SIZE` | `10000` | Max number of secrets in the memory cache. Entries are tiny, and a miss costs a hypixel request, so keep this large. |

The memory caches are separate so profiles can never push secrets out.

### Rate limiting

Every client has three separate rate limits. Every request counts towards the `RATELIMIT_*` limit, including ones answered from the cache. Requests that have to be sent to hypixel also count towards a smaller limit, `SECRETS_RATELIMIT_*` for secrets or `PROFILE_RATELIMIT_*` for profiles. Each is `REQUESTS` per `PERIOD_SECONDS` with up to `BURST` at once, for example 5 per 60 seconds with a burst of 5 allows 5 immediately, then one every 12 seconds. All values must be above zero.

The `RATELIMIT_*` limit only protects this server from abuse and does not depend on the hypixel key. The default allows 30 requests per minute per client.

When a request is already being fetched, everyone asking for the same thing waits on it and gets its result. The request to hypixel is paid for by any one of the clients waiting on it that is within their limit, so a client over its limit still gets the result when someone else waiting is not. Every one of them still counts towards their own `RATELIMIT_*` limit, only the hypixel limit is shared. When hypixel is rate limiting us or fails, an expired cached entry (up to an hour old) is served instead of an error if there is one.

Clients are identified by the `X-Real-IP` header set by the reverse proxy, falling back to `X-Forwarded-For`, and these are only trusted when the connection comes from a loopback or private address (your proxy), otherwise the connection address is used. The fallback can be set by the client, so make sure your proxy sets `X-Real-IP` (the nginx config from the install script does). ipv6 clients are limited by their /64.

### Errors

Errors are plain text, short enough for a client to show to the user as is. The message says who the error is from, `Hypixel error` or `Proxy error`, and then what it is:

| Status | Message |
| --- | --- |
| `429` | `Hypixel error: rate limited, try again in a moment` |
| `429` | `Proxy error: too many requests, slow down` |
| `502` | `Hypixel error: responded with 403 Forbidden` (whatever status hypixel answered with) |
| `502` | `Hypixel error: could not be reached` |
| `504` | `Hypixel error: did not respond in time` |
| `400` | `Proxy error: invalid uuid` |
| `500` | `Proxy error: ...` |

