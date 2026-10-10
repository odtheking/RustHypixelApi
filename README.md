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
| `RATELIMIT_REFRESH` | `3` | Seconds it takes a client (by real ip) to regain one request of its rate limit allowance. |
| `RATELIMIT_BURST` | `10` | Number of requests a client can make at once before being rate limited. |
| `PROFILE_CACHE_TTL_SECONDS` | `120` | How long a profile (`/get/<uuid>`) stays in the memory cache. |
| `PROFILE_DB_TTL_SECONDS` | `3600` | How long a profile stays in the on-disk database. A profile found there is served without asking hypixel. |
| `SECRETS_TTL_SECONDS` | `120` | How long secrets (`/secrets/<uuid>`) stay in the memory cache. Secrets are not stored in the database. |
| `PROFILE_CACHE_SIZE` | `512` | Max number of profiles in the memory cache. Counted in entries, not bytes, and a profile can be hundreds of KB, so this is the one that costs memory. A miss only costs a database read. |
| `SECRETS_CACHE_SIZE` | `10000` | Max number of secrets in the memory cache. Entries are tiny, and a miss costs a hypixel request, so keep this large. |

The memory caches are separate so profiles can never push secrets out.

### Hypixel rate limiting

When hypixel responds with a `429`, the proxy remembers it (using the `Retry-After` / `RateLimit-Reset` headers, or 10 seconds if there are none, capped at 5 minutes) and answers `429` immediately without contacting hypixel until that time has passed. The same happens when a response reports `RateLimit-Remaining: 0`. Errors are never cached, so without this every client retry would be sent to hypixel again. The latest values are visible at `/stats`.
