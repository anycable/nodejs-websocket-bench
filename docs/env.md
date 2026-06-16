# Environment variables

Reference for the bench-runner server, the driver scripts, and the
`railway-metrics` helper. Defaults are what the script picks if the
variable is unset.

## Driver scripts (local)

| Variable             | Default                            | Used by                       |
| -------------------- | ---------------------------------- | ----------------------------- |
| `BENCH_RUNNER_URL`   | *(required for Railway drivers)*   | every driver that POSTs to a bench-runner |
| `BENCH_RUNNER_TOKEN` | *(empty)*                          | every Railway driver, when the bench-runner enforces auth |
| `SOCKETIO_URL`       | `http://localhost:3000`            | `jitter-socketio*`, `avalanche-railway-socketio` |
| `ANYCABLE_URL`       | `ws://localhost:8080/cable`        | `jitter-anycable`, `avalanche-anycable` |
| `BROADCAST_URL`      | `http://localhost:8090/_broadcast` | `jitter-anycable`, `avalanche-anycable`, `publisher` |
| `BROADCAST_SECRET`   | *(empty)*                          | `jitter-anycable`, `publisher`; bearer token if `anycable-go` is started with `ANYCABLE_HTTP_BROADCAST_SECRET` |
| `NUM_CLIENTS`        | `50` (local) / `1000` (Railway)    | every bench script             |
| `DURATION`           | `150`                              | jitter scripts; test length in seconds |
| `JITTER_INTERVAL`    | `15`                               | jitter scripts; seconds between disconnects |
| `JITTER_DURATION`    | `1000`                             | jitter scripts; ms offline per event |
| `TOTAL_MESSAGES`     | `600`                              | jitter scripts (in-process publisher), standalone publisher |
| `INTERVAL_MS`        | `200`                              | jitter scripts, publisher; ms between messages |
| `STREAM`             | `benchmark` / `avalanche`          | every script; stream name      |
| `RAMP_RATE`          | `50`                               | every bench script; new connections per second |
| `RESULTS_DIR`        | `results` (relative to cwd)        | scripts that write CSV / JSON; override to a path of your choice |

## Servers

| Variable                    | Default                                                | Service                          | Description |
| --------------------------- | ------------------------------------------------------ | -------------------------------- | ----------- |
| `SERVICE_ENTRY`             | `socketio/server`                                      | both                             | Selects compiled entry point at container start |
| `PORT`                      | `3000` / `3001`                                        | `socketio-server` / `bench-runner` | HTTP port |
| `SOCKETIO_CSR`              | *(unset / `0`)*                                        | `socketio-server`                | `1` enables Connection State Recovery |
| `SOCKETIO_CSR_MAX_MS`       | `120000` (2 min)                                       | `socketio-server`                | `maxDisconnectionDuration` for CSR |
| `REDIS_URL`                 | *(unset)*                                              | `socketio-server`                | When set, enables the Socket.io Redis adapter for multi-node fan-out. Mutually exclusive with CSR. |
| `BENCH_RUNNER_TOKEN`        | *(empty)*                                              | `bench-runner`                   | Required bearer token on every `/bench-*` and `/jobs/*` request. `/health` stays open. Empty = no auth (only safe for internal-only deployments). |
| `SOCKETIO_REDIS_URL_A`      | `http://socketio-server-redis-a.railway.internal:3000` | `bench-runner`                   | First multi-node Socket.io target for the Redis-adapter throughput test |
| `SOCKETIO_REDIS_URL_B`      | `http://socketio-server-redis-b.railway.internal:3000` | `bench-runner`                   | Second multi-node Socket.io target for the Redis-adapter throughput test |
| `SOCKETIO_URL`              | `http://socketio-server.railway.internal:3000`         | `bench-runner`                   | Target for `socketio` bench endpoints |
| `ANYCABLE_URL`              | `ws://anycable-go.railway.internal:8080/cable`         | `bench-runner`                   | Target for `anycable` bench endpoints |
| `ANYCABLE_BROADCAST_URL`    | `http://anycable-go.railway.internal:8080/_broadcast`  | `bench-runner`                   | Broadcast endpoint for AnyCable runs |
| `ANYCABLE_BROADCAST_SECRET` | *(empty)*                                              | `bench-runner`                   | Bearer token if `anycable-go` has `ANYCABLE_HTTP_BROADCAST_SECRET` set |
| `UWS_WS_URL`                | `ws://uws-server.railway.internal:3000/ws`             | `bench-runner`                   | Target for `uws` bench endpoints |
| `UWS_HTTP_URL`              | `http://uws-server.railway.internal:3000`              | `bench-runner`                   | HTTP publish endpoint for uWS broadcast tests |

## `railway-metrics`

| Variable        | Description                                                                        |
| --------------- | ---------------------------------------------------------------------------------- |
| `PROJECT_ID`    | Railway project UUID (required)                                                    |
| `SERVICE_ID`    | Railway service UUID (required)                                                    |
| `SERVICE_NAME`  | Display name for the report                                                        |
| `START_DATE`    | ISO8601 start of metrics window (required)                                         |
| `END_DATE`      | ISO8601 end of window (defaults to now)                                            |
| `SAMPLE_RATE`   | `30`; Railway enforces a minimum (~30 s) for short windows                         |
| `RAILWAY_TOKEN` | Optional override; falls back to `~/.railway/config.json`                          |
