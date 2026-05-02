# AnyCable vs Socket.io Benchmarks

Reproducible benchmarks behind [AnyCable vs Socket.io](https://anycable.io/compare/socket-io). Three questions:

1. **Delivery under jitter** — how many messages does each server actually deliver when clients experience real-world WiFi drops and cellular handoffs? And when delivery succeeds via replay, **how long does it take**?
2. **Deploy resilience** — what happens to live WebSocket connections when you ship new code?
3. **Connection capacity** — how many idle WebSocket connections can a single instance hold?

Three configurations are tested for question (1):

- **Default Socket.io** — `socket.io` 4.x, no Connection State Recovery, no Redis (single instance, in-memory).
- **Socket.io + CSR** — same Socket.io version with `connectionStateRecovery` opt-in flag, in-memory adapter (the simplest CSR setup).
- **AnyCable** — `anycable-go` 1.6+ with its protocol (`actioncable-v1-ext-json`) and the in-memory broker.

## Headline results

All numbers are from identical Railway infrastructure (same region, same Pro tier — 32 vCPU, 32 GB).

### Delivery under jitter — 10,000 clients, 120 messages at 2/sec

**Disruption profile.** Every client's TCP socket is force-closed for **1 second every ~15 seconds** — no clean close, the kind of failure WiFi drops produce. Over the test, each client experiences ~10 jitter events. With ~1.3 s of blind window per event, the cumulative offline window is ~13% of the publishing run.

|                                  | Default Socket.io | Socket.io + CSR | AnyCable    |
| -------------------------------- | ----------------- | ---------------- | ----------- |
| Clients                          | 10,000            | 10,000           | 10,000      |
| Expected deliveries              | 1,200,000         | 1,200,000        | 1,200,000   |
| Jitter events                    | 98,889            | 103,430          | 83,862      |
| **Deliveries lost**              | **150,642**       | **0**            | **0**       |
| **Delivery rate**                | **87.41%**        | **100%**         | **100%**    |
| CSR session resume rate          | n/a               | 99.5%            | n/a         |
| Connect failures                 | 0                 | 0                | 0           |
| **Replay latency p50** (over min) | 167 ms            | 279 ms           | 246 ms      |
| **Replay latency p95**           | 1.19 s            | 4.92 s           | **0.68 s**  |
| **Replay latency p99**           | 1.66 s            | **8.99 s**       | **1.04 s**  |
| **Replay latency max**           | 2.29 s            | **12.03 s**      | 3.53 s      |
| Server peak memory               | 676 MB (Node)     | 616 MB (Node)    | 1.65 GB (Go) |
| Server peak CPU (of 32 vCPU)     | 0.74% (~0.24 vCPU) | 0.42% (~0.13 vCPU) | 0.98% (~0.31 vCPU) |

**What the numbers mean.**

- **Default Socket.io loses ~13% of messages.** The blind-window ratio matches almost exactly — nothing is delivered during the outage and nothing is recovered after. Per the [Socket.io delivery-guarantees doc](https://socket.io/docs/v4/delivery-guarantees), this is expected: *"if the connection is broken while an event is being sent, then there is no guarantee that the other side has received it."*
- **Socket.io + CSR closes the delivery gap** but with a multi-second replay tail. p99 = 9 seconds, max = 12 seconds. CSR has [documented caveats](https://socket.io/docs/v4/connection-state-recovery): opt-in, "experimental", incompatible with the Redis pub/sub adapter, and state is lost on restart unless you use Redis Streams or MongoDB.
- **AnyCable closes the delivery gap with a sub-second replay tail.** p99 = 1 second, max = 3.5 seconds — about 7× faster than CSR at the tail.

### Reconnection avalanche — 5,000 clients, single deploy

|                                   | Socket.io    | AnyCable |
| --------------------------------- | ------------ | -------- |
| Connections dropped               | 5,000 (100%) | 0        |
| Recovery p50                      | 4,967 ms     | 0 ms     |
| Recovery p95                      | 5,992 ms     | 0 ms     |
| Clients that never reconnected    | 189 (3.8%)   | 0        |
| **Total downtime**                | **~6.8 s**   | **0 s**  |

CSR with the in-memory adapter doesn't help here — server state is lost on restart. CSR with Redis Streams keeps state, but the connections themselves are still all severed; the avalanche is architectural.

### Connection capacity — idle WebSockets to anycable-go

| Idle connections | AnyCable memory | AnyCable CPU (of 32 vCPU) |
| ---------------- | --------------- | ------------------------ |
| 1,000            | 280 MB          | 0%                       |
| 10,000           | 280 MB          | 0%                       |
| 20,000           | 751 MB          | 1.08% (~0.3 vCPU)        |
| **50,000**       | **1.98 GB**     | **1.08% (~0.3 vCPU)**    |

About 40 KB per connection in steady state. We hit a test-client ceiling at ~56K (the Node bench runner couldn't open more outbound TCP connections from one container) — anycable-go itself didn't break a sweat. Server CPU stayed near zero throughout.

## Why the results are what they are

**Delivery — three protocols, three behaviours.**

- *Default Socket.io* is at-most-once. Messages sent during a disconnect are gone — the server doesn't buffer, the client doesn't ask for them back.
- *Socket.io + CSR* (4.6+) appends an opaque per-packet offset to every event. On unexpected disconnect, the server stashes socket state for `maxDisconnectionDuration`. On reconnect, the client sends pid + last-offset; the server replays buffered packets.
- *AnyCable* uses its protocol (`actioncable-v1-ext-json`). Each broadcast carries `(stream, epoch, offset)`. The client tracks its position; on reconnect it issues a `history` command and the server replays the missed range as a batch.

The latency gap between CSR and AnyCable comes from how each replays. CSR drains a per-socket buffer over a single re-established WebSocket; AnyCable's history is per-stream and parallel.

**Deploys.** A Socket.io server *is* your application — WebSocket connections live inside the same Node.js process that handles HTTP. Restart the process (i.e. deploy) and every connection dies. AnyCable is a separate Go binary; your app broadcasts to it over HTTP. Your app restarts; AnyCable stays up; connections don't notice.

**Connection capacity.** Go goroutines multiplex thousands of WebSockets per OS thread with minimal per-connection overhead. Node's single-event-loop model is fundamentally different.

## Repository layout

```
benchmark/
├── docker-compose.yml        # Local Socket.io + anycable-go
├── railway.toml              # Railway deploy config
└── backend/
    ├── Dockerfile            # One image, two entry points (SERVICE_ENTRY env selects)
    ├── package.json
    └── src/
        ├── publisher.ts             # Standalone HTTP publisher (rarely needed now)
        ├── socketio/server.ts       # Socket.io server (/_broadcast + /publish-local)
        ├── bench-runner/server.ts   # Railway-hosted bench-runner — thin HTTP wrapper
        ├── bench/
        │   ├── jitter-socketio.ts          # Local — delivery under jitter, Socket.io
        │   ├── jitter-socketio-csr.ts      # Local — delivery under jitter, Socket.io + CSR
        │   ├── jitter-anycable.ts          # Local — delivery under jitter, AnyCable
        │   ├── avalanche-socketio.ts       # Local — deploy simulation, Socket.io
        │   ├── avalanche-railway-socketio.ts  # Railway-restart avalanche, Socket.io
        │   ├── avalanche-anycable.ts       # Deploy "simulation" (no-op), AnyCable
        │   └── railway-metrics.ts          # Pull memory / CPU from Railway GraphQL
        └── lib/                     # Shared core — single source of truth
            ├── params.ts                   # Param parsing (env or query-string)
            ├── stats.ts                    # ClientStat, percentiles, summarize
            └── jitter-runners.ts           # runJitter{Anycable,Socketio,SocketioCsr}
```

## Two run modes

The local CLI scripts and the Railway-hosted HTTP endpoints both call the same `runJitter*` functions in `src/lib/jitter-runners.ts`, so headline numbers are produced by exactly the same code regardless of where you run them.

**Local (small scale, dev laptop).** Each `src/bench/jitter-*.ts` script reads env vars, calls a runner, prints a human-readable report. Comfortable up to ~1,000 clients on a developer machine; beyond that you'll hit local NAT or event-loop limits.

**Railway-hosted bench-runner (10K+).** `src/bench-runner/server.ts` is an Express app that runs as a separate Railway service in the same project as `socketio-server` and `anycable-go`. It uses Railway's internal network (`*.railway.internal`) to reach the targets — no NAT, no public-internet round-trip, no client-side bottlenecks. This is how the 10K headline numbers above were produced.

## Local quick-start

### Prerequisites

- Node.js 22+
- Either Docker (for the docker-compose quick-start) or a local [anycable-go](https://docs.anycable.io/anycable-go/getting_started) binary (`brew install anycable-go`).

```bash
cd backend
npm install
```

### Run the three jitter variants at small scale

Start the servers in two terminals:

```bash
# Terminal 1 — Socket.io (default, no CSR)
npm run dev:socketio                  # :3000

# OR: Socket.io with Connection State Recovery enabled
npm run dev:socketio-csr              # :3000, SOCKETIO_CSR=1

# Terminal 2 — anycable-go
anycable-go --port 8080 --broker=memory --presets=broker --public
```

Run any of the three benches in a third terminal — each script publishes its own messages, so no separate publisher process is needed:

```bash
# Default Socket.io (publishes via socketio-server's /publish-local — io.to().emit())
SOCKETIO_URL=http://localhost:3000 NUM_CLIENTS=50 DURATION=60 \
  TOTAL_MESSAGES=60 INTERVAL_MS=500 \
  npm run bench:jitter:socketio

# Socket.io + CSR (server must be started with SOCKETIO_CSR=1)
SOCKETIO_URL=http://localhost:3000 NUM_CLIENTS=50 DURATION=60 \
  TOTAL_MESSAGES=60 INTERVAL_MS=500 \
  npm run bench:jitter:socketio-csr

# AnyCable (publishes via anycable-go's /_broadcast over HTTP)
ANYCABLE_URL=ws://localhost:8080/cable BROADCAST_URL=http://localhost:8090/_broadcast \
  NUM_CLIENTS=50 DURATION=60 TOTAL_MESSAGES=60 INTERVAL_MS=500 \
  npm run bench:jitter:anycable
```

Each script prints delivery rate, jitter event count, latency percentiles (raw + min-normalized), and runner-process peak RSS.

### Local avalanche test (Socket.io spawns, kills, restarts)

```bash
npm run build                          # the script runs node dist/socketio/server.js
NUM_CLIENTS=1000 PORT=4000 npm run bench:avalanche:socketio
```

Output includes disconnect spread, recovery p50 / p95 / p99, and total downtime.

For the AnyCable counterpart (which is essentially "confirm nothing happens"):

```bash
anycable-go --port 8080 --broker=memory --presets=broker --public &
NUM_CLIENTS=1000 ANYCABLE_URL=ws://localhost:8080/cable \
  BROADCAST_URL=http://localhost:8090/_broadcast \
  npm run bench:avalanche:anycable
```

## Railway: 10K-client jitter (production-scale)

The 10K results above are produced by deploying the same source as **two Railway services** in one project:

- `socketio-server` — the Socket.io server (also serves `/publish-local` to drive in-process publishing for jitter tests).
- `bench-runner` — the bench runner; uses `*.railway.internal` to reach `socketio-server` and `anycable-go`.

Plus a third existing service:

- `anycable-go` — official Docker image (`anycable/anycable-go:latest`) with `ANYCABLE_BROKER=memory`, `ANYCABLE_PRESETS=broker`, `ANYCABLE_PUBLIC=true`.

### Set up

1. Deploy `socketio-server` from this repo (uses `backend/Dockerfile`). Default `SERVICE_ENTRY` is `socketio/server`.
2. Deploy `anycable-go` from the public image. Set the env above plus `ANYCABLE_HTTP_BROADCAST_SECRET=<your-secret>`.
3. Create a second service from this repo for `bench-runner`. Set:
   - `SERVICE_ENTRY=bench-runner/server`
   - `ANYCABLE_BROADCAST_SECRET=<same secret>`
4. Generate a public domain for `bench-runner` (so you can curl it from your machine).

The bench-runner targets `*.railway.internal` by default. Override with `SOCKETIO_URL`, `ANYCABLE_URL`, `ANYCABLE_BROADCAST_URL` if your service names differ.

### Run

Each endpoint is synchronous: the request blocks until the run completes (typically 3–5 minutes at 10K) and returns the full result as JSON.

```bash
# AnyCable @ 10K — replace bench-runner-production with your domain
curl --max-time 320 -X POST \
  "https://bench-runner-production.up.railway.app/bench-jitter-anycable?n=10000&duration=200&msgs=120&interval=500&jitter=15&jitterMs=1000&ramp=300&stream=run-ac"

# Default Socket.io @ 10K (set SOCKETIO_CSR=0 on socketio-server first)
curl --max-time 320 -X POST \
  "https://bench-runner-production.up.railway.app/bench-jitter-socketio?n=10000&duration=200&msgs=120&interval=500&jitter=15&jitterMs=1000&ramp=300&stream=run-d"

# Socket.io + CSR @ 10K (set SOCKETIO_CSR=1 on socketio-server first; redeploy is automatic)
curl --max-time 320 -X POST \
  "https://bench-runner-production.up.railway.app/bench-jitter-socketio-csr?n=10000&duration=200&msgs=120&interval=500&jitter=15&jitterMs=1000&ramp=300&stream=run-csr"
```

Each response includes:

- `deliveryRatePct`, `lostDeliveries`, `expectedDeliveries`, `receivedDeliveries`
- `jitterEvents`, `csrResumes`, `csrResumeRatePct`, `connectFailures`
- `latencyRawMs` and `latencyOverMinMs` — `{ avg, p50, p95, p99, max }` plus the `skewFloor` (clock skew between bench-runner and the broadcasting host)
- `runnerPeakRssMb` — bench-runner process peak RSS

### Server-side memory and CPU

`bench-runner` doesn't itself measure server resources. Use the `railway-metrics.ts` helper to pull memory and CPU for each service over the test window from Railway's GraphQL API:

```bash
PROJECT_ID=<project-id> SERVICE_ID=<service-id> SERVICE_NAME=anycable-go \
  START_DATE=2026-05-01T08:38:00Z END_DATE=2026-05-01T08:42:04Z SAMPLE_RATE=30 \
  npm run bench:metrics
```

Authentication uses `RAILWAY_TOKEN` if set, otherwise reads `~/.railway/config.json` (the file the `railway` CLI writes after `railway login`).

## Railway: avalanche at scale

For the 5,000-client avalanche numbers, the Railway-hosted Socket.io server is restarted via the Railway CLI while clients are connected. The script runs locally; the disruption it measures is server-side, so client-side capacity isn't the constraint here.

```bash
SOCKETIO_URL=https://your-socketio.up.railway.app NUM_CLIENTS=5000 \
  npm run bench:avalanche:railway
```

In a second terminal, when the script reports "All clients connected":

```bash
railway restart -s socketio-server --yes
```

## Connection-capacity test

The `socketio-server` exposes a small probe endpoint that opens N raw WebSocket connections to anycable-go (via internal network) and holds them. This is the test that produced the 50,000-idle number.

```bash
curl -X POST "https://your-socketio.up.railway.app/idle-anycable?n=10000&hold=30&ramp=300"
# then watch the service logs:
railway logs --service socketio-server | grep idle
```

For higher counts (20K, 50K), increase `n`. We hit a test-client TCP/port ceiling at ~56K — anycable-go's actual limit is higher.

## Environment variables

### Bench scripts (local)

| Variable           | Default                              | Used by                      |
| ------------------ | ------------------------------------ | ---------------------------- |
| `SOCKETIO_URL`     | `http://localhost:3000`              | jitter-socketio*, avalanche-railway-socketio |
| `ANYCABLE_URL`     | `ws://localhost:8080/cable`          | jitter-anycable, avalanche-anycable |
| `BROADCAST_URL`    | `http://localhost:8090/_broadcast`   | publisher, avalanche-anycable |
| `BROADCAST_SECRET` | *(empty)*                            | publisher — bearer token for AnyCable broadcast auth |
| `NUM_CLIENTS`      | `50` / `1000`                        | all bench scripts            |
| `DURATION`         | `150`                                | jitter scripts — test length in seconds |
| `JITTER_INTERVAL`  | `15`                                 | jitter scripts — seconds between disconnects |
| `JITTER_DURATION`  | `1000`                               | jitter scripts — ms offline per event |
| `TOTAL_MESSAGES`   | `600`                                | publisher                    |
| `INTERVAL_MS`      | `200`                                | publisher — ms between messages |
| `STREAM`           | `benchmark` / `avalanche`            | all scripts — stream name    |
| `RAMP_RATE`        | `50`                                 | all bench scripts — new connections per second |

### Servers

| Variable               | Default              | Service          | Description |
| ---------------------- | -------------------- | ---------------- | ----------- |
| `SERVICE_ENTRY`        | `socketio/server`    | both             | Selects compiled entry point at container start |
| `PORT`                 | `3000` / `3001`      | socketio-server / bench-runner | HTTP port |
| `SOCKETIO_CSR`         | *(unset / `0`)*      | socketio-server  | `1` enables Connection State Recovery |
| `SOCKETIO_CSR_MAX_MS`  | `120000` (2 min)     | socketio-server  | `maxDisconnectionDuration` for CSR |
| `SOCKETIO_URL`         | `http://socketio-server.railway.internal:3000` | bench-runner | Target for socketio bench endpoints |
| `ANYCABLE_URL`         | `ws://anycable-go.railway.internal:8080/cable` | bench-runner | Target for anycable bench endpoints |
| `ANYCABLE_BROADCAST_URL` | `http://anycable-go.railway.internal:8080/_broadcast` | bench-runner | Broadcast endpoint for AnyCable runs |
| `ANYCABLE_BROADCAST_SECRET` | *(empty)*       | bench-runner     | Bearer token if anycable-go has `ANYCABLE_HTTP_BROADCAST_SECRET` set |

### railway-metrics

| Variable        | Description |
| --------------- | ----------- |
| `PROJECT_ID`    | Railway project UUID (required) |
| `SERVICE_ID`    | Railway service UUID (required) |
| `SERVICE_NAME`  | Display name for the report |
| `START_DATE`    | ISO8601 — start of metrics window (required) |
| `END_DATE`      | ISO8601 — end of window (defaults to now) |
| `SAMPLE_RATE`   | `30` — Railway enforces a minimum (~30s) for short windows |
| `RAILWAY_TOKEN` | Optional override; falls back to `~/.railway/config.json` |

## Notes and caveats

- **CSR adapter choice.** We benchmarked CSR with the default in-memory adapter. With Redis Streams or MongoDB the latency tail might shift; the docs note CSR is incompatible with Redis pub/sub specifically.
- **Like-for-like transports.** Both Socket.io and AnyCable run with WebSocket-only — no long-polling fallback for Socket.io.
- **AnyCable broker.** Benchmarks use the in-memory broker; production deployments typically use NATS or Redis to survive restarts and run multi-node.
- **Latency clock skew.** Publisher and clients run in different processes, possibly different containers. We report both raw and min-normalized latency so cross-variant comparisons are unaffected by skew.
- **Connection capacity ceiling.** The 50K result is anycable-go's *current* idle-connection demonstration — anycable-go itself wasn't saturated; we hit a TCP outbound port ceiling on the Node test client. Real ceiling on this Pro tier is higher.

## About

Built by the [AnyCable](https://anycable.io) team alongside the comparison page at https://anycable.io/compare/socket-io. Reproducible benchmarks let any reader verify the claims; we keep the numbers honest by being able to re-run them.

If you find a methodological flaw, open an issue or a PR — we'd rather fix it than leave a wrong number standing.

## License

MIT — see [LICENSE](./LICENSE).
