# AnyCable vs Socket.io Benchmarks

Reproducible benchmarks behind [AnyCable vs Socket.io](https://anycable.io/compare/nodejs-websocket). Three questions:

1. **Delivery under jitter.** How many messages does each server actually deliver when clients experience real-world WiFi drops and cellular handoffs? And when delivery succeeds via replay, how long does it take?
2. **Deploy resilience.** What happens to live WebSocket connections when you ship new code?
3. **Connection capacity.** How many idle WebSocket connections can a single instance hold?

Three configurations are tested for question (1):

- **Default Socket.io.** `socket.io` 4.x, no Connection State Recovery, no Redis (single instance, in-memory).
- **Socket.io + CSR.** Same Socket.io version with `connectionStateRecovery` opt-in flag, in-memory adapter (the simplest CSR setup).
- **AnyCable.** `anycable-go` 1.6+ with its protocol (`actioncable-v1-ext-json`) and the in-memory broker.

For the methodology in narrative form (architectural choices, measurement traps, what we got wrong on first attempts, how we keep the numbers honest), see [`docs/methodology.md`](./docs/methodology.md).

## Headline results

All numbers are from identical Railway infrastructure (same region, same Pro tier, 32 vCPU / 32 GB).

### Delivery under jitter — 10,000 clients, 120 messages at 2/sec

**Disruption profile.** Every client's TCP socket is force-closed every ~15 seconds; no clean close, the kind of failure WiFi drops produce. Each client is then offline for ~2 seconds before its first reconnect attempt completes (set by `MIN_OFFLINE_MS` for default Socket.io, by the client library's reconnect backoff for CSR / AnyCable / uWS). Over the 160 s test, each client experiences ~8 jitter events.

|                                  | Default Socket.io | Socket.io + CSR | uWS         | AnyCable OSS | AnyCable Pro |
| -------------------------------- | ----------------- | ---------------- | ----------- | ------------ | ------------ |
| Clients                          | 10,000            | 10,000           | 10,000      | 10,000       | 10,000       |
| Expected deliveries              | 1,200,000         | 1,200,000        | 1,200,000   | 1,200,000    | 1,200,000    |
| Jitter events                    | 74,496            | 81,532           | 81,232      | 83,375       | 83,283       |
| **Deliveries lost**              | **184,449**       | **0**            | **153,805** | **0**        | **0**        |
| **Delivery rate**                | **84.55%**        | **100%**         | **87.03%**  | **100%**     | **100%**     |
| CSR session resume rate          | n/a               | 99.7%            | n/a         | n/a          | n/a          |
| Connect failures                 | 0                 | 0                | 0           | 0            | 0            |
| **Replay latency p50** (raw)     | 106 ms            | 148 ms           | 92 ms       | 250 ms       | 261 ms       |
| **Replay latency p95**           | 394 ms            | 1.97 s           | 0.72 s      | 4.10 s       | 4.10 s       |
| **Replay latency p99**           | 1.07 s            | 4.58 s           | 1.72 s      | 6.14 s       | 6.15 s       |
| **Replay latency max**           | 1.75 s            | 9.71 s           | 2.95 s      | 9.23 s       | 9.36 s       |

**What the numbers mean.**

- **At-most-once protocols lose ~13–16% of messages.** Default Socket.io and uWS both drop the broadcasts that landed during each client's 2 s offline window; with no replay, those messages are gone. Per the [Socket.io delivery-guarantees doc](https://socket.io/docs/v4/delivery-guarantees), this is expected: *"if the connection is broken while an event is being sent, then there is no guarantee that the other side has received it."* The loss rate is independent of the WS implementation: uWS's faster wire doesn't restore lost messages.
- **Socket.io + CSR delivers 100%** with a replay tail of ~4.6 s p99, ~10 s max. CSR resumes ~99.7% of disconnects cleanly via its pid + offset protocol; the small fraction that fall back to live-from-now still get their messages on the next cycle within `maxDisconnectionDuration` (default 2 min). CSR has [documented caveats](https://socket.io/docs/v4/connection-state-recovery): opt-in, marked "experimental", incompatible with the Redis pub/sub adapter, and state is lost on restart unless you pair it with Redis Streams or MongoDB.
- **AnyCable delivers 100%** with a replay tail of ~6.1 s p99, ~9.3 s max. The per-stream history protocol (epoch + offset) is heavier than CSR's per-socket buffer; in the in-memory comparison here CSR's tail is slightly shorter at p99. AnyCable's edge is elsewhere: it survives app deploys, it scales horizontally via NATS or Redis without losing the replay guarantee, it supports native client-to-client whispers, and it slots into Ruby + Rails as easily as Node + Bun + Deno.

### Reconnection avalanche — 5,000 clients, single deploy

|                                   | Socket.io    | AnyCable |
| --------------------------------- | ------------ | -------- |
| Connections dropped               | 5,000 (100%) | 0        |
| Recovery p50                      | 4,967 ms     | 0 ms     |
| Recovery p95                      | 5,992 ms     | 0 ms     |
| Clients that never reconnected    | 189 (3.8%)   | 0        |
| **Total downtime**                | **~6.8 s**   | **0 s**  |

CSR with the in-memory adapter doesn't help here, server state is lost on restart. CSR with Redis Streams keeps state, but the connections themselves are still all severed; the avalanche is architectural.

### Connection capacity — idle WebSockets, single-instance, identical Railway hardware

![Railway services for the benchmark — anycable-go (OSS), anycable-go-pro, socketio-server, publisher, plus 50 sharded bench-runners](docs/railway-services.png)
*The bench-runners are deployed across 50 separate Railway containers so that each shard has its own source IP and ~64K outbound-port pool. The 1M-connection idle test fans out to all 50 in parallel; the per-IP port limit is what makes single-machine 1M tests hard, and this is how we work around it without kernel tuning.*

All three setups ran on the same Railway Pro tier (32 vCPU / 32 GB RAM allocated), same broker config where applicable, one stream subscription per connection. The headline test was a 1,000,000-connection idle target across 25 test-client shards × 40,000 each (per-IP outbound-port pool caps any single shard at ~50K, so the load is distributed).

| Server                    | Connections held         | Peak memory                | Peak CPU (of 32 vCPU)    | What was the limit              |
| ------------------------- | ------------------------ | -------------------------- | ------------------------ | ------------------------------- |
| Socket.io 4.x (Node 22)   | 119,826                  | 6.3 GB                     | 1.34% (1 core saturated) | single Node event loop          |
| `anycable-go` (open source) | 993,994                | **32.00 GB** (ceiling)     | 12.22% (~3.9 vCPU)       | 32 GB RAM ceiling of the box    |
| `anycable-go-pro` v1.6.13 | **999,954**              | 19.34 GB                   | 9.37% (~3.0 vCPU)        | nothing, 13 GB memory headroom  |

About **33 KB per connection for OSS, 19 KB for Pro** at 1M scale. AnyCable Pro is roughly 1.7× more memory-efficient than the open-source build at this load (the gap holds across scales: at 200K it's 3.5 GB vs 8.3 GB, ~2.4×). The Pro binary is bundled separately and licensed; the OSS comparison is the apples-to-apples-with-Socket.io baseline.

Reading the table: Socket.io's wall is its architecture (handshakes serialize through one event loop, regardless of memory). OSS's wall is the physical RAM of the box. Pro has both more headroom on memory and the same Go-runtime concurrency advantage as OSS over Node. Reaching 1M with Socket.io requires running many Node processes behind a Redis adapter (typical guidance: 10K–30K per process).

## Methodology choices worth flagging

Three calls we made that materially shape the numbers above. The full reasoning is in [`docs/methodology.md`](./docs/methodology.md); we surface them here so a casual reader doesn't have to chase footnotes.

- **Default Socket.io's offline window is floored to ~2 s.** Without that floor (`MIN_OFFLINE_MS` in `lib/timing.ts`), the manual fresh-socket reconnect path would only be offline for ~1 s per event and the measured loss would understate what a typical `socket.io-client` user with default `reconnection: true` settings would actually experience. CSR / AnyCable / uWS naturally land near the same window because their client library's backoff dominates. All four configurations are now measured against the same disruption shape.
- **CSR was benchmarked with the in-memory adapter.** It's the simplest opt-in path. A production CSR setup would typically use Redis Streams or MongoDB, both of which add network RTT to the replay tail. The structural picture (CSR closes the delivery gap with a multi-second tail) stays the same. CSR is documented as incompatible with the Redis pub/sub adapter specifically, so the "Redis adapter" you'd reach for first is not the one CSR can use.
- **AnyCable's RAM cost on the jitter row is real.** AnyCable's replay buffer is per-stream (not per-socket like CSR), which is what lets `history` parallelise across streams; it also costs more RAM during a jittery run. Page-level RAM-per-connection numbers come from the idle test, where AnyCable shines; the jitter row is the tradeoff side of the same architectural choice.

## Repository layout

```
benchmark/
├── docker-compose.yml         # Local Socket.io + anycable-go
├── railway.toml               # Railway deploy config
├── docs/
│   ├── methodology.md         # Narrative methodology + measurement traps
│   ├── railway-ops.md         # Railway resize / redeploy / fleet recipes
│   └── env.md                 # Full env-var reference
└── backend/
    ├── Dockerfile             # One image, three entry points (SERVICE_ENTRY env selects)
    ├── package.json
    ├── results/               # CSV/JSON outputs land here (gitignored)
    └── src/
        ├── publisher.ts                # Standalone HTTP publisher (legacy; most tests publish inline)
        ├── socketio/server.ts          # Socket.io server (/_broadcast + /publish-local)
        ├── uws/server.ts               # uWebSockets.js comparison server
        ├── standalone-publisher/server.ts  # Publisher as its own Railway service (deploy-impact tests)
        ├── bench-runner/server.ts      # Railway-hosted bench-runner; bearer-auth HTTP wrapper
        ├── bench/                      # Driver scripts. Each is `npm run bench:<name>`.
        │   ├── jitter-{anycable,socketio,socketio-csr,uws,multi}.ts
        │   ├── avalanche-{anycable,socketio,railway-socketio,railway-uws,multi,multi-uws}.ts
        │   ├── throughput-{anycable,socketio,socketio-csr,socketio-redis,uws,multi}.ts
        │   ├── deploy-impact-{socketio-redis,standalone-anycable,standalone-socketio}.ts
        │   ├── whispers.ts             # Whispers (client-to-client fan-out) driver
        │   ├── whispers-multi.ts       # Multi-shard whispers driver
        │   ├── idle-multi.ts           # Multi-shard idle-connection capacity driver
        │   ├── latency-trace-anycable.ts        # Phase-decomposed latency tracer
        │   ├── jitter-anycable-trace.ts         # Jitter w/ per-cable timeline trace
        │   ├── fetch-jitter-metrics.ts          # Pull jitter-window metrics from Railway
        │   ├── railway-metrics.ts               # Pull memory/CPU from Railway GraphQL
        │   ├── tests-manifest.ts                # Canonical list of every rebaseline test
        │   ├── rebaseline.ts                    # Walk manifest, regress vs baselines, exit non-zero on drift
        │   └── rebaseline-history.ts            # Print per-metric trend across past rebaseline runs
        └── lib/                        # Shared core, runners at top + foundational helpers in core/
            ├── jitter-runners.ts                # runJitter{Anycable,Socketio,SocketioCsr}
            ├── jitter-uws.ts                    # runJitterUws (parallel for shape with above)
            ├── jitter-anycable-traced.ts        # AnyCable jitter + per-cable timeline trace
            ├── avalanche-runner.ts              # Socket.io avalanche runner
            ├── avalanche-uws.ts                 # uWS avalanche runner
            ├── deploy-impact-runner.ts          # Socket.io+Redis adapter deploy-impact
            ├── standalone-deploy-impact-runner.ts          # Standalone-publisher deploy-impact (Socket.io)
            ├── standalone-deploy-impact-anycable-runner.ts # Same for AnyCable
            ├── whispers-runner.ts               # Whispers (3 protocols share)
            ├── throughput.ts                    # Throughput runners (all 6 protocols)
            ├── idle-runner.ts                   # Idle-connection capacity runner
            ├── anycable-trace.ts                # Phase-decomposed AnyCable broadcast latency
            └── core/                            # Foundational helpers; every runner imports from here
                ├── params.ts                        # Param parsing (env or query-string)
                ├── stats.ts                         # ClientStat, percentiles, summarize
                ├── stats.test.ts                    # Unit tests; npm test runs them
                ├── timing.ts                        # MIN_OFFLINE_MS + ramp settle helpers
                ├── peak-rss.ts                      # Polling RSS peak tracker (per-test)
                ├── log.ts                           # Leveled logger (BENCH_LOG_LEVEL)
                ├── results-dir.ts                   # CSV/JSON output path helper (backend/results/)
                ├── chart.ts                         # ASCII chart helpers for the metrics dumps
                ├── bench-runner-client.ts           # Driver-side bearer-token fetch wrapper
                ├── job-queue.ts                     # bench-runner async job state
                ├── shard-coordinator.ts             # Multi-shard fan-out + result merger
                └── railway-api.ts                   # Railway GraphQL: tokens, metrics, mutations
```

## Two run modes

The local CLI scripts and the Railway-hosted HTTP endpoints both call the same `runJitter*` (and `runThroughput*`, etc.) functions in `src/lib/*`, so headline numbers are produced by exactly the same code regardless of where you run them.

**Local (small scale, dev laptop).** Each `src/bench/*.ts` script reads env vars, calls a runner, prints a human-readable report. Comfortable up to ~1,000 clients on a developer machine; beyond that you'll hit local NAT or event-loop limits.

**Railway-hosted bench-runner (10K+).** `src/bench-runner/server.ts` is an Express app deployed as a separate Railway service in the same project as `socketio-server` and `anycable-go`. It uses Railway's internal network (`*.railway.internal`) to reach the targets, no NAT, no public-internet round-trip, no client-side bottlenecks. This is how the 10K headline numbers above were produced. The bench-runner enforces bearer-token auth on every `/bench-*` endpoint (see `BENCH_RUNNER_TOKEN` in [`docs/env.md`](./docs/env.md)).

## Local quick-start

### Prerequisites

- Node.js 22+
- Either Docker (for the `docker-compose` quick-start) or a local [anycable-go](https://docs.anycable.io/anycable-go/getting_started) binary (`brew install anycable-go`).

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

Run any of the three benches in a third terminal. Each script publishes its own messages, so no separate publisher process is needed:

```bash
# Default Socket.io (per-message POST to socketio-server's /_broadcast,
# symmetric with how AnyCable is published)
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

Deploy the same source as two Railway services in one project:

- `socketio-server` from this repo (uses `backend/Dockerfile`; default `SERVICE_ENTRY` is `socketio/server`). Serves `/_broadcast` for per-message HTTP publishing (the default jitter / throughput path, symmetric with AnyCable's `/_broadcast`) and `/publish-local` for in-process `io.to().emit()` publishing (diagnostic only).
- `bench-runner`: second service from this repo with `SERVICE_ENTRY=bench-runner/server`. Targets `*.railway.internal` by default.

Plus the AnyCable target:

- `anycable-go`: official Docker image (`anycable/anycable-go:latest`) with `ANYCABLE_BROKER=memory`, `ANYCABLE_PRESETS=broker`, `ANYCABLE_PUBLIC=true`, and `ANYCABLE_HTTP_BROADCAST_SECRET=<your-secret>`.

Set on bench-runner:

- `ANYCABLE_BROADCAST_SECRET=<same secret>`
- `BENCH_RUNNER_TOKEN=<random 32+ char string>` (gate; the drivers send the same token via `Authorization: Bearer <...>`)

Generate a public domain for `bench-runner` (so you can curl it). With `BENCH_RUNNER_TOKEN` set, the public domain is safe: every endpoint except `/health` requires the bearer header.

### Run

Each endpoint is synchronous by default (`?async=1` switches to background-job mode + poll `/jobs/:id`). The request blocks until the run completes, typically 3 to 5 minutes at 10K.

```bash
# AnyCable @ 10K
curl --max-time 320 -X POST \
  -H "Authorization: Bearer $BENCH_RUNNER_TOKEN" \
  "https://bench-runner-production.up.railway.app/bench-jitter-anycable?n=10000&duration=200&msgs=120&interval=500&jitter=15&jitterMs=1000&ramp=300&stream=run-ac"

# Default Socket.io @ 10K
curl --max-time 320 -X POST \
  -H "Authorization: Bearer $BENCH_RUNNER_TOKEN" \
  "https://bench-runner-production.up.railway.app/bench-jitter-socketio?n=10000&duration=200&msgs=120&interval=500&jitter=15&jitterMs=1000&ramp=300&stream=run-d"

# Socket.io + CSR @ 10K
curl --max-time 320 -X POST \
  -H "Authorization: Bearer $BENCH_RUNNER_TOKEN" \
  "https://bench-runner-production.up.railway.app/bench-jitter-socketio-csr?n=10000&duration=200&msgs=120&interval=500&jitter=15&jitterMs=1000&ramp=300&stream=run-csr"
```

Each response includes:

- `deliveryRatePct`, `lostDeliveries`, `expectedDeliveries`, `receivedDeliveries`
- `jitterEvents`, `csrResumes`, `csrResumeRatePct`, `connectFailures`
- `latencyRawMs` and `latencyOverMinMs`: `{ avg, p50, p95, p99, max }` plus `skewFloor` (clock skew between bench-runner and the broadcasting host)
- `runnerPeakRssMb`: bench-runner process peak RSS

## Railway: avalanche at scale

```bash
SOCKETIO_URL=https://your-socketio.up.railway.app NUM_CLIENTS=5000 \
  npm run bench:avalanche:railway
```

In a second terminal, when the script reports "All clients connected":

```bash
railway restart -s socketio-server --yes
```

## Connection-capacity test

The bench-runner exposes a synchronous probe (`POST /bench-idle-anycable`, `/bench-idle-socketio`, `/bench-idle-uws`) that opens N raw WebSocket connections to the target via internal network, holds for `holdSec`, and returns final counts.

**Single-shard (up to ~50K):**

```bash
curl --max-time 600 -X POST -H "Authorization: Bearer $BENCH_RUNNER_TOKEN" \
  "https://bench-runner.up.railway.app/bench-idle-anycable?n=50000&hold=120&ramp=300"
```

Each Linux container has a per-source-IP outbound port pool of ~64K, which caps any single shard at ~50K useful connections.

**Multi-shard (100K – 1M):** see [`docs/railway-ops.md`](./docs/railway-ops.md#deploy-code-to-all-50-shards-in-parallel) for how the 50-shard fleet gets provisioned, then drive with `bench:idle:multi`:

```bash
# Build the SHARDS env var (50 shards):
SHARDS=$(printf 'https://bench-runner-production.up.railway.app'
         for i in $(seq 2 50); do
           printf ',https://bench-runner-%s-production.up.railway.app' "$i"
         done)

# Run 1M idle against anycable-go on a 32 vCPU / 32 GB box:
SHARDS="$SHARDS" \
  PER_SHARD_N=20000 HOLD_SEC=120 RAMP_PER_SEC=200 \
  PROJECT_ID=<railway-project-uuid> SERVICE_ID=<anycable-go-service-uuid> \
  SERVICE_NAME=anycable-go \
  npm run bench:idle:multi

# Socket.io: TARGET=socketio + SERVER_URL=
TARGET=socketio SERVER_URL=http://socketio-server.railway.internal:3000 \
  SHARDS="$SHARDS" PER_SHARD_N=20000 ... npm run bench:idle:multi

# uWebSockets.js: TARGET=uws + UWS_WS_URL=
TARGET=uws UWS_WS_URL=ws://uws-server.railway.internal:3000/ws \
  SHARDS="$SHARDS" PER_SHARD_N=20000 ... npm run bench:idle:multi
```

`PROJECT_ID` and `SERVICE_ID` are optional. Without them the script reports aggregate counts only (no Railway metrics chart or CSV).

## Re-baselining

The page numbers are kept honest by a tests manifest at `backend/src/bench/tests-manifest.ts` and a single-command driver.

```
cd backend
BENCH_RUNNER_URL=https://bench-runner-production.up.railway.app \
BENCH_RUNNER_TOKEN=<your-token> \
  npm run bench:rebaseline
```

This walks the 24 default tests (latency, jitter, whispers, throughput), hits each bench-runner endpoint, writes the result JSON to `tmp/v1.6.14-bench-results/{id}.json`, and prints a delta-vs-baseline report. Drift outside the per-test threshold gets a yellow `drift` flag; regressions (delivery dropped, threshold breached on key metrics) get a red `regress` and the process exits non-zero.

```
FILTER=jitter             # only jitter tests
FILTER=latency-anycable   # only AnyCable latency
FILTER=jitter,whispers    # multiple categories or substrings
DRY_RUN=1                 # print the plan, don't run
INCLUDE_IDLE=1            # adds 4 idle tests (multi-shard, ~16 min)
INCLUDE_AVALANCHE=1       # adds 5 avalanche tests (auto-redeploys server)
```

A full sweep with everything (33 tests) takes ~90 min wall-clock.

**Baselines vs page numbers.** The page numbers were captured during a Railway shared-tenant window where neighbour load pushed latencies higher; the `baseline` field in `tests-manifest.ts` is what the same tests now deliver on a quieter refresh, which is ~50% better on latency. So the page shows the *worst* we measured under realistic shared-infra load (the cautious number), and the rebaseline tests against today's *current* steady-state floor (the optimistic number). A green rebaseline confirms "we still beat today's floor"; it does not assert "the page numbers reproduce exactly right now" (they reproduce *or better*). When we refresh the page, baselines and page numbers move together. See the comment block at the top of `tests-manifest.ts`.

**Per-run history** lives at `tmp/v1.6.14-bench-results/runs/{ISO-ts}/`. The latest result is also kept at the un-timestamped path so existing `jq` queries still work. To see how each headline number has moved across runs:

```
npm run bench:rebaseline:history
LAST=10 FILTER=jitter npm run bench:rebaseline:history
```

## Environment variables

See [`docs/env.md`](./docs/env.md) for the full reference. The variables that show up most often:

- `BENCH_RUNNER_URL` and `BENCH_RUNNER_TOKEN` for any Railway driver.
- `NUM_CLIENTS`, `DURATION`, `JITTER_INTERVAL`, `JITTER_DURATION`, `TOTAL_MESSAGES`, `INTERVAL_MS`, `RAMP_RATE` for the jitter scripts.
- `SOCKETIO_URL` / `ANYCABLE_URL` / `BROADCAST_URL` to point local scripts at a non-default host.
- `RESULTS_DIR` to write CSV/JSON somewhere other than `backend/results/`.

## Infrastructure recipes

Provisioning, resizing, redeploying, deploying a 50-shard fleet, and pause-after-tests cost control all live in [`docs/railway-ops.md`](./docs/railway-ops.md).

## Notes and caveats

- **Like-for-like transports.** Both Socket.io and AnyCable run with WebSocket-only, no long-polling fallback for Socket.io.
- **AnyCable broker.** Benchmarks use the in-memory broker; production deployments typically use NATS or Redis to survive restarts and run multi-node.
- **Latency clock skew.** Publisher and clients run in different processes, possibly different containers. We report both raw and min-normalized latency so cross-variant comparisons are unaffected by skew. The headline replay-latency numbers on the compare page use the min-normalized view.
- **Cross-shard min-normalization assumes a fast sample per shard.** For multi-shard jitter runs, `mergeJitterResults` concatenates per-shard downsampled latencies and subtracts the global minimum to land at a common floor. This works when every shard observes at least one near-zero-latency sample. At our typical durations (3 to 5 minutes, thousands of messages per shard), that's reliably true; a shorter run where one shard's tail never dips low would over-report its merged p99 because the global minimum was set elsewhere. If you cut shard duration below ~60 s, sanity-check `skewFloor` per shard before trusting the merged percentiles.
- **Single-shard client-side saturation.** One bench-runner saturates around ~50K subscribers, the bottleneck is the Node event-loop work, not memory. Tests above that count fan out across multiple bench-runner replicas via `bench/idle-multi.ts` / `bench/jitter-multi.ts`.
- **Randomized jitter timings are not seeded.** Inter-jitter delays, reconnect jitter, and whisper stagger all use unseeded `Math.random()`. Runs reproduce statistically but not bit-for-bit. If you rerun the manifest and see p99 a few ms different from a recorded baseline, that's expected; consult `tmp/v1.6.14-bench-results/runs/` for the run-history context.
- **Delivery rate denominator.** `deliveryRatePct` divides received deliveries by `totalMessages × clients`, including clients that failed to connect. The result JSON also exposes `deliveryRateOfConnectedPct` (denominator excludes never-connected clients) so a run with N connect failures isn't silently capped below 100%. In healthy runs (`connectFailures: 0`) the two numbers are identical.

## About

Built by the [AnyCable](https://anycable.io) team alongside the comparison page at https://anycable.io/compare/nodejs-websocket. Reproducible benchmarks let any reader verify the claims; we keep the numbers honest by being able to re-run them.

If you find a methodological flaw, open an issue or a PR; we'd rather fix it than leave a wrong number standing.

## License

MIT, see [LICENSE](./LICENSE).
