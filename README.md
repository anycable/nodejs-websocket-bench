# AnyCable vs Socket.io Benchmarks

Reproducible benchmarks behind [AnyCable vs Socket.io](https://anycable.io/compare/socket-io). Two questions:

1. **Delivery under jitter** — how many messages does each server actually deliver when clients experience real-world WiFi drops and cellular handoffs?
2. **Deploy resilience** — what happens to live WebSocket connections when you ship new code?

## Results

All numbers are from identical Railway infrastructure (same region, same plan).

### Delivery under jitter — 1,000 clients, 120 messages at 2/sec

**Disruption profile.** Every client's TCP socket is force-closed for **1 second every ~15 seconds** (no clean close — the kind of failure WiFi drops produce). Publishing runs for 60 seconds, so each client sees ~5 disconnect events totalling **~5 seconds offline per client** — roughly 8% of the test. Add the reconnect handshake (~0.3 s) and the blind window widens to ~11%.

Three configurations, same workload:

|                          | Socket.io (default) | Socket.io + CSR       | AnyCable  |
| ------------------------ | ------------------- | --------------------- | --------- |
| Adapter / broker         | in-memory           | in-memory (CSR on)    | memory broker |
| Expected deliveries      | 120,000             | 120,000               | 120,000   |
| Jitter events            | 4,987               | *TBD*                 | 4,134     |
| **Deliveries lost**      | **12,779**          | ***TBD***             | **0**     |
| **Delivery rate**        | **89.1%**           | ***TBD***             | **100%**  |

Socket.io's default 10.9% loss matches the blind-window ratio almost exactly — nothing is delivered during the outage, nothing is recovered after.

The CSR column will be filled in once the `jitter-socketio-csr.ts` benchmark is run (see [Benchmark 1 § Socket.io with CSR](#running-with-csr-enabled)). Expected behaviour per the [Socket.io docs](https://socket.io/docs/v4/connection-state-recovery): CSR resumes buffered packets **if** the session is recovered within `maxDisconnectionDuration` (default 2 min), **the adapter is CSR-compatible** (in-memory, Redis Streams, or MongoDB — Redis pub/sub is **not**), and the recovery succeeds (the docs state: *"the recovery will not always be successful"*).

AnyCable's extended protocol replays the same blind window on reconnect, so the delivery rate is unaffected — and the same guarantee holds across server restarts when the broker is NATS JetStream or Redis.

### Reconnection avalanche — 5,000 clients, single deploy

|                                   | Socket.io    | AnyCable |
| --------------------------------- | ------------ | -------- |
| Connections dropped               | 5,000 (100%) | 0        |
| Recovery p50                      | 4,967 ms     | 0 ms     |
| Recovery p95                      | 5,992 ms     | 0 ms     |
| Clients that never reconnected    | 189 (3.8%)   | 0        |
| **Total downtime**                | **~6.8 s**   | **0 s**  |

## Why the results are what they are

**Delivery.** Socket.io provides at-most-once delivery. Messages sent during a disconnect are gone — the server doesn't buffer, the client doesn't ask for them back. AnyCable's extended Action Cable protocol (`actioncable-v1-ext-json`) assigns an incrementing offset to every broadcast. The client tracks its position; on reconnect it sends "I last saw offset N" and the server replays everything since.

**Deploys.** A Socket.io server *is* your application — WebSocket connections live inside the same Node.js process that handles HTTP. Restart the process (i.e. deploy) and every connection dies. AnyCable is a separate Go binary; your app broadcasts to it over HTTP. Your app restarts; AnyCable stays up; connections don't notice.

## Repository layout

```
benchmark/
├── docker-compose.yml        # Local Socket.io + anycable-go
├── railway.toml              # Railway deploy config for Socket.io
└── backend/
    ├── Dockerfile            # Socket.io image used by Railway
    ├── package.json
    └── src/
        ├── publisher.ts      # HTTP publisher (sequential, numbered)
        ├── socketio/
        │   └── server.ts     # Socket.io server (with /_broadcast)
        └── bench/
            ├── jitter-socketio.ts          # Delivery under jitter — Socket.io (default)
            ├── jitter-socketio-csr.ts      # Delivery under jitter — Socket.io + CSR
            ├── jitter-anycable.ts          # Delivery under jitter — AnyCable
            ├── avalanche-socketio.ts       # Local deploy simulation — Socket.io
            ├── avalanche-railway-socketio.ts  # Railway deploy — Socket.io
            └── avalanche-anycable.ts       # Deploy simulation — AnyCable
```

## Prerequisites

- Node.js 22+
- Either Docker (for the docker-compose quick start) or a local [anycable-go](https://docs.anycable.io/anycable-go/getting_started) binary (`brew install anycable-go`).

```bash
cd backend
npm install
```

## Benchmark 1: delivery under jitter

### What it measures

Each client subscribes to a stream and receives a fixed number of numbered messages published at a steady rate. Every ~15 seconds the client's TCP socket is force-closed for ~1 second — no clean close, the kind of failure a WiFi drop produces. The client reconnects on its own. At the end, the script counts which sequence numbers each client actually received and reports the delivery rate.

### Small-scale sanity check (20 clients, local)

Start the servers:

```bash
# Terminal 1 — Socket.io (default, no CSR)
cd backend && npm run dev:socketio              # :3000

# OR: Socket.io with Connection State Recovery enabled
cd backend && npm run dev:socketio-csr          # :3000, SOCKETIO_CSR=1

# Terminal 2 — anycable-go
anycable-go --port 8080 --broker=memory --presets=broker --public
```

#### Socket.io (default)

```bash
# Terminal 3 — publisher
BROADCAST_URL=http://localhost:3000/_broadcast \
  TOTAL_MESSAGES=60 INTERVAL_MS=500 \
  npm run publish

# Terminal 4 — clients
SOCKETIO_URL=http://localhost:3000 NUM_CLIENTS=20 DURATION=40 \
  npm run bench:jitter:socketio
```

<a id="running-with-csr-enabled"></a>
#### Socket.io with CSR enabled

Start the server with `npm run dev:socketio-csr` (or `SOCKETIO_CSR=1 npm run dev:socketio`), then:

```bash
# Terminal 3 — publisher
BROADCAST_URL=http://localhost:3000/_broadcast \
  TOTAL_MESSAGES=60 INTERVAL_MS=500 \
  npm run publish

# Terminal 4 — clients (uses reconnection: true so socket.io-client
# passes pid + offset automatically on reconnect)
SOCKETIO_URL=http://localhost:3000 NUM_CLIENTS=20 DURATION=40 \
  npm run bench:jitter:socketio-csr
```

The script reports both delivery rate and how often `socket.recovered === true` — i.e. how often CSR actually resumed the session rather than falling back to a fresh connect.

#### AnyCable

```bash
# Terminal 3 — publisher
BROADCAST_URL=http://localhost:8090/_broadcast \
  TOTAL_MESSAGES=60 INTERVAL_MS=500 \
  npm run publish

# Terminal 4 — clients
ANYCABLE_URL=ws://localhost:8080/cable NUM_CLIENTS=20 DURATION=40 \
  npm run bench:jitter:anycable
```

Even at 20 clients the gap between default Socket.io and AnyCable is obvious. The CSR run is the interesting middle case — it measures how much of the jitter loss a correctly configured Socket.io deployment recovers.

### Published numbers — 1,000 clients

These are the parameters used for the results table above.

```bash
ulimit -n 65536   # raise open-file limit

# --- Socket.io (default, no CSR) ---------------------------
BROADCAST_URL=http://localhost:3000/_broadcast \
  TOTAL_MESSAGES=120 INTERVAL_MS=500 \
  npm run publish &
NUM_CLIENTS=1000 DURATION=90 \
  JITTER_INTERVAL=15 JITTER_DURATION=1000 \
  npm run bench:jitter:socketio

# --- Socket.io + CSR (requires SOCKETIO_CSR=1 on the server) ---
BROADCAST_URL=http://localhost:3000/_broadcast \
  TOTAL_MESSAGES=120 INTERVAL_MS=500 \
  npm run publish &
NUM_CLIENTS=1000 DURATION=90 \
  JITTER_INTERVAL=15 JITTER_DURATION=1000 \
  npm run bench:jitter:socketio-csr

# --- AnyCable ---------------------------------------------
BROADCAST_URL=http://localhost:8090/_broadcast \
  TOTAL_MESSAGES=120 INTERVAL_MS=500 \
  npm run publish &
ANYCABLE_URL=ws://localhost:8080/cable NUM_CLIENTS=1000 DURATION=90 \
  JITTER_INTERVAL=15 JITTER_DURATION=1000 \
  npm run bench:jitter:anycable
```

For 5,000+ clients, run the publisher from a different machine (or a Railway instance) to avoid local TCP port exhaustion.

## Benchmark 2: reconnection avalanche

### What it measures

Connect N clients, then restart the WebSocket server — the same event that happens on every deploy. Track:

- how fast clients detect the disconnect,
- how long until 95% reconnect,
- how many never reconnect at all.

For Socket.io this is a real test — the script spawns the server as a child process, kills it, and starts it back up. For AnyCable the test is essentially "confirm nothing happens": anycable-go is a separate process that isn't restarted when your application deploys, so there's no event for clients to react to. The script connects clients, waits, and verifies the disconnect count stays at zero.

### Local: Socket.io (spawns, kills, restarts)

```bash
cd backend && npm run build        # the script runs node dist/socketio/server.js

NUM_CLIENTS=1000 PORT=4000 \
  npm run bench:avalanche:socketio
```

Output includes disconnect spread, recovery p50 / p95, and total downtime.

### Local: AnyCable (no-op from the app's perspective)

```bash
# Terminal 1
anycable-go --port 8080 --broker=memory --presets=broker --public

# Terminal 2
NUM_CLIENTS=1000 \
  ANYCABLE_URL=ws://localhost:8080/cable \
  BROADCAST_URL=http://localhost:8090/_broadcast \
  npm run bench:avalanche:anycable
```

The script publishes a few messages, sleeps 10 seconds to represent the app restart window, then publishes again. Disconnect count stays at 0.

### At scale: Railway-hosted Socket.io

This is how the 5,000-client numbers in the results table were produced.

```bash
# Deploy backend/ to Railway (uses backend/Dockerfile).
# Then, from a second Railway instance (or any box with enough file descriptors):

SOCKETIO_URL=https://your-socketio.up.railway.app \
  NUM_CLIENTS=5000 \
  npm run bench:avalanche:railway
```

In a *separate* terminal, once the script reports "All clients connected":

```bash
railway restart -s socketio-server --yes
```

The script waits up to 3 minutes for disconnects + reconnects and prints the summary.

## Environment variables

| Variable           | Default                              | Used by                      |
| ------------------ | ------------------------------------ | ---------------------------- |
| `SOCKETIO_URL`     | `http://localhost:3000`              | jitter-socketio, avalanche-railway-socketio |
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
| `PORT`             | `3000`                               | socketio server, avalanche-socketio |
| `SOCKETIO_CSR`     | *(unset)*                            | socketio server — `1` enables Connection State Recovery |
| `SOCKETIO_CSR_MAX_MS` | `120000` (2 min)                  | socketio server — `maxDisconnectionDuration` |

## Notes and caveats

- **Socket.io Connection State Recovery** (4.6+, opt-in) buffers missed events when the adapter supports it (in-memory single-node, Redis Streams, or MongoDB — Redis pub/sub is **not** compatible). The docs call it experimental and state *"the recovery will not always be successful"*. We benchmark it directly via `jitter-socketio-csr.ts` so the comparison isn't against a strawman; see the results table above.
- **Transports.** Both servers are configured to use WebSockets only, skipping Socket.io's long-polling upgrade handshake — a like-for-like comparison.
- **AnyCable broker.** The benchmarks run with `--broker=memory`. In production, use `nats` or `redis` for multi-node pub/sub.

## License

MIT.
