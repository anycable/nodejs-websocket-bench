# How to benchmark a WebSocket layer honestly

Field notes from building the benchmarks behind [anycable.io/compare/nodejs-websocket](https://anycable.io/compare/nodejs-websocket). The repo is open; the bench-runner endpoints accept the same parameters anyone can curl.

Three questions for production JS/TS apps:

1. Does the WebSocket layer deliver every message it was told to send, including when the network blinks?
2. Does it survive deploys without losing or stalling user connections?
3. How many connections fit on a single instance?

Each one has its own measurement trap. The traps were the interesting part.

## The architecture

```
                ┌──────────────────────────────────────────────┐
                │            Railway, single region            │
                │                                              │
   driver       │   socketio-server  ◄─── /_broadcast ┐        │
   (local) ───► │    (Node 22)                        │        │
                │                                     │        │
                │   anycable-go ◄── /_broadcast ──────┤        │
                │   (or anycable-go-pro)              │        │
                │                                     │        │
                │   uws-server     ◄── /_broadcast ───┤        │
                │                                     │        │
                │   bench-runner ◄───── POST ─────────┘        │
                │   (50× shards for 1M-scale tests)            │
                └──────────────────────────────────────────────┘
                         ▲
                         │  HTTPS, 5-min edge cap
                         │  → bench-runner returns 202 {jobId}
                         │  → driver polls /jobs/:id
```

Three rules:

- **The thing being measured is separate from the thing measuring.** The WS layer (Socket.io, AnyCable, uWS) is the subject. The bench-runner is the load generator. Put them in the same process and every dropped frame in the generator becomes a dropped frame in your result, with no way to tell which.
- **The driver runs local, the work runs remote.** The CLI on your laptop triggers tests and waits. WebSocket load lives entirely on the bench-runner.
- **One clock owns latency.** `sentAt` is stamped when the bench-runner dispatches the publish; `receivedAt` is stamped when the subscriber callback runs. One process, one clock, no cross-host NTP arithmetic.

## Question 1: delivery under jitter

### Disruption model

Every subscriber, every ~15 seconds, force-closes its underlying TCP socket. Not `socket.close()`. `socket.terminate()` where the WS implementation exposes it. Socket.io's `close()` sends a polite leave packet; the WiFi-drop case gives the server nothing.

```ts
// lib/jitter-runners.ts
function terminateUnderlyingTcp(socket: Socket): boolean {
  const raw = (socket as unknown as {
    io?: { engine?: { transport?: { ws?: WebSocket } } };
  }).io?.engine?.transport?.ws;
  if (typeof raw?.terminate === "function") {
    raw.terminate();
    return true;
  }
  return raw?.close ? (raw.close(), true) : false;
}
```

Reaching into private `io.engine.transport.ws` is the only way; socket.io-client doesn't expose the raw `ws` instance.

### The trap that flipped the page narrative

Our first jitter test used `jitterDurationMs = 1000`. A reader sees "1 second offline" and assumes every config faces the same window. Each config actually faces something different:

| Setup | Mechanism | Effective offline window |
|---|---|---|
| Default Socket.io | `reconnection: false`, runner opens fresh socket after sleep | sleep length sets it directly |
| Socket.io + CSR | library's `reconnectionDelay: 2000` | ~2.0–5.0 s |
| AnyCable | `@anycable/core` Monitor backoff | ~2.0 s |
| uWS | custom reconnecting wrapper matching socket.io-client | ~2.0–5.0 s |

Default Socket.io was the outlier because the library never got a chance to apply its backoff. We added `MIN_OFFLINE_MS = 2000` in `lib/core/timing.ts` so the four configurations face the same disruption shape.

Equalizing surfaced the lesson. Before the fix, default Socket.io showed ~27% delivery and CSR showed ~80% with a 100-second p95 tail. After: default Socket.io 84%, CSR 100% / 1.97 s p95. The 100-second CSR tail was reconnect-storm behaviour, 10K clients all reconnecting inside a 1-second window while the Railway box serialised accept-queue work. The headline had conflated a delivery story with a load-shedding story.

When a benchmark gives you a dramatic result, ask what other variable might be coupled to the one you think you're testing. Equalise it. If the result holds, it's real. If the shape changes, you were measuring something else.

### Headline numbers

10K subscribers, 120 broadcasts at 500 ms intervals, force-close every ~15 s, ~2 s offline per event:

| Setup | Delivery | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| Default Socket.io | 84.55% | 106 ms | 394 ms | 1.07 s | 1.75 s |
| Socket.io + CSR | **100%** | 148 ms | 1.97 s | 4.58 s | 9.71 s |
| uWS topics | 87.03% | 92 ms | 722 ms | 1.72 s | 2.95 s |
| AnyCable OSS | **100%** | 250 ms | 4.10 s | 6.14 s | 9.23 s |
| AnyCable Pro | **100%** | 261 ms | 4.10 s | 6.15 s | 9.36 s |

At-most-once protocols drop what landed during each offline window. Replay protocols deliver. CSR's tail is slightly shorter at p99 in this in-memory setup. AnyCable's wins live elsewhere: deploy resilience (separate Go process), horizontal scaling with replay intact, memory efficiency at 1M scale, native client-to-client whispers, broad backend support (Ruby + Rails + Node + Bun + Deno).

### Latency arithmetic across clocks

A subscriber records `Date.now() - sentAt`. Publisher and subscriber are different processes by design (separate WS server next to the app); wall-clock skew is real.

We min-normalise. After the run, subtract the minimum observed latency from every sample. Report both the raw and the normalised series, plus the `skewFloor` so a reviewer can spot anomalous skew.

```ts
// lib/core/stats.ts
const lmin = allLatencies[0]; // sorted ascending
const norm = allLatencies.map((v) => v - lmin);
```

Pinning both timestamps with `performance.now()` on one host would solve the skew problem and break the comparison: production never colocates the publisher with subscribers.

### Cross-shard percentile honesty

For tests beyond one container's port pool, we fan out. Averaging per-shard p99s gives the wrong answer; it ignores the union distribution. Each shard returns a downsampled sorted-latency array (~5K samples); the driver concatenates, resorts, recomputes:

```ts
// lib/core/stats.ts
export function mergeJitterResults(label: string, shards: JitterResult[]) {
  const merged: number[] = [];
  for (const s of shards) for (const v of s.latencySamplesSorted!) merged.push(v);
  merged.sort((a, b) => a - b);
  // recompute p50/p95/p99 over merged
}
```

The catch: min-normalisation assumes each shard sees at least one fast sample. True at 3–5 minute windows; risky at 30-second shards.

---

## Question 2: deploys

### Avalanche test

5,000 clients ramp, then the operator runs `railway redeploy -s socketio-server --yes`. The bench-runner watches for the first `disconnect` event (that's "deploy detected") and measures how long until 95% have reconnected.

Our first version had a bug worth flagging. Listeners were attached after the ramp loop, so the deploy's millisecond disconnect storm landed on a runner that hadn't started listening. Fix: attach `connect`/`disconnect` at socket creation, gate them with a `tearingDown` flag during cleanup.

```ts
// lib/avalanche-runner.ts (comment carries the why)
// Listeners attached at socket creation, before the restart can possibly
// fire, so events are never lost in the gap between the SIGKILL landing
// and the runner being ready to listen.
```

### Standalone vs embedded

Comparing "Socket.io in-process" against "AnyCable as a separate process" measures deployment topology, not protocol. To be fair, test both shapes:

- **In-process** for Socket.io / uWS. Publisher runs inside the WS server via `/publish-local`. The default deployment for those libraries.
- **Standalone** for Socket.io / uWS. Separate publisher service POSTs to `/_broadcast` over HTTP. The shape we'd use in production for survivability, and the only fair comparison for AnyCable (always a separate process).

The page shows both. The point is matching the reader's planned topology, not picking a winner per protocol.

### Publisher symmetry

Our first design had AnyCable publishing one POST per broadcast and Socket.io publishing in-process via a bulk `/publish-local` kickoff (one POST, then N emits inside the server). Different workloads. If the test is 100 broadcasts, every setup does 100 broadcasts.

We converted Socket.io and uWS to per-message HTTP publishing as the default. `/publish-local` survives as a diagnostic for isolating in-process fan-out cost.

The general lesson: **the test shape stays identical across all setups.** Number of HTTP requests, size of each, path between publisher and broadcaster. Symmetric, or the comparison is decoration.

### What the test reveals

A Socket.io server is your application. Connections live in the same Node process that handles HTTP. Restart the process and every connection dies. AnyCable is a separate Go binary; your app broadcasts to it over HTTP, your app restarts, AnyCable stays up, connections don't notice.

5,000 / 5,000 disconnects for Socket.io versus 0 / 5,000 for AnyCable. No clever measurement. Architecture.

### Rolling deploys

For Socket.io + Redis cluster, we redeploy nodes sequentially. Each client lands on one node and experiences exactly one disconnect-reconnect cycle, which makes the "first disconnect = deploy detected" heuristic correct for 2-node setups. A 3-node rolling deploy where a client lands on A, gets bounced to B, then experiences another deploy on B would break the heuristic. We haven't tested that; `deploy-impact-runner.ts` doesn't handle it.

---

## Question 3: connection capacity

### Why "open N and hold" is harder than it looks

A Linux container has a per-source-IP outbound port pool of ~64K. After kernel reservations, ~50K useful ports. A single load-generator container caps at ~50K WebSocket connections to one target.

1M idle WebSockets to one server needs at least 20 source IPs. The traditional workaround is kernel tuning (`net.ipv4.ip_local_port_range`, `net.ipv4.tcp_tw_reuse`), which requires root, is platform-specific, and turns a benchmark into a kernel-tuning exercise nobody will reproduce.

Our path: 50 bench-runner containers, each its own source IP, each its own port pool. A coordinator fans out POSTs in parallel.

```bash
SHARDS=https://bench-runner-1.up.railway.app,https://bench-runner-2.up.railway.app,...
PER_SHARD_N=20000 HOLD_SEC=120 RAMP_PER_SEC=200 \
  npm run bench:idle:multi
```

50 containers cost something. We downsize them between sessions to ~64 MB each via Railway's `serviceInstanceLimitsUpdate` GraphQL mutation; standing cost is negligible. Recipes live in [`railway-ops.md`](./railway-ops.md).

### Railway's 5-minute HTTP edge cap

Railway's edge proxy caps inbound HTTP at 5 minutes. A 50K ramp + 120 s hold runs ~7 minutes. A synchronous request dies mid-test.

Job queue. Every bench-runner endpoint accepts `?async=1`. POST returns 202 with `{jobId}` immediately; work runs in background; coordinator polls `GET /jobs/:id` every 5 s and receives `{status, logTail, result?}`. Each request is sub-second.

```ts
// bench-runner/server.ts
async function respondAsync(req, res, run) {
  if (req.query.async === "1") {
    const jobId = startJob(run);
    res.status(202).json({ jobId });
    return;
  }
  res.json(await run());
}
```

The same pattern lets the coordinator stream per-shard progress to one terminal: each shard's `logTail` flows back, a small diff helper prints only the new lines.

### What "filled" means

`anycable-go-pro` held 999,954 of 1,000,000 at 19.34 GB / 9.4% CPU. The 46 missing connections were handshake races during ramp, not a memory or CPU limit. At "does it fit in one instance?", 99.995% is the answer.

`anycable-go` OSS hit 993,994 at 32 GB peak. That **is** the ceiling: 32 GB box, ~33 KB per connection, no headroom, kernel started OOM-thinking. Pro at 19 KB/conn has 13 GB to spare and would hold more on a bigger box.

Socket.io stopped at 119,826 on the same 32-vCPU box with memory and CPU near idle. One core was saturated handling handshakes serially. The architectural ceiling, single-threaded JS, puts Socket.io in a different scaling category than the Go options regardless of RAM.

Three different stop conditions: RAM ceiling, Go-runtime soft ceiling, single-threaded JS ceiling. The numbers are only useful if you can name what stopped them.

---

## Things we got wrong

### Mixing offline window with reconnect-storm capacity

The CSR narrative flip is the headline lesson. With a 1-second offline window, the results read "CSR replays 80% with a 100-second tail; AnyCable replays 100% in 6 seconds." With a 2-second window (forced via `MIN_OFFLINE_MS`), CSR delivers 100% with a 4.6-second tail. AnyCable holds 100% with a 6.1-second tail. Two protocols, roughly equivalent on delivery and tail. AnyCable's wins move to architecture.

The 100-second tail was reconnect storm, not the CSR protocol.

### 100 subscribers × 10K messages for throughput

Our first throughput test had 100 subscribers each receiving 10K messages. The right pushback: real-world chat is more like 10K subscribers and 100 messages each. Same 1M deliveries, different load profile.

- **100 × 10K** stresses per-socket throughput. TCP send buffers, per-connection ring buffers.
- **10K × 100** stresses fan-out. Broker subscriber index, iteration over subscriber sets, concurrent socket writes.

For a chat app with 10K concurrent users, fan-out is the shape that matters. We switched.

### "AnyCable is slower at throughput"

We saw AnyCable under-perform uWS on raw throughput and assumed a test bug. Several rounds in (including swapping to NATS as the broker transport), Vladimir reproduced the test with his in-process `benchi` binary. No network hop. The gap was real for our config: a single HTTP publisher feeding `anycable-go`'s broadcaster. We added `benchi` to the bench-runner image and exposed it as an endpoint. The page now reports "production-shape over HTTP" and "in-process ceiling" side by side.

When a result looks wrong, the test might be right and the assumption wrong. Build the simplest second measurement that brackets the question from a different angle.

---

## The honesty loop: rebaseline

Numbers on the page rot. A patch release that improves things, a Railway scheduler change that makes them worse: both silently turn headlines into lies. Protection: `bench:rebaseline` walks a manifest of 24+ tests, hits each endpoint, diffs against recorded baselines, fails loudly when a number drifts.

```
$ BENCH_RUNNER_URL=https://bench-runner-production.up.railway.app \
  BENCH_RUNNER_TOKEN=<token> \
    npm run bench:rebaseline

  jitter-anycable-oss-10k  ✓ delivery=100% p99=6141ms (baseline 6200ms)
  jitter-socketio-csr-10k  ✓ delivery=100% p99=4575ms (baseline 4575ms)
  jitter-socketio-10k      ✓ delivery=84.55% p95=394ms (baseline 84% / 450ms)
```

Per-run history at `tmp/v1.6.14-bench-results/runs/{ISO-ts}/` plots trend lines across runs. Heavier categories (idle, avalanche) gate behind opt-in flags.

When a reviewer challenges a number, the answer is "rerun the manifest and show me the delta," not "trust the page."

When a drift threshold flags a test that's inside intrinsic variance, widen the threshold and write down why. (`throughput-anycable-pro` p99 swings 3–7 seconds between consecutive runs at 100 msg/s × 10K subs because `anycable-go` GC pauses dominate the tail. Threshold is 50% with a comment.) A noisy false-positive teaches people to ignore the report; worse than no signal.

---

## Methodology disclosures

The README's caveats section carries the full list. The ones worth knowing here:

- CSR tested with the default in-memory adapter. Production CSR typically uses Redis Streams for restart survivability; that adds RTT to the replay tail and doesn't change the structural picture.
- Both Socket.io and AnyCable run WebSocket-only. No long-polling fallback.
- AnyCable uses the in-memory broker; production runs typically use NATS or Redis.
- Latency clock skew handled by min-normalisation. The compare page uses the min-normalised view.
- Jitter timing, reconnect jitter, and whisper stagger all use unseeded `Math.random()`. Runs reproduce statistically, not bit-for-bit.
- Default Socket.io's per-event offline window is floored to `MIN_OFFLINE_MS=2000` so the four configurations face the same disruption shape.
- `deliveryRatePct` denominator includes clients that failed to connect; `deliveryRateOfConnectedPct` excludes them. Identical in healthy runs.

These are the surfaces where someone with more experience can push back productively.

---

## What "honest" means here

uWS holds more connections per GB than `anycable-go-pro` at 1M scale; the page says so. CSR's protocol works at the offline windows real users see, with a tail comparable to AnyCable's in the in-memory setup; we kept that result when the data flipped.

The claim: every number was produced by running this code on Railway, in the same project, over the test windows recorded in `tmp/v1.6.14-bench-results/`. Find a flaw, open an issue. Rerun and get different numbers, open an issue. The page goes wrong the moment the rebaseline manifest can't reproduce its numbers; the manifest is what keeps us honest.

Four rules that survived this build:

- Run the comparator's library at its best, not its weakest.
- Use the comparator's default settings unless you have a documented reason to disagree.
- Publish the code, the parameters, and the raw results. Not just the chart.
- Rebuild the test the moment someone shows you a fair criticism.

The rest is plumbing.

---

*Built by the AnyCable team. Code at [github.com/anycable/nodejs-websocket-bench](https://github.com/anycable/nodejs-websocket-bench). Page at [anycable.io/compare/nodejs-websocket](https://anycable.io/compare/nodejs-websocket).*
