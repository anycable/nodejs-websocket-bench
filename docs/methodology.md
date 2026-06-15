# How to benchmark a WebSocket layer honestly

A field report on building the benchmarks behind [anycable.io/compare/nodejs-websocket](https://anycable.io/compare/nodejs-websocket). The repo is open source: anyone can hit the same bench-runner endpoints and check the deltas.

Three questions, for production JS/TS apps:

1. Does the WebSocket layer deliver the messages it was told to send, including when the network blinks?
2. Does it survive deploys without losing or stalling user connections?
3. How many connections can a single instance hold?

Each one carries its own measurement trap. What follows is what we ended up with after enough rounds of "wait, that's not what we're measuring."

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

Three rules carried us through:

- **The thing being measured is not the thing measuring.** The WS layer (Socket.io, AnyCable, uWS) is the subject. The bench-runner is the load generator. Put them in the same process and every dropped frame in the generator becomes a dropped frame in your result, and you can't tell which.
- **The driver is local, the work is remote.** The CLI on your laptop triggers tests and waits. It doesn't generate WebSocket load itself.
- **All clocks belong to the bench-runner.** `sentAt` is stamped when the bench-runner dispatches the publish; `receivedAt` is stamped when the subscriber callback runs. One process, one clock. Latency arithmetic survives without cross-host NTP.

## Question 1: does it deliver during jitter?

### Disruption model

Each subscriber, every ~15 seconds, force-closes the underlying TCP socket. Not `socket.close()`. `socket.terminate()` if the WS implementation exposes it. Socket.io's `close()` sends a leave packet; the WiFi-drop case gives the server nothing.

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

The reach into private `io.engine.transport.ws` is unavoidable. socket.io-client doesn't expose the raw `ws` instance.

### The trap that flipped the page narrative

Our first jitter test used `jitterDurationMs = 1000`. A reader sees "1 second offline" and assumes every config faces the same window. They don't:

| Setup | Mechanism | Effective offline window |
|---|---|---|
| Default Socket.io | `reconnection: false`, runner opens fresh socket after sleep | sleep length sets it directly |
| Socket.io + CSR | library's `reconnectionDelay: 2000` | ~2.0–5.0 s |
| AnyCable | `@anycable/core` Monitor backoff | ~2.0 s |
| uWS | custom reconnecting wrapper matching socket.io-client | ~2.0–5.0 s |

Default Socket.io was the outlier because the library never got a chance to apply its backoff. We added `MIN_OFFLINE_MS = 2000` in `lib/timing.ts` so the four configurations face the same disruption shape.

Equalizing surfaced the lesson. Before the fix, default Socket.io showed ~27% delivery and CSR showed ~80% with a 100-second p95 tail. After the fix: default Socket.io 84%, CSR 100% / 1.97 s p95. The 100-second CSR tail wasn't the CSR protocol's behavior. It was 10K clients all reconnecting within a 1-second window while the Railway box serialized accept-queue work. The headline conflated a delivery story with a load-shedding story.

If your benchmark gives you a dramatic-looking result, ask what other variable might be coupled to the one you think you're testing. Equalize across that variable. If the result holds, it's real. If it changes shape, you were measuring something else.

### What the headline numbers say now

10K subscribers, 120 broadcasts at 500 ms intervals, force-close every ~15 s, ~2 s offline per event:

| Setup | Delivery | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| Default Socket.io | 84.55% | 106 ms | 394 ms | 1.07 s | 1.75 s |
| Socket.io + CSR | **100%** | 148 ms | 1.97 s | 4.58 s | 9.71 s |
| uWS topics | 87.03% | 92 ms | 722 ms | 1.72 s | 2.95 s |
| AnyCable OSS | **100%** | 250 ms | 4.10 s | 6.14 s | 9.23 s |
| AnyCable Pro | **100%** | 261 ms | 4.10 s | 6.15 s | 9.36 s |

At-most-once protocols lose what landed during each offline window. Replay protocols deliver. CSR's tail is slightly shorter at p99 in the in-memory setup. AnyCable's edge over CSR is elsewhere: deploy resilience (separate Go process), horizontal scaling with replay intact, memory efficiency at 1M scale, native client-to-client whispers, broader backend support (Ruby + Rails + Node + Bun + Deno vs Node-only).

### Latency arithmetic across clocks

A subscriber records `Date.now() - sentAt`. Publisher and subscriber are different processes by design (separate WS server next to the app); wall-clock skew is real.

We min-normalize. After the run, subtract the minimum observed latency from every sample, report both the raw and the normalized series, plus the `skewFloor` so a reviewer can spot anomalous skew.

```ts
// lib/stats.ts
const lmin = allLatencies[0]; // sorted ascending
const norm = allLatencies.map((v) => v - lmin);
```

Pinning both timestamps with `performance.now()` on one host would solve the skew problem and break the comparison: production never colocates the publisher with subscribers.

### Cross-shard percentile honesty

For tests beyond one container's port pool, we fan out. Averaging per-shard p99s is wrong; it doesn't reflect the union distribution. Each shard returns a downsampled sorted-latency array (~5K samples); the driver concatenates, resorts, recomputes:

```ts
// lib/stats.ts
export function mergeJitterResults(label: string, shards: JitterResult[]) {
  const merged: number[] = [];
  for (const s of shards) for (const v of s.latencySamplesSorted!) merged.push(v);
  merged.sort((a, b) => a - b);
  // recompute p50/p95/p99 over merged
}
```

The catch: min-normalization assumes each shard sees at least one fast sample. True at 3–5 minute test windows; potentially not at 30-second shards.

---

## Question 2: how does the WS layer survive a deploy?

### Avalanche test

5,000 clients ramped, then operator runs `railway redeploy -s socketio-server --yes`. Bench-runner watches for the first `disconnect` event (that's "deploy detected") and measures how long until 95% have reconnected.

Our first version had a bug worth flagging: listeners were attached after the ramp loop, so the deploy's millisecond disconnect storm landed on a runner that wasn't listening yet. Fix: attach `connect`/`disconnect` at socket creation, gate them with a `tearingDown` flag during cleanup:

```ts
// lib/avalanche-runner.ts (comment carries the why)
// Listeners attached at socket creation, before the restart can possibly
// fire, so events are never lost in the gap between the SIGKILL landing
// and the runner being ready to listen.
```

### Standalone vs embedded

When you compare "Socket.io in-process" against "AnyCable as a separate process," you're not comparing protocols, you're comparing deployment topologies. To be fair, test both shapes:

- **In-process** for Socket.io / uWS. Publisher runs inside the WS server via `/publish-local`. The default deployment for those libraries.
- **Standalone** for Socket.io / uWS. Separate publisher service POSTs to `/_broadcast` over HTTP. The shape we'd use in production for survivability. The only fair comparison for AnyCable, which is always a separate process.

The page shows both. It's not "which is better"; it's matching the reader's planned topology.

### Publisher symmetry

Our first design had AnyCable publishing one POST per broadcast and Socket.io publishing in-process via a bulk `/publish-local` kickoff (one POST, then N emits inside the server). Not a comparable workload. If we're testing 100 broadcasts, every setup does 100 broadcasts.

We converted Socket.io and uWS to per-message HTTP publishing as the default. `/publish-local` still exists as a diagnostic for isolating in-process fan-out cost.

The lesson generalizes: **the test shape needs to be the same across all setups.** Number of HTTP requests, size of each, path between publisher and broadcaster. Symmetric or your comparison isn't a comparison.

### What the test reveals

A Socket.io server is your application. Connections live in the same Node process that handles HTTP. Restart the process and every connection dies. AnyCable is a separate Go binary; your app broadcasts to it over HTTP. Your app restarts, AnyCable stays up, connections don't notice.

5,000 / 5,000 disconnects for Socket.io vs 0 / 5,000 for AnyCable. No clever measurement. Architecture.

### Rolling deploys

For Socket.io + Redis cluster, we redeploy nodes sequentially. Each client lands on one node and experiences exactly one disconnect-reconnect cycle, which makes our "first disconnect = deploy detected" heuristic correct for 2-node setups. A 3-node rolling deploy where a client lands on A, gets bounced to B, then experiences another deploy on B would break the heuristic. We haven't tested that yet; `deploy-impact-runner.ts` doesn't handle it.

---

## Question 3: how many connections can one instance hold?

### Why "open N and hold" is harder than it looks

A Linux container has a per-source-IP outbound port pool of ~64K. After kernel reservations, ~50K useful ports. So a single load-generator container caps at ~50K WebSocket connections to one target.

If you want 1M idle WebSockets to one server, you need at least 20 source IPs. The traditional workaround is kernel tuning (`net.ipv4.ip_local_port_range`, `net.ipv4.tcp_tw_reuse`), which requires root, is platform-specific, and turns your benchmark into a kernel-tuning exercise nobody will reproduce.

Our path: 50 bench-runner containers, each its own source IP, each its own port pool. A coordinator fans out POSTs in parallel:

```bash
SHARDS=https://bench-runner-1.up.railway.app,https://bench-runner-2.up.railway.app,...
PER_SHARD_N=20000 HOLD_SEC=120 RAMP_PER_SEC=200 \
  npm run bench:idle:multi
```

50 containers cost something. We downsize them between sessions (`railway` GraphQL `serviceInstanceLimitsUpdate`) to ~64 MB each, standing cost negligible. Recipes in the README.

### Railway's 5-minute HTTP edge cap

Railway's edge proxy caps inbound HTTP at 5 minutes. A 50K ramp + 120 s hold takes ~7 minutes. The synchronous request would die mid-test.

Job queue. Every bench-runner endpoint supports `?async=1`. POST returns 202 with `{jobId}` immediately; work runs in background; coordinator polls `GET /jobs/:id` every 5 s and gets `{status, logTail, result?}`. Each request is sub-second.

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

Same pattern lets the coordinator stream per-shard progress to one terminal: each shard's `logTail` flows back, a small diff helper prints only the new lines.

### What "filled" means

anycable-go-pro held 999,954 of 1,000,000 at 19.34 GB / 9.4% CPU. The 46 missing connections weren't a memory or CPU limit; probably handshake races during ramp. For "does it fit in one instance?", 99.995% answers.

anycable-go OSS hit 993,994 at 32 GB peak. That **is** the ceiling: 32 GB box, ~33 KB per connection, no headroom, kernel started OOM-thinking. Pro at 19 KB/conn has 13 GB to spare; would hold more on a bigger box.

Socket.io stopped at 119,826 on the same 32-vCPU box, with memory and CPU near idle. One core was saturated handling handshakes serially. The architectural ceiling, single-threaded JS, puts Socket.io in a different scaling category than the Go options regardless of RAM.

Three different stop conditions: RAM ceiling, Go-runtime soft ceiling, single-threaded JS ceiling. The numbers are only useful if you can name what stopped them.

---

## Things we got wrong

### Mixing offline window with reconnect-storm capacity

The CSR narrative flip is the headline lesson. With a 1-second offline window, our results read "CSR replays 80% with a 100-second tail; AnyCable replays 100% in 6 seconds." With a 2-second window (forced via `MIN_OFFLINE_MS`), CSR delivers 100% with a 4.6-second tail. AnyCable holds 100% with a 6.1-second tail. Two protocols, roughly equivalent on delivery and tail. AnyCable's wins move to architecture.

The 100-second tail wasn't the CSR protocol. It was reconnect storm.

### 100 subscribers × 10K messages for throughput

Our first throughput test had 100 subscribers receiving 10K messages each. The right pushback: "real-world is more like 10K subscribers and 100 messages." Same 1M deliveries, different load profile.

- **100 × 10K** stresses per-socket throughput. TCP send buffers, per-connection ring buffers.
- **10K × 100** stresses fan-out. Broker subscriber index, iteration over subscriber sets, concurrent socket writes.

For a chat app with 10K concurrent users, fan-out is the shape that matters. We switched.

### "AnyCable is slower at throughput"

We saw AnyCable under-perform uWS on raw throughput and assumed a test bug. After several rounds (including swapping to NATS as the broker transport), Vladimir reproduced the test with his in-process `benchi` binary. No network hop. Gap was real for our config: a single HTTP publisher feeding anycable-go's broadcaster. We added `benchi` to the bench-runner image and exposed it as an endpoint. Page now reports "production-shape over HTTP" and "in-process ceiling" side by side.

When the result looks wrong, your test might be right and your assumption wrong. Build the simplest second measurement that brackets the question from a different angle.

---

## The honesty loop: rebaseline

Numbers on the page rot. A patch release that improves things, a Railway scheduler change that worsens them, both silently turn your headlines into lies. Protection: `bench:rebaseline` walks a manifest of 24+ tests, hits each endpoint, diffs against recorded baselines, fails loudly when a number drifts.

```
$ BENCH_RUNNER_URL=https://bench-runner-production.up.railway.app \
    npm run bench:rebaseline

  jitter-anycable-oss-10k  ✓ delivery=100% p99=6141ms (baseline 6200ms)
  jitter-socketio-csr-10k  ✓ delivery=100% p99=4575ms (baseline 4575ms)
  jitter-socketio-10k      ✓ delivery=84.55% p95=394ms (baseline 84% / 450ms)
```

Per-run history at `tmp/v1.6.14-bench-results/runs/{ISO-ts}/` lets you plot trend lines across runs. Heavier categories (idle, avalanche) gate behind opt-in flags.

If a reviewer challenges a number, the answer isn't "trust the page." It's "rerun the manifest and show me the delta."

When a drift threshold flags a test that's within the test's intrinsic variance, widen the threshold and write down why. (`throughput-anycable-pro` p99 swings 3-7 seconds between consecutive runs at 100 msg/s × 10K subs because anycable-go GC pauses dominate the tail. Threshold is 50% with a comment.) A noisy false-positive teaches people to ignore the report; that's worse than no signal.

---

## Methodology disclosures

The README's "Notes and caveats" carries the full list. The ones worth knowing here:

- CSR tested with the default in-memory adapter, not Redis Streams. Production CSR would typically use Redis Streams for restart survivability; adds RTT to the replay tail, doesn't change the structural picture.
- Both Socket.io and AnyCable run WebSocket-only. No long-polling fallback.
- AnyCable uses the in-memory broker; production runs typically use NATS or Redis.
- Latency clock skew handled by min-normalization. Headline replay-latency numbers use the min-normalized view.
- Jitter timing, reconnect jitter, whisper stagger all use unseeded `Math.random()`. Runs reproduce statistically, not bit-for-bit.
- Default Socket.io's per-event offline window floored to `MIN_OFFLINE_MS=2000` so the four configurations face the same disruption shape.
- `deliveryRatePct` includes clients that failed to connect; `deliveryRateOfConnectedPct` excludes them. Identical in healthy runs.

These aren't asterisks. They're the surface where someone with more experience can push back productively.

---

## What "honest" means here

uWS holds more connections per GB than anycable-go-pro at 1M scale; we say so on the page. CSR's protocol works at the offline windows real users see, with a tail comparable to AnyCable's in the in-memory setup; we didn't bury that when the data flipped.

What we claim: every number was produced by running this code on Railway, in the same project, over the test windows recorded in `tmp/v1.6.14-bench-results/`. Find a flaw, open an issue. Rerun and get different numbers, open an issue. The page is wrong by construction the moment we can't reproduce its numbers; the rebaseline manifest is what keeps us honest.

Four rules that survived this build:

- Run the comparator's library at its best, not at its weakest.
- Use the comparator's default settings unless you have a reason to disagree, and document the reason.
- Publish the code, the parameters, and the raw results. Not just the chart.
- Rebuild the test the moment someone shows you a fair criticism.

The rest is plumbing.

---

*Built by the AnyCable team. Code at [github.com/irinanazarova/anycable-socketio-benchmarks](https://github.com/irinanazarova/anycable-socketio-benchmarks). Page at [anycable.io/compare/nodejs-websocket](https://anycable.io/compare/nodejs-websocket).*
