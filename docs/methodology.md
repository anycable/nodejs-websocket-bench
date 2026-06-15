# How to benchmark a WebSocket layer honestly

A field report on building the benchmarks behind [anycable.io/compare/nodejs-websocket](https://anycable.io/compare/nodejs-websocket). The repo is open source: [github.com/irinanazarova/anycable-socketio-benchmarks](https://github.com/irinanazarova/anycable-socketio-benchmarks). The page numbers are what bench-runners on Railway produce when you POST to them. Anyone can rerun the same endpoints and check the deltas.

This isn't a "benchmarks 101" post. The job is to compare three WebSocket stacks for production JS/TS apps (Socket.io default, Socket.io + Connection State Recovery, AnyCable, plus uWebSockets.js as a same-stack reference) on three questions:

1. Does the WebSocket layer deliver the messages it was told to send, including when the network blinks?
2. Does it survive deploys without losing or stalling user connections?
3. How many connections can a single instance hold?

Each probe carries a different kind of measurement trap. What follows is what we ended up with after enough rounds of "wait, that's not what we're actually measuring." We left the obvious bits short and spent more space on the WebSocket-specific surprises.

## The architecture in one diagram

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
                │   (Express)                                  │
                │   (50× shards for 1M-scale tests)            │
                └──────────────────────────────────────────────┘
                         ▲
                         │  HTTPS (5-min cap on edge proxy)
                         │  → bench-runner returns 202 {jobId}
                         │  → driver polls /jobs/:id
```

The driver is a CLI on the developer's laptop. The thing being measured is the WS layer (Socket.io, AnyCable, uWS). The bench-runner is the load generator. They all live in the same Railway project so traffic stays on the internal network: `*.railway.internal` resolves to private addresses with no NAT and no public-internet RTT contamination.

The shape that mattered most:

- **The thing being measured is not the thing measuring.** Sounds obvious. It bites you the moment you put 10K clients on the same box that's running the broker. Every dropped frame in the load generator becomes a dropped frame in your result and you can't tell which.
- **The driver is local, the work is remote.** The driver triggers tests and waits. It doesn't generate WebSocket load itself.
- **All clocks belong to the bench-runner.** `sentAt` is stamped by the bench-runner when it dispatches the publish. `receivedAt` is stamped by the bench-runner when the subscriber callback runs. Both timestamps come from one Node process, so latency arithmetic doesn't need cross-host NTP precision.

The interesting failure modes are below.

## Question 1: does it deliver during jitter?

This is the test that pushed us to rebuild our methodology twice.

### The disruption model

Each subscriber maintains a loop that, every ~15 seconds, force-closes the underlying TCP socket. Not a clean `socket.close()`, `socket.terminate()` if the WS implementation exposes it. That matters because Socket.io's behavior on `close()` is "graceful disconnect, send a leave packet"; the WiFi-drop case we care about gives the server nothing, and the client's TCP stack only learns of the loss when the next keepalive times out.

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

The reach into socket.io-client's private `io.engine.transport.ws` is unavoidable. The library doesn't expose the raw `ws` instance; if you don't reach for it, you can only ask for a graceful close, which isn't what we're modeling.

### The non-obvious surprise: different libraries have different "offline" windows

Here's the trap we walked into. The disruption loop says `terminate(); sleep(jitterDurationMs); ...`. A reader sees `jitterDurationMs = 1000` and assumes all four setups face the same 1 s offline window. They don't:

| Setup | Mechanism | Effective offline window per event |
|---|---|---|
| Default Socket.io | `reconnection: false`, the loop opens a fresh socket after `sleep(jitterDurationMs)` | sleep length sets it directly |
| Socket.io + CSR | `reconnection: true`, library's `reconnectionDelay: 2000` | ~2.0–5.0 s |
| AnyCable | `@anycable/core` Monitor backoff (~2 s baseline) | ~2.0 s |
| uWS | Custom reconnecting wrapper matching socket.io-client's backoff | ~2.0–5.0 s |

The default-Socket.io path is the outlier because we open a fresh socket ourselves after the sleep — the library never gets a chance to apply its backoff. The other three configurations let the client library handle reconnect, so the library's backoff dominates the offline window.

We fixed this with a floor in `lib/timing.ts`:

```ts
// Floor for the per-event offline window. Without it, default Socket.io's
// manual fresh-socket path would only be offline ~1 s while CSR / AnyCable /
// uWS sit at 2 s under their libraries' backoff. Pinning the same 2 s on
// every config makes the delivery-rate comparison reflect protocol
// differences, not reconnect-delay differences.
export const MIN_OFFLINE_MS = 2000;
```

This is the kind of fix that re-shapes the result. Before normalizing, default Socket.io showed ~27% delivery and CSR showed ~80% with a 100-second p95 tail. After normalizing, default Socket.io climbed to 84%, CSR climbed to 100%, and CSR's p95 fell to 1.97 s.

The story those old numbers were telling wasn't "Socket.io can't be reliable." It was "if you cram 10,000 reconnects into a 1-second window on a Railway shared-infra box, the server thrashes." That's a real failure mode, but it's a load-shedding story conflated with a delivery-protocol story. Equalizing the offline window separated them.

### What the headline numbers say now

10K subscribers, 120 broadcasts at 500 ms intervals, force-close every ~15 s, 2 s offline per event:

| Setup | Delivery | p50 (raw) | p95 | p99 | max |
|---|---|---|---|---|---|
| Default Socket.io | 84.55% | 106 ms | 394 ms | 1.07 s | 1.75 s |
| Socket.io + CSR | **100%** | 148 ms | 1.97 s | 4.58 s | 9.71 s |
| uWS topics | 87.03% | 92 ms | 722 ms | 1.72 s | 2.95 s |
| AnyCable OSS | **100%** | 250 ms | 4.10 s | 6.14 s | 9.23 s |
| AnyCable Pro | **100%** | 261 ms | 4.10 s | 6.15 s | 9.36 s |

The at-most-once protocols (default Socket.io, uWS) lose what landed during each offline window: about 15% for default Socket.io, 13% for uWS. Both replay protocols (CSR, AnyCable) deliver 100%, with CSR's tail slightly shorter at p99 in this in-memory setup. AnyCable's edge over CSR isn't tail latency at 10K; it's elsewhere — deploy resilience (separate Go process), horizontal scaling via NATS/Redis with replay intact, memory efficiency at 1M scale, native client-to-client whispers, and broader backend support (Ruby + Rails + Node + Bun + Deno vs Node-only).

### Latency arithmetic across clocks

A subscriber records `Date.now() - sentAt`. If `sentAt` was stamped on the publishing host and the subscriber is in a different process, you have wall-clock skew. Cross-region you'd need NTP; same-region same-rack you usually don't, but you can't assume zero.

The trick we use is **min-normalization**: after the run, find the minimum observed latency across all samples, subtract it from every sample, and report the normalized series alongside the raw. If the minimum is +18 ms (your publisher's clock is 18 ms ahead of the subscriber's), the normalized view shifts the floor to zero. The `skewFloor` is reported so a reviewer can spot when it's anomalously large.

```ts
// lib/stats.ts
const lmin = allLatencies[0]; // sorted ascending
const norm = allLatencies.map((v) => v - lmin);
// latencyRawMs and latencyOverMinMs both reported in the result
```

Why not pin both timestamps with `performance.now()` on a single host? Because the publisher and the subscriber are different processes by design — that's the whole point of "a separate WS server next to your app." Wall-clock with min-normalization gives a fair comparison without forcing single-process colocated subscribers, which would only measure something nobody runs in production.

### Aggregating across shards: percentile honesty

For tests that need more clients than one bench-runner container can produce, we fan out. The naive merge — averaging per-shard p99s — is wrong: it doesn't tell you anything about the union-distribution p99. So each shard returns a downsampled sorted-latency array (~5K samples preserving the distribution shape), the driver concatenates them, resorts, and recomputes percentiles over the merged view.

```ts
// lib/stats.ts
export function mergeJitterResults(label: string, shards: JitterResult[]) {
  const merged: number[] = [];
  for (const s of shards) for (const v of s.latencySamplesSorted!) merged.push(v);
  merged.sort((a, b) => a - b);
  // recompute p50/p95/p99 over merged
}
```

There's a subtle requirement here: each shard must have a chance to observe a "fast" sample so the global minimum subtraction stays close to zero for everyone. With our test windows (3–5 minutes, thousands of messages per shard) that's true in practice. If you ran 30-second shards you could see one shard's tail pulled down too far. The README's `Notes and caveats` calls this out.

### What we don't test (and why)

We don't model continuously-flaky networks where each client's disruptions overlap with their own reconnects. We test ~8 disruptions per client at ~15 s gaps. Adding more would surface library-specific reconnect-storm behavior (the kind of thing where 10K clients all retry at the same tick and DoS the server's accept queue), which is interesting but a different question. The page asks "what happens during normal WiFi blips?", not "what happens when the entire data center has a bad day?"

---

## Question 2: how does the WS layer survive a deploy?

This is where deployment architecture (in-process vs. separate process) shows up in the result.

### The avalanche test

5,000 connected clients. Bench-runner ramps the fleet to steady state, the operator triggers a deploy of the WS server (`railway redeploy -s socketio-server --yes`). The bench-runner watches for the first `disconnect` event — that's "deploy detected" — and measures how long until 95% of the fleet has reconnected.

What broke our first version: the deploy lands in milliseconds; our listeners weren't attached yet because we attached them after the ramp loop. Fix: attach `connect` / `disconnect` listeners at socket creation, gate them with a `tearingDown` flag during cleanup:

```ts
// lib/avalanche-runner.ts
// Listeners (connect / disconnect) are attached at socket creation —
// before the restart can possibly fire — so events are never lost in
// the gap between the SIGKILL/`railway restart` landing and the test
// process being ready to listen.
```

This is an obvious-in-hindsight bug. It's also the canonical first benchmark bug. Document it because a reviewer reading your code is looking for exactly this kind of trap.

### The standalone vs. embedded honesty

Vladimir, AnyCable's author, asked us a hard question early: when you compare "Socket.io with the WS layer in-process" against "AnyCable as a separate process," you're not comparing protocols, you're comparing deployment topologies. To be fair, you have to either test both in-process or test both as separate services.

We resolved this by adding both shapes:

- **In-process for Socket.io / uWS** — the default deployment for those libraries. The publisher runs inside the WS server's Node process via `/publish-local`. This is how a real team would deploy them.
- **Standalone for Socket.io / uWS** — a deliberately-separated publisher service that POSTs to the WS server's `/_broadcast` over HTTP. This is the shape we'd use in production if we wanted survivability. It's also the only fair comparison for AnyCable, which is always a separate process.

This isn't about "which is better." It's about giving the reader the comparison that matches their planned topology. The page calls each out.

### Publisher placement (the symmetry the user caught)

Our first design had AnyCable publishing over HTTP per-message (one POST per broadcast) and Socket.io publishing in-process via a bulk `/publish-local` kickoff (one POST, then N emits inside the server). That's not a comparable workload — one path is N HTTP requests, the other is one. After the right pushback ("if we're testing 100 broadcasts, all setups should do 100 broadcasts") we converted Socket.io and uWS to per-message HTTP publishing by default. The `/publish-local` path still exists as a diagnostic, isolating in-process fan-out cost when that's the question.

The general lesson: **the test shape needs to be the same across all setups.** The number of HTTP requests, the size of each, the path between publisher and broadcaster — symmetric across configurations or your comparison isn't a comparison.

### The structural difference the test reveals

A Socket.io server **is** your application — WebSocket connections live in the same Node process that handles HTTP. Restart the process (i.e., deploy) and every connection dies. AnyCable is a separate Go binary; your app broadcasts to it over HTTP. Your app restarts; AnyCable stays up; connections don't notice.

This shows up in the numbers as 5,000 / 5,000 disconnects for Socket.io vs. 0 / 5,000 for AnyCable. There's no clever measurement here, it's the architecture. The test exists to make the architectural consequence visible to a CTO who's deciding between deployment shapes.

### Rolling-deploy realism

For the Socket.io + Redis cluster test, we deploy the nodes sequentially (`railway redeploy -s socketio-server-redis-a --yes` then `... -b ...`). Clients land on whichever node Redis happens to send them to. Each client experiences exactly one disconnect-reconnect cycle in their lifetime, which makes our "first disconnect = deploy detected" heuristic correct for the current setup but would break in a 3-node rolling deploy where a client lands on node A, gets bounced to node B, and experiences two cycles. We haven't tested 3+ node rolling deploys yet; if you read the code expecting `deploy-impact-runner.ts` to handle that case, it doesn't.

---

## Question 3: how many connections can one instance hold?

This is the test that pushed us deepest into the infrastructure side of benchmarking.

### Why "open N connections and hold" is harder than it looks

A Linux container has a per-source-IP outbound port pool of ~64K. Practically, after kernel reservations, you get ~50K useful outbound ports. So a single load-generator container can open at most ~50K WebSocket connections to one target before it runs out of ephemeral ports. The connections aren't refused, they just can't be opened.

If you want to hold 1M idle WebSockets to one server, you need 20 source IPs minimum (1M / 50K), with some margin. The traditional workaround is kernel tuning (`net.ipv4.ip_local_port_range`, `net.ipv4.tcp_tw_reuse`, etc.), which: (a) requires root, (b) is platform-specific, (c) means your "benchmark" is now also a kernel-tuning exercise nobody will reproduce.

Our path: **deploy 50 separate bench-runner containers on Railway**. Each is its own Linux container with its own source IP, its own ~50K outbound port pool. A coordinator script (`bench/idle-multi.ts`) fans out POSTs to all 50 in parallel. Each shard handles ~20K connections; the union is ~1M.

```bash
SHARDS=https://bench-runner-1.up.railway.app,https://bench-runner-2.up.railway.app,...
PER_SHARD_N=20000 HOLD_SEC=120 RAMP_PER_SEC=200 \
  npm run bench:idle:multi
```

This sidesteps kernel tuning entirely. The cost: 50 Railway containers cost something to run. We downsize them between test sessions (`railway` GraphQL `serviceInstanceLimitsUpdate`) to ~64 MB each so the standing cost is negligible. The README has the recipes.

### Railway's 5-minute HTTP edge cap

Railway's edge proxy caps inbound HTTP at 5 minutes. A 50K-subscriber ramp + 120 s hold takes ~7 minutes per shard, past the cap. The synchronous request would die mid-test.

Solution: a job queue. The bench-runner has `?async=1` on every endpoint. POST returns 202 with `{jobId}` immediately; the work runs in the background; the coordinator polls `GET /jobs/:id` every 5 s and gets `{status, logTail, result?}`. Each request is sub-second, so the edge cap never matters.

```ts
// bench-runner/server.ts
async function respondAsync(req, res, run) {
  if (req.query.async === "1") {
    const jobId = startJob(run);
    res.status(202).json({ jobId });
    return;
  }
  // sync fallback for short tests
  res.json(await run());
}
```

This pattern generalizes — any long-running operation behind an edge proxy with a deadline. It also lets the multi-shard coordinator stream per-shard progress to one terminal: each shard's `logTail` flows back through the poll, and a small diff-the-log helper prints only the new lines.

### What "filled to capacity" means

At 1M connections, anycable-go-pro held 999,954. Our box was 32 GB; peak memory was 19.34 GB. CPU peaked at 9.4% of 32 vCPU. The limit wasn't memory or CPU; there isn't a clean number for what stopped us at 999,954 vs 1,000,000. Three possible explanations: (a) one shard lost a few connections to handshake races during ramp, (b) the server side dropped a handful during settle, (c) measurement noise. We didn't track it down because for the question being asked — "does it fit in one instance?" — 999,954 / 1,000,000 = 99.995% answers it.

The OSS variant hit 993,994 / 32 GB peak. That **is** the ceiling: 32 GB is the box, and anycable-go OSS uses ~33 KB per connection. There's no headroom; the test stopped because the kernel started OOM-thinking. anycable-go-pro at 19 KB/conn has 13 GB of headroom; it could hold more on a bigger box.

For Socket.io at 119,826 on the 32-vCPU box, the limit is the **Node event loop**. Memory and CPU were both nearly idle on average; one CPU core was saturated handling WS handshakes serially. This is the architectural ceiling — single-threaded JS — that puts Socket.io into a different scaling category than the Go-based options, regardless of how much RAM you throw at it.

These three failure modes — RAM ceiling, Go-runtime soft ceiling, single-threaded JS ceiling — are exactly what the test is here to find. The numbers are only useful if you can name what stopped them.

---

## Things we got wrong on first attempts

### Mixing offline window with reconnect-storm capacity

This is the lesson that flipped the page's narrative. Our first jitter test had a 1 s offline window, and default Socket.io was delivering 27%, CSR was delivering 80%, AnyCable was delivering 100%. The headline read "CSR replays 80% with a 100-second tail; AnyCable replays 100% in 6 seconds."

That story was wrong, or rather it was telling two stories layered on top of each other. The 100-second CSR tail wasn't the CSR protocol's behavior, it was the symptom of 10K clients all reconnecting within a 1-second window while the server's accept queue serialized through a Node event loop. With a 2-second window (added via `MIN_OFFLINE_MS`), CSR's true behavior surfaced: 100% delivery, 4.6 s p99 tail. AnyCable's stayed 100% / 6.1 s. The two protocols are roughly equivalent on delivery and tail; AnyCable's wins are architectural.

If your benchmark gives you a dramatic-looking result, ask what other variable might be coupled to the one you think you're testing. Equalize across that variable. If the result holds, it's real. If it changes shape, you were measuring something different than the headline claimed.

### "100 subscribers × 10K messages" for throughput

Our initial throughput test had 100 subscribers receiving 10K messages each. The right pushback came: "real-world is more like 10K subscribers and 100 messages." Same 1M deliveries either way, but vastly different load profile.

- **100 × 10K** stresses per-socket throughput. Bottlenecks live in TCP send buffers, per-connection ring buffers, and per-subscriber serialization.
- **10K × 100** stresses fan-out. Bottlenecks live in the broker's subscriber index, in iteration over the subscriber set, in concurrent socket writes.

For "a chat app with 10K concurrent users seeing each other's messages," the fan-out shape is right. We switched.

### "AnyCable is slower at throughput!"

We saw AnyCable under-perform uWS on raw throughput and assumed something was wrong with the test. After several rounds — including switching to NATS as the broker transport — Vladimir reproduced the test with his own in-process `benchi` binary. That eliminated the network hop entirely and showed the gap was real for our config (a single HTTP publisher feeding anycable-go's broadcaster). We added Vladimir's binary to our bench-runner image and exposed it as an endpoint, so the page can report both "production-shape over HTTP" and "in-process ceiling" numbers side by side.

Lesson: when the result seems wrong, the answer might be "your test is right and your assumption was wrong." Build the simplest possible second measurement that brackets the question from a different angle.

---

## The honesty-loop: rebaseline

Numbers on the page rot. A patch release that improves things or a Railway scheduler change that worsens them silently makes your headlines lie. The protection is the `bench:rebaseline` script: walk a manifest of 24+ tests (every claim on the page is one entry), hit each bench-runner endpoint, diff against the recorded baseline, fail loudly when a number drifts past its threshold.

```
$ BENCH_RUNNER_URL=https://bench-runner-production.up.railway.app \
    npm run bench:rebaseline

  jitter-anycable-oss-10k  ✓ delivery=100% p99=6141ms (baseline 6200ms)
  jitter-socketio-csr-10k  ✓ delivery=100% p99=4575ms (baseline 4575ms)
  jitter-socketio-10k      ✓ delivery=84.55% p95=394ms (baseline 84% / 450ms)
  throughput-anycable-pro  ⚠ p99=7057ms (baseline 3927ms, +80% — within widened threshold)
```

Per-run history lives at `tmp/v1.6.14-bench-results/runs/{ISO-ts}/` so you can plot trend lines and see "did this drift from a recent baselining run or was it like this six months ago?" Heavier categories (idle, avalanche) are gated behind opt-in flags because they take >30 min each.

The rebaseline is the load-bearing trust mechanism. If a reviewer challenges a number, the answer isn't "trust the page", it's "rerun the manifest and show me the delta." That's the only honest way to keep an open-source benchmark from rotting in place.

When a drift threshold flags a test that's actually within the test's intrinsic variance, widen the threshold and add a comment to the manifest explaining why. (Example: `throughput-anycable-pro` p99 swings 3-7 seconds between consecutive runs at 100 msg/s × 10K subs because anycable-go GC pauses dominate the tail at that fanout rate. The threshold is 50% with a comment saying so.) A noisy false-positive is worse than no signal because it teaches people to ignore the report.

---

## Methodology disclosures

The README's "Notes and caveats" section documents the choices a reviewer might otherwise want to challenge:

- CSR was benchmarked with the default in-memory adapter, not Redis Streams. Production CSR setups would typically use Redis Streams for restart survivability and durability; that adds network RTT to the replay tail but doesn't change the structural picture (CSR delivers, with a sub-10 s tail).
- Both Socket.io and AnyCable run with WebSocket-only — no long-polling fallback. Like-for-like transport.
- AnyCable uses the in-memory broker for these tests. Production runs typically use NATS or Redis to survive restarts and run multi-node; the in-memory broker is the simplest opt-in.
- Latency clock skew is handled by min-normalization. The headline replay-latency numbers on the page use the min-normalized view.
- For cross-shard runs, min-normalization assumes each shard observes at least one near-zero sample within its window. True at our 3-5 minute test windows; potentially not true at sub-60-second shard durations.
- Jitter timing, reconnect jitter, and whisper stagger use unseeded `Math.random()`. Runs reproduce statistically but not bit-for-bit. Per-run history captures the spread.
- Default Socket.io's per-event offline window is floored to `MIN_OFFLINE_MS=2000` so the four configurations face the same disruption shape. The other three sit at ~2 s naturally because their libraries' reconnect backoff dominates.
- Delivery rate denominator includes clients that failed to connect (`deliveryRatePct`), with `deliveryRateOfConnectedPct` exposed in the result JSON so a connect-failure run isn't silently capped below 100%. In healthy runs (`connectFailures: 0`) the two numbers are identical.

These aren't asterisks. They're the surface where someone with more experience than us can push back productively.

---

## What "honest" means here

We don't claim AnyCable wins everything. uWS holds more connections per GB (5.4 KB / conn) than anycable-go-pro (19 KB / conn) at 1M scale; we say that on the page. CSR's protocol works at the offline windows real users experience, and its tail is comparable to AnyCable's in the in-memory comparison. We didn't bury that result when the data flipped.

What we claim is: every number was produced by running this code on Railway, in the same project, over the test windows recorded in `tmp/v1.6.14-bench-results/`. If you find a methodology flaw, open an issue. If you rerun and get different numbers, open an issue. The page is wrong by construction the moment we stop being able to reproduce its numbers, and the rebaseline manifest is what keeps us honest about that.

The four rules that survived this build:

- Run the comparator's library at its best, not at its weakest.
- Use the comparator's default settings unless you have a reason to disagree, and document the reason.
- Publish the code, the parameters, and the raw results, not just the chart.
- Rebuild the test the moment someone shows you a fair criticism.

The rest is plumbing.

---

*Built by the AnyCable team. Code at [github.com/irinanazarova/anycable-socketio-benchmarks](https://github.com/irinanazarova/anycable-socketio-benchmarks). Page at [anycable.io/compare/nodejs-websocket](https://anycable.io/compare/nodejs-websocket).*
