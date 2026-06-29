# socketioxide in the comparison

[socketioxide](https://github.com/totodore/socketioxide) is a Rust
implementation of the Socket.io v4+ protocol. The library's author asked us
to include it in the benchmark, and it's a good fit: same wire protocol as
the Node Socket.io server we already test, so the bench-runner's existing
`socket.io-client` driver works against it unchanged. The work is server-side.

This page tracks what we added, what the open questions are, and what the
numbers look like once we have them.

## Status

- **Server scaffold:** `socketioxide/` in this repo. Cargo project plus
  Dockerfile that mirrors the shape of `backend/src/socketio/server.ts`:
  `/health`, `/stats`, `/_broadcast`, `/publish-local`. **Compiles and
  runs** (release build against `socketioxide 0.18.4`); validated locally
  against the bench-runner's socket.io-client driver (see Results).
- **Crate pinned to `socketioxide = 0.18`**, resolves to `0.18.4`.
  Features: `v4` for the Socket.io v4 wire protocol the bench-runner
  speaks, `tracing` for logs, `state` for the connection counter behind
  `/stats`. Maintainer was actively pushing engineio hardening fixes on
  the day this branch landed; bump the pin in tandem with the next tagged
  release if those fixes have shipped by the time you deploy at scale.
- **Bench-runner endpoints:** none new. The Rust server speaks the
  Socket.io wire protocol, so the existing `bench-jitter-socketio`,
  `bench-idle-socketio`, and `bench-avalanche-socketio` endpoints all
  accept it via `?serverUrl=...`.
- **Manifest entries:** added under each rubric in
  `backend/src/bench/tests-manifest.ts`. Baselines are empty until the
  first run lands.
- **Railway service:** one to deploy from this directory:
  `socketioxide-server`. Same hardware tier as the other Socket.io
  targets so the comparison stays apples-to-apples. The CSR variant is
  deferred (see open questions).

## Open question that gates the CSR comparison

The Socket.io + CSR row of the page tests `connectionStateRecovery`:
on disconnect, the server stashes packet state and a per-socket pid; on
reconnect, the client sends pid + last-offset and the server replays.
Node Socket.io supports this since 4.6.

**Does socketioxide ship CSR?** As of `0.18.3` it does not appear to.
The crate's `Cargo.toml` features list (`v4`, `msgpack`, `tracing`,
`extensions`, `state`, `__test_harness`) has no CSR-shaped flag; the
README and examples don't mention session resume; the `state` feature
is for application-level shared state, not session state.

So we deferred the CSR variant. If the library author confirms CSR is
on the roadmap (or already present under a different name we missed),
we'll add `socketioxide-server-csr` as a second service and the
`-csr` manifest entries. Until then, socketioxide is tested only in
its at-most-once mode and reported next to default Socket.io / uWS.

## Why this strengthens the comparison

The page's architectural claim is that at-most-once delivery losses under
jitter and the in-process avalanche on deploy are properties of the
deployment shape, not of any particular implementation language. Adding a
Rust at-most-once Socket.io server lets us test that claim across runtimes.
Three outcomes are possible, all useful:

- **Same shape, same numbers.** Rust loses ~16% under jitter, all
  connections die on deploy. Confirms the claim: it was always about
  topology, never the runtime.
- **Same shape, better numbers.** Rust event loop holds more connections
  before the cliff (avalanche pushes out from 25K to 50K, say), but still
  goes to zero at deploy. Refines the claim: implementation slows the
  cliff, doesn't move it.
- **Different shape.** If socketioxide ships CSR on by default or stays up
  through a deploy somehow, the comparison gets more interesting and the
  page narrative gets richer.

Either way, the result is a stronger page, not a weaker one.

## Rubrics + manifest IDs

| Rubric | socketioxide (at-most-once) |
|---|---|
| Latency 1K | `latency-socketioxide-1k` |
| Latency 10K | `latency-socketioxide-10k` |
| Jitter 10K | `jitter-socketioxide-10k` |
| Idle 1M target | `idle-socketioxide` |
| Avalanche 5K | `avalanche-socketioxide-5k` |
| Avalanche 10K | `avalanche-socketioxide-10k` |
| Avalanche 20K | `avalanche-socketioxide-20k` |

Whispers and throughput entries can be added the same way once we have
the latency + jitter numbers and the server config holds up.

## Open questions for the library author

Tagged in the GitHub issue:

1. **Connection State Recovery roadmap.** Does socketioxide support
   server-side session resume (pid + last-offset replay) today? If yes,
   how is it enabled (feature flag, builder config)? If no, is it on
   the roadmap? Answer decides whether we add a `-csr` variant or skip
   the CSR rubric for this server.
2. **Production-shaped publisher.** Our `/_broadcast` handler calls
   `io.to(stream).emit(...).await` on every POST. That's the natural
   shape but might not match the recommended pattern for socketioxide
   at high broadcast rates. Open to a PR that swaps it.
3. **Default config knobs.** Anything we should tune for the
   comparison to be fair? Compression, ping intervals, buffer sizes.
   Defaults here match the Node Socket.io server's bench config; happy
   to take a config patch.
4. **Engineio hardening.** Several `fix(engineio): ... malicious /
   malformed packet` commits landed on 2026-06-20. Should we pin to a
   commit past those rather than the `0.18.3` tag? Or wait for the
   next release that bundles them?

## Results

### Local head-to-head, socketioxide vs AnyCable (2026-06-23)

First real run. socketioxide `0.18.4` (release build) against `anycable-go`
1.6.14, both on one machine, 200 clients, per-message HTTP publishing for
both. AnyCable runs in the same window as a control: its expected shape
(100% delivery, multi-second replay tail under jitter) confirms the
environment was sound, so the socketioxide numbers aren't an artifact of a
bad local moment. This is small-scale local, not the Railway 10K headline
setup; treat it as "the harness works and the architectural shape holds",
not as a published page number.

**Latency (jitter disabled), 200 clients, 100 messages at 5/sec:**

| | Delivery | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| socketioxide | 100% | 4 ms | 12 ms | 18 ms | 27 ms |
| AnyCable (control) | 100% | 5 ms | 12 ms | 22 ms | 29 ms |

Roundtrip latency is the same order on both. Nothing separates a Rust
Socket.io server from AnyCable when the network is steady.

**Delivery under jitter, 200 clients, TCP force-close every ~15 s:**

| | Delivery | Lost | p50 | p95 | p99 | max |
|---|---|---|---|---|---|---|
| socketioxide | **91.6%** | 928 | 4 ms | 12 ms | 16 ms | 26 ms |
| AnyCable (control) | **100%** | 0 | 7 ms | 3.6 s | 5.6 s | 8.5 s |

This is the architectural result the page argues for, reproduced across a
fourth runtime. socketioxide is at-most-once: it has no replay, so the
broadcasts that land during a client's offline window are gone (~8% lost
here). The messages it *does* deliver are fast. AnyCable delivers 100%
because it replays the missed range on reconnect, which is what the
multi-second p95/p99 tail is: late-but-delivered messages. The Rust
implementation lands in the same at-most-once band as default Socket.io
and uWS. The delivery gap is about the protocol (replay vs none), not the
language.

Raw numbers: `backend/results/socketioxide-local-2026-06-23.json`.

### Railway run @ 10K, socketioxide vs AnyCable OSS (2026-06-23)

Phase 1 on the real infra: `socketioxide-server` deployed to the same
Railway project as the page targets, `anycable-go` OSS woken alongside as
the same-window canary, both driven from the Railway-hosted bench-runner
over the internal network. AnyCable held its expected shape on every test
(latency in band, 100% delivery under jitter, 100% throughput), so the
window was healthy and the socketioxide numbers are not Railway noise.

**Latency (jitter disabled):**

| Scale | socketioxide p50 / p99 | AnyCable OSS p50 / p99 | Both delivery |
|---|---|---|---|
| 1K | 23 / 66 ms | 16 / 46 ms | 100% |
| 10K | 289 / 972 ms | 232 / 731 ms | 100% |

Same order of magnitude. AnyCable is a touch faster at the tail; nothing
separates them in a way a user would feel. socketioxide delivers 100% when
the network is steady.

**Delivery under jitter (TCP force-close every ~15 s, no replay protocol):**

| Scale | socketioxide delivery | AnyCable OSS (canary) |
|---|---|---|
| 200 (local) | 91.6% | 100% |
| 1K (Railway) | 89.4% | 100% |
| 10K (Railway) | **40.6%**, then **32.7%** (two runs) | 100% |

This is the headline finding. socketioxide sits in the at-most-once band
with default Socket.io (~85%) and uWS (~87%) up to 1K, then **collapses
under the 10K reconnect storm**: two independent runs landed at 41% and
33% delivery. The cliff is reproducible and is not a server crash (no
errors logged, 0 connect failures, 10K/10K clients connect every time)
and not Railway noise (AnyCable held 100% in the same windows).

Two things compound at 10K. socketioxide is at-most-once, so anything that
lands during a client's offline window is gone. And the jitter path opens
a fresh connection per disruption (the standard Socket.io recovery, since
the protocol has no resume), so 10K clients churning ~7 reconnects each
is ~70K fresh handshakes against one in-process server. The Rust runtime
does not rescue the architecture: at scale, an in-process at-most-once WS
layer sheds most of its messages during a reconnect storm. AnyCable holds
100% because the WS layer is a separate process the disruption never
restarts, and replay recovers whatever the offline window missed.

Caveat for fairness: AnyCable's jitter path reconnects the same cable
in place (its client library's built-in resume), which is lighter than
socketioxide's fresh-socket-per-event path. Part of the 10K gap is that
asymmetry, which is itself a property of having a resume protocol versus
not. The 1K row (89% socketioxide, same fresh-socket path) shows the
mechanism is sound at moderate scale; the 10K row shows where it breaks.

Raw numbers: `backend/results/railway-phase1/` (per-test JSON) and
`backend/results/socketioxide-railway-2026-06-23.json` (summary).

### Idle connection capacity @ ~600K (phase 2, 2026-06-23)

50-shard fleet woken, both targets sized 32 GB / 32 vCPU, each ramped
toward 1M idle socket.io/cable connections.

**The harness capped before either server did.** Both socketioxide and
anycable-go held ~600K and then the bench-runner shards ran out of
client-side capacity (~12K socket.io/cable clients per shard, well under
the ~50K port limit, so the shards' own event loops / memory were the
wall). The near-identical ceiling (600,091 vs 600,084) is the tell: it's
a property of the load generator, not the servers. Neither target
saturated; both sized 32 GB peaked at ~21-22 GB with ~10 GB headroom.

| At ~600K held | Peak memory | RAM / conn | Peak CPU |
|---|---|---|---|
| socketioxide | 21.4 GB | ~37 KB | 1.8% |
| anycable-go OSS | 22.1 GB | ~39 KB | 9.0% |

Two things worth stating. First, per-connection memory is comparable:
~37 KB (socketioxide) vs ~39 KB (anycable-go), both far below Node
Socket.io's ~52 KB. Second, and more telling: **socketioxide held 600K+
idle connections, about 5x past Node Socket.io's ~120K ceiling.** Node
Socket.io caps there because handshakes serialise through one event loop
regardless of memory; socketioxide's multi-threaded tokio runtime clears
that wall. So the Rust implementation fixes Socket.io's idle-capacity
problem (a runtime/concurrency limit) even though it cannot fix the
at-most-once delivery problem (a protocol limit). The two ceilings have
different causes, and only one of them is about the language.

Caveat: 600K is harness-limited, so this is a floor on each server's true
capacity, not the ceiling. The RAM/conn figures include the memory churn
of ~400K failed connection attempts hitting each target during ramp, so
treat them as approximate upper bounds. To find the real server ceilings
we would need larger bench-runner shards or more of them.

**Pushing toward 1M (and why we stopped at the harness).** We traced the
600K cap to a hard per-shard limit of ~12,002 connections to a single
`host:port` from one source IP (ephemeral-port exhaustion, not memory:
the bench-runner containers report `nofile=122880`). 50 shards x 12K =
600K, exactly the cap. To go higher we grew the fleet to 85 shards
(~1.02M of theoretical capacity) and re-ran. The expanded fleet got
flaky: 49 of 85 shards delivered a clean 12,000 each (588,000 total, 0
failures on those) while 36 shards errored or timed out under the
coordinator's fan-out. So the load generator, not socketioxide, remained
the wall, and we did not land a clean 1M. socketioxide itself never
showed stress: it accepted every connection the surviving shards threw
(588K, 0 failures) and stayed well under its memory limit.

What this establishes: socketioxide comfortably holds **at least ~600K**
idle socket.io connections on a 32 GB box with headroom to spare, roughly
5x past Node Socket.io's ~120K event-loop ceiling, at per-connection
memory comparable to AnyCable. We could not measure its true ceiling
because reaching 1M needs a more capable load-generation fleet (more
source IPs, or a lighter idle client than socket.io-client). That is a
harness limitation, and the page does not claim a socketioxide idle
ceiling on the strength of it.

Raw: `backend/results/railway-phase2/idle-*.json`.

### Avalanche (deploy survival) @ 5K / 10K / 20K

Each scale ramps N socket.io clients against `socketioxide-server`, then
a real `railway redeploy` swaps the container mid-test (same methodology
as the Socket.io avalanche ladder). socketioxide tracks Node Socket.io's
cliff almost exactly:

| Clients | Reconnected | Recovery | Never back |
|---|---|---|---|
| 5,000 | 100% | 2.9 s | 0 |
| 10,000 | 96% | 67 s | 411 |
| 20,000 | **0%** | never (capped at 10 min) | all |

On the deploy, every connection drops (in-process WS dies with the app,
the architectural fact). At 5K it recovers cleanly. By 10K the reconnect
storm against the freshly-restarted single instance stretches recovery to
over a minute with a few hundred clients never returning. By 20K it
collapses to 0% recovered, the same cliff Node Socket.io hits around 25K
on the page (Socket.io 10K was ~65 s / 96%, near-identical to
socketioxide's 67 s / 96%). The Rust runtime does not change the shape:
an in-process WS layer cannot survive its own app's deploy, and the
reconnect storm overwhelms the new instance at scale regardless of
language. AnyCable's avalanche row is 0 s by construction: the WS process
is never restarted by an app deploy.

(The 20K row is muddied by the client side: one bench-runner caps near
~12K socket.io clients, so the 20K avalanche only fully ramped ~12K. The
0% recovery is unambiguous either way.)

Raw: `backend/results/railway-phase2/avalanche-socketioxide-*.json`.

### How phase 1 was deployed

`socketioxide-server` is a net-new Railway service built from
`socketioxide/`. Three things the local build didn't catch surfaced on
Railway and are fixed in the Dockerfile / server:

- Base image must be Rust 1.94+ (socketioxide 0.18.4 MSRV); `rust:1-slim`.
- Bind `[::]` not `0.0.0.0`: Railway's private network is IPv6, so an
  IPv4-only bind is unreachable internally.
- Pin `PORT=3000` on the service so the listen port matches the manifest
  target (Railway injects `PORT=8080` otherwise).

### Reproduce the local run

```bash
# Terminal 1 — socketioxide
cd socketioxide && cargo run --release      # :3000

# Terminal 2 — anycable-go
anycable-go --port 8080 --broker=memory --presets=broker --public

# Terminal 3 — jitter, both, same window
cd backend
SOCKETIO_URL=http://localhost:3000 NUM_CLIENTS=200 DURATION=90 \
  TOTAL_MESSAGES=60 INTERVAL_MS=500 JITTER_INTERVAL=15 JITTER_DURATION=1000 \
  npm run bench:jitter:socketio
ANYCABLE_URL=ws://localhost:8080/cable BROADCAST_URL=http://localhost:8090/_broadcast \
  NUM_CLIENTS=200 DURATION=90 \
  TOTAL_MESSAGES=60 INTERVAL_MS=500 JITTER_INTERVAL=15 JITTER_DURATION=1000 \
  npm run bench:jitter:anycable
```
