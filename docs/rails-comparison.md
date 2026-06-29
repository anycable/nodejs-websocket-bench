# Rails in the comparison: Action Cable vs Solid Cable vs Async::Cable vs AnyCable

The repo behind [anycable.io/compare/rails-actioncable](https://anycable.io/compare/rails-actioncable).
Four WebSocket adapters for the same Rails app, same Railway box, same
shared-tenant window:

- **Action Cable** on the Redis adapter (the Rails default). WebSockets
  terminate in-process inside Puma's Ruby threads; Redis carries pub/sub.
- **Solid Cable** (Rails 8 default-stack adapter). Same in-process
  termination, no Redis; the adapter polls a database table (default
  every 100 ms) for new messages.
- **Async::Cable** ([socketry/async-cable](https://github.com/socketry/async-cable)):
  serves Action Cable in-process on [Falcon](https://github.com/socketry/falcon),
  a fiber-based reactor, instead of Puma's threads. Same wire protocol; a
  different concurrency engine. Built on `actioncable-next`, so it runs on
  stable Rails 8.1.
- **AnyCable** in full RPC mode: Rails runs a gRPC backend
  (`bundle exec anycable`) for auth and commands, and `anycable-go`
  (a separate Go process) holds the WebSockets and fans out broadcasts.

The whole point of the comparison: all four are **Action Cable-compatible
at the app and client level**. The channel code, the `stream_from` calls,
and the Turbo Stream broadcasts are identical in all four. Only one line
of `config/cable.yml` and the process topology change. So any behavioral
difference is the adapter, not the app.

## The one thing that differs on the wire

Action Cable, Solid Cable and Async::Cable speak the base
`actioncable-v1-json` protocol, which is **at-most-once**: a broadcast that
goes out while a client is briefly offline is gone, because there is no
history to replay on reconnect. AnyCable speaks the extended
`actioncable-v1-ext-json` protocol, which keeps **per-stream history with an
epoch and offset**, so a reconnecting client resumes from the last message
it saw. The AnyCable JS client negotiates the extended protocol
automatically; a plain Action Cable client still connects over the base
protocol. That single difference drives the reliability gap below.

Note Async::Cable: switching Puma threads for Falcon fibers changes the
runtime, not the guarantees. It is still in-process (drops on deploy) and
still speaks the base protocol (loses messages under jitter, same as the
others). Falcon does not close the reliability or deploy gap, because those
gaps come from the protocol and the topology.

## Bench harness

No new bench-runner endpoints. The AnyCable JS driver (`@anycable/core`)
already speaks both protocols, so the existing `bench-jitter-anycable`,
`bench-idle-anycable`, and `bench-avalanche-anycable` endpoints serve all
four Rails targets, parameterized by:

- `cableUrl`: the `ws://…/cable` endpoint per target.
- `broadcastUrl`: the Rails `POST /_bench/broadcast` trigger
  (`ActionCable.server.broadcast`), or anycable-go's `/_broadcast`.
- `channel`: `BenchmarkChannel` (real Rails channel), vs `$pubsub` for
  the bare-stream nodejs targets.
- `acProtocol`: `actioncable-v1-json` for Action Cable / Solid Cable /
  Async::Cable, `actioncable-v1-ext-json` for AnyCable.

The Puma/Redis and Solid Cable targets live in `cable-bench/` (named
`cable-bench` because `rails` is a reserved Railway app name): one Docker
image, two modes via `BENCH_MODE`. AnyCable runs the same app as its gRPC
RPC backend with `anycable-go-rails` as the gateway. Async::Cable runs from
`cable-bench-falcon/`, a copy of the app booted on Falcon
(`bundle exec falcon serve`) with `actioncable-next` + `async-cable`.
Manifest entries are in `backend/src/bench/tests-manifest.ts` under each
rubric.

### Sharding is mandatory for latency and jitter

All numbers below are **sharded**. A single bench-runner holding thousands
of `@anycable/core` cables saturates its own Node event loop and distorts
the measurement in both directions: receive latency inflates and delivery
deflates, worst for the extended-protocol AnyCable client because it does
more per-message work. So latency, jitter, and capacity runs are sharded
across 13 drivers (~250 to 770 cables each), keeping each driver well under
the saturation point; the percentiles are merged across the union of all
samples via `mergeJitterResults`. See the bench repo README, "Running the
latency (and jitter) test."

## Results

All numbers from sharded runs in one shared-tenant Railway window
(2026-06-28), captured in
`backend/results/rails-sharded-2026-06-28.json`. The in-process targets ran
Puma with 8 workers × 5 threads; Async::Cable ran Falcon with 8 processes.

### Roundtrip latency (steady network, 100% delivery)

AnyCable is fastest at both sizes: fan-out happens in the Go gateway rather
than in Ruby, so it stays at single-digit milliseconds where the in-process
adapters climb with load. Action Cable on Puma is close behind; Async::Cable
on Falcon sits a touch higher; Solid Cable carries a fixed floor from its
100 ms database poll.

| Adapter | 1K p50 / p99 | 5K p50 / p99 |
| --- | --- | --- |
| Solid Cable | 62 / 119 ms | 74 / 164 ms |
| Async::Cable (Falcon) | 11 / 80 ms | 20 / 71 ms |
| Action Cable (Puma) | 9 / 47 ms | 13 / 57 ms |
| AnyCable | **4 / 23 ms** | **7 / 31 ms** |

### Delivery under jitter (5K subscribers, ~2 s drops every ~15 s)

The strongest AnyCable win. The base protocol has no resume, so whatever
lands during an offline window is lost. All three in-process adapters behave
identically here, because the loss is in the protocol, not the runtime.

| Adapter | Delivery | p50 | p99 (replay tail) |
| --- | --- | --- | --- |
| Solid Cable | **78.1%** | 71 ms | no resume |
| Action Cable | **78.1%** | 12 ms | no resume |
| Async::Cable (Falcon) | **78.1%** | 18 ms | no resume |
| AnyCable | **99.9%** | 7 ms | ~6.0 s |

AnyCable's longer p99 is the resumed history landing a beat late on
reconnect. It lands; the in-process adapters drop about a fifth of
broadcasts outright.

### Capacity: 10K under load, and idle-to-break

In-process Action Cable has a reputation for collapsing under load. With a
properly sharded load generator, it does not at everyday sizes.

**Under broadcast load, all four hold 10K at 100%.** AnyCable keeps the
tightest tail; Solid Cable's poll shows in its p99.

| Adapter | 10K subs (delivery / p99) |
| --- | --- |
| Solid Cable | 100% / 200 ms |
| Action Cable | 100% / 84 ms |
| Async::Cable (Falcon) | 100% / 112 ms |
| AnyCable | **100% / 31 ms** |

**Idle-to-break, on identical 32 GB boxes, default 8-worker config.** Ramped
idle connections until the holding box failed (raw data in
`backend/results/rails-capacity-break-2026-06-28.json`):

| Adapter | Max held (0 fail) | Peak RAM | What capped it |
| --- | --- | --- | --- |
| Action Cable (Puma) | ~52K | 2.3 GB | 8-worker file-descriptor ceiling |
| Solid Cable (Puma) | ~52K | 2.5 GB | 8-worker file-descriptor ceiling |
| Async::Cable (Falcon) | ~97K | 27 GB | memory, ~290 KB/conn |
| AnyCable (Go gateway) | **600K+** | 27 GB | not broken; load fleet maxed out |

Three findings. The Puma adapters wall at ~52K using only ~2.5 GB: an
8-worker file-descriptor ceiling, not memory (raising worker fd limits
lifts it). Async::Cable on Falcon is memory-bound at ~97K, because each
fiber-backed connection costs roughly 290 KB, about six times the others.
AnyCable held 600K idle connections with zero failures across a 50-runner
fleet before we ran out of load generators; its gateway memory scaled
linearly to 27 GB (~47 KB/conn, the same per-connection cost as Puma),
about 84% of the box, so its real ceiling is near ~700K and memory-bound.
The per-connection RAM is similar to Puma's; what differs is that one Go
process has no per-worker fd wall, so it uses the whole box instead of
stalling at 52K with 90% of the box idle.

Finding a gateway's true ceiling takes roughly one load driver per 10K
connections: a single Node driver tops out around 10K cables, so reaching
600K needed the full 50-runner fleet. With fewer drivers the fleet caps the
result before the server does, so capacity-to-break runs scale the driver
count to the target.

### Deploy survival (avalanche, 5K clients, real app redeploy)

In-process WebSocket servers drop every connection when the app restarts,
Puma and Falcon alike. On our test the in-process adapters went fully dark
on the redeploy and stayed down for roughly 7.5 to 8 seconds before about
96% of clients reconnected. For AnyCable the redeploy hit the Rails gRPC
backend, not the gateway holding the sockets, so its connections were never
dropped: zero seconds of downtime.

The "Down for" column below is how long connections stayed dropped after the
redeploy before the reconnect storm settled.

| Adapter | Dropped | Down for | Reconnected |
| --- | --- | --- | --- |
| Action Cable | all 5,000 | 7.5 s | 96.3% (187 still out at cutoff) |
| Solid Cable | all 5,000 | 7.6 s | 95.7% (215 still out at cutoff) |
| Async::Cable (Falcon) | all 5,000 | 8.0 s | 96.4% (179 still out at cutoff) |
| AnyCable | **0** | **0 s** | n/a |

The AnyCable run redeployed its Rails RPC backend the same way; the gateway
reported no disconnect across the full 180 s observation window, so the
downtime is zero by construction.

## The takeaway

On Rails, AnyCable leads on latency at every scale (7 ms p50 at 5K vs 13 ms
for Action Cable, 20 ms for Async::Cable, 74 ms for Solid Cable), because
fan-out runs in Go off the Ruby process. It wins everything that decides
whether realtime holds up in production: 100% delivery under jitter where
the base protocol drops about a fifth of broadcasts, and every connection
kept alive across an app deploy where the in-process adapters drop all of
them. Capacity ties at everyday sizes (all four hold 10K at 100%) but splits
sharply past that: AnyCable held 600K idle connections where the Puma
adapters wall at ~52K on a file-descriptor ceiling and Falcon runs out of
memory at ~97K. Async::Cable on Falcon is a real alternative runtime to
Puma with latency and capacity in the same range, but it shares the
in-process limits: at-most-once and deploy-fragile. Solid Cable's own edge
is operational: no Redis, just the database, at the cost of a fixed
polling-latency floor. All four keep your channels and Turbo Streams exactly
as written.
