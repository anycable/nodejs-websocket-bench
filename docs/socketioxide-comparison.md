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
  `/health`, `/stats`, `/_broadcast`, `/publish-local`.
- **Crate pinned to `socketioxide = 0.18`** (latest stable, April 2026).
  Features: `v4` for the Socket.io v4 wire protocol the bench-runner
  speaks, `tracing` for structured logs. Maintainer was actively pushing
  engineio hardening fixes on the day this branch landed; bump the pin
  in tandem with the next tagged release if those fixes have shipped by
  the time you deploy.
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

*Run pending. Numbers will land here once the services are deployed and the
manifest is rerun against them.*

To run the socketioxide-only subset:

```bash
cd backend
BENCH_RUNNER_URL=https://bench-runner-production.up.railway.app \
BENCH_RUNNER_TOKEN=<token> \
FILTER=socketioxide \
  npm run bench:rebaseline
```

Multi-shard idle and avalanche entries gate behind `INCLUDE_IDLE=1` and
`INCLUDE_AVALANCHE=1` as usual.
