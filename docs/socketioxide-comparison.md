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
  `/health`, `/stats`, `/_broadcast`, `/publish-local`. Needs a compile +
  review pass by someone with current `socketioxide` API expertise; the
  Connection State Recovery section in `socketioxide/src/main.rs` is the
  rough spot (the crate's CSR API has moved across versions).
- **Bench-runner endpoints:** none new. The Rust server speaks the Socket.io
  wire protocol, so the existing `bench-jitter-socketio`,
  `bench-jitter-socketio-csr`, `bench-idle-socketio`, and
  `bench-avalanche-socketio` endpoints all accept it via `?serverUrl=...`.
- **Manifest entries:** added under each rubric in
  `backend/src/bench/tests-manifest.ts`. Baselines are empty until the first
  run lands.
- **Railway services:** two services to deploy from this directory:
  `socketioxide-server` (default mode) and `socketioxide-server-csr`
  (`SOCKETIO_CSR=1` set at boot). Both on the same hardware tier as the
  other Socket.io targets so the comparison stays apples-to-apples.

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

| Rubric | Default | + CSR |
|---|---|---|
| Latency 1K | `latency-socketioxide-1k` | `latency-socketioxide-csr-1k` |
| Latency 10K | `latency-socketioxide-10k` | `latency-socketioxide-csr-10k` |
| Jitter 10K | `jitter-socketioxide-10k` | `jitter-socketioxide-csr-10k` |
| Idle 1M target | `idle-socketioxide` | n/a |
| Avalanche 5K | `avalanche-socketioxide-5k` | n/a |
| Avalanche 10K | `avalanche-socketioxide-10k` | n/a |
| Avalanche 20K | `avalanche-socketioxide-20k` | n/a |

Whispers and throughput entries can be added the same way once we have
the latency + jitter numbers and we know the server config holds up.

## Open questions for the library author

Tagged in the GitHub issue:

1. **CSR API.** The `socketioxide/src/main.rs` skeleton has the
   Connection State Recovery builder call commented out with a FIXME.
   Confirm the right shape for the pinned crate version (or bump
   the version, whichever is cleaner).
2. **Recovered flag.** When a CSR resume succeeds, we need to bump the
   `recovered` counter so `/stats-csr` reports it. The right accessor
   on the socket varies across crate versions.
3. **Production-shaped publisher.** Our `/_broadcast` handler calls
   `io.to(stream).emit(...).await`. That's the natural shape but it
   might not match the recommended pattern for socketioxide in
   production. Open to a PR that swaps it.
4. **Default config knobs.** Anything we should tune for the comparison
   to be fair? Compression, ping intervals, buffer sizes. Defaults
   here match the Node Socket.io server's bench config; happy to take
   a config patch.

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
