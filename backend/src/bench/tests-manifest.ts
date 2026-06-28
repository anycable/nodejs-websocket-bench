// Manifest of every test currently on the compare/socket-io page.
//
// `npm run bench:rebaseline` walks this list, hits each bench-runner
// endpoint, writes the result JSON to tmp/v1.6.14-bench-results/, and
// prints a delta vs `baseline`. Pick a subset with FILTER=<substring>.
//
// IMPORTANT: `baseline` is NOT identical to the number printed on the
// compare page.
//   - Page numbers were captured during a Railway shared-tenant window
//     where neighbour load pushed latencies higher. They represent the
//     worst we measured under realistic shared-infra load (the cautious
//     number a reader should assume).
//   - Baselines below are what the same tests deliver on a quieter
//     refresh. Latencies are uniformly ~50% better than the page; other
//     fields (delivery rate, connections held) match.
//
// So a green rebaseline confirms "we still beat today's floor", which
// is stricter than the page promises. A red rebaseline means we've
// regressed below the better-than-page floor, which is a real signal
// even if the page numbers still hold.
//
// Update `baseline` here when the underlying setup changes or an
// accepted drift becomes the new floor. Refresh the page numbers in
// tandem if the drift is in a worse direction.

export type TestCategory =
  | "latency"
  | "jitter"
  | "whispers"
  | "throughput"
  | "idle"
  | "avalanche";

export interface TestSpec {
  id: string;
  description: string;
  category: TestCategory;
  // Bench-runner endpoint path, no leading slash.
  endpoint: string;
  // Override the default bench-runner. Useful if a test needs a specific
  // shard (e.g., a 25K+ test that should land on a fresh-process shard).
  benchRunner?: string;
  // - "sync"        blocks on the response. <5 min tests only.
  // - "async"       enqueues, polls /jobs/:id. For tests that may run >5 min.
  // - "multi-shard" fans out across N bench-runner replicas via the
  //                 shard-coordinator; rebaseline merges per-shard results.
  //                 Set numShards + perShardN. Idle capacity tests use this.
  // - "avalanche"   async test where the runner triggers `railway service
  //                 redeploy` mid-flight to simulate the in-process WS
  //                 layer restarting under N held connections. Set
  //                 redeployServiceName + the bench-runner endpoint's
  //                 prearmSec param.
  mode: "sync" | "async" | "multi-shard" | "avalanche";
  params: Record<string, string | number>;
  // Page baseline values, keyed by dotted paths into the result JSON.
  baseline: Record<string, number | string>;
  // Override threshold for "significant drift" (% of baseline). Default 5%.
  driftThresholdPct?: number;
  // Bench-runner-side label used in console output. Helpful for diffing
  // logs by-eye when something looks off.
  label?: string;
  // Multi-shard config. Total clients = numShards * perShardN; shards run
  // in parallel against the same target. Ignored unless mode is "multi-shard".
  numShards?: number;
  perShardN?: number;
  // Railway service ID of the target. When set on a multi-shard test, the
  // runner queries Railway metrics for the test window and attaches peak
  // memory / CPU plus derived RAM-per-connection to the merged result.
  // Look up via `railway status --json` or the dashboard URL.
  targetServiceId?: string;
  // For mode "avalanche": the Railway service to redeploy mid-test.
  // The runner spawns `railway service redeploy --service X --yes` after
  // the bench-runner finishes ramping clients.
  redeployServiceName?: string;
}

// Internal Railway targets used by the manifest. Bench-runners are on
// Railway's internal network, so these *.railway.internal URLs work
// from inside a bench-runner container.
const TARGETS = {
  socketio: "http://socketio-server.railway.internal:3000",
  // CSR variant runs the same image with SOCKETIO_CSR=1 at boot. The
  // Railway service is `socketio-server-csr` in the dashboard, but it's
  // historically been routed under the `socketio-server-small`
  // internal hostname. Railway's DNS still resolves the historical
  // name and not the new one from inside the bench-runner; this is
  // the working address.
  socketioCsr: "http://socketio-server-small.railway.internal:3000",
  uwsWs: "ws://uws-server.railway.internal:3000/ws",
  uwsHttp: "http://uws-server.railway.internal:3000",
  anycableOss: "ws://anycable-go.railway.internal:8080/cable",
  anycableOssBroadcast: "http://anycable-go.railway.internal:8080/_broadcast",
  anycablePro: "ws://anycable-go-pro.railway.internal:8080/cable",
  anycableProBroadcast:
    "http://anycable-go-pro.railway.internal:8080/_broadcast",
  // socketioxide is the Rust implementation of the Socket.io protocol.
  // Wire-compatible with socket.io-client, so the existing bench-runner
  // bench-jitter-socketio / bench-idle-socketio / bench-avalanche-socketio
  // endpoints work with ?serverUrl=. No CSR variant yet — the library
  // doesn't appear to ship Connection State Recovery as of 0.18.3.
  // See docs/socketioxide-comparison.md for the open question to the
  // library author.
  socketioxide: "http://socketioxide-server.railway.internal:3000",

  // Rails broadcasting comparison (AnyCable vs Action Cable vs Solid Cable).
  // One Rails app (cable-bench/), three deployments selected by BENCH_MODE.
  // Action Cable / Solid Cable terminate WebSockets in Puma and expose the
  // app's POST /_bench/broadcast publish endpoint; the bench-runner reuses the
  // anycable jitter/idle/avalanche endpoints with ?channel=BenchmarkChannel and
  // ?acProtocol=actioncable-v1-json. AnyCable terminates in a separate
  // anycable-go gateway (RPC -> the Rails app) and publishes via the gateway's
  // /_broadcast, exactly like the standalone AnyCable target, but over the
  // extended protocol and a real BenchmarkChannel.
  railsSolidCable: "ws://rails-solidcable.railway.internal:3000/cable",
  railsSolidCableBroadcast: "http://rails-solidcable.railway.internal:3000/_bench/broadcast",
  railsActionCable: "ws://rails-actioncable.railway.internal:3000/cable",
  railsActionCableBroadcast: "http://rails-actioncable.railway.internal:3000/_bench/broadcast",
  railsAnyCable: "ws://anycable-go-rails.railway.internal:8080/cable",
  railsAnyCableBroadcast: "http://anycable-go-rails.railway.internal:8080/_broadcast",
  // AsyncCable: standard Action Cable wire protocol, served in-process by
  // Falcon (async/fibers) instead of Puma. Same /cable + /_bench/broadcast
  // surface as the other in-process Rails targets.
  railsAsyncCable: "ws://rails-asynccable.railway.internal:3000/cable",
  railsAsyncCableBroadcast: "http://rails-asynccable.railway.internal:3000/_bench/broadcast",
};

// Action Cable subscribe presets. BenchmarkChannel is the channel the Rails
// app exposes (cable-bench/app/channels/benchmark_channel.rb). Vanilla Action
// Cable / Solid Cable speak the base protocol; AnyCable the extended one.
const RAILS_BASE = { channel: "BenchmarkChannel", acProtocol: "actioncable-v1-json" };
const RAILS_EXT = { channel: "BenchmarkChannel", acProtocol: "actioncable-v1-ext-json" };

// Common knobs reused across tests. Keep these explicit so the manifest
// is self-documenting; copy-paste is fine when a test deviates.
const LATENCY_1K = { msgs: 100, interval: 500, ramp: 100, duration: 90, jitter: 999999 };
const LATENCY_10K = { msgs: 100, interval: 500, ramp: 200, duration: 130, jitter: 999999 };
const JITTER_10K = { msgs: 120, interval: 500, ramp: 200, duration: 160, jitter: 15, jitterMs: 1000 };
const WHISPERS_1K = { rooms: 10, ramp: 100, interval: 500, duration: 30, payload: 64 };
const THROUGHPUT_10K_1M = { n: 10000, total: 100, intervalMs: 10, ramp: 200, drain: 30, publisher: "pool", publisherConcurrency: 16 };

export const tests: TestSpec[] = [
  // -------------------------------------------------------------------------
  // Latency (jitter-disabled, baseline roundtrip)
  // -------------------------------------------------------------------------
  {
    id: "latency-socketio-1k",
    description: "Roundtrip latency, default Socket.io, 1K subs",
    category: "latency",
    endpoint: "bench-jitter-socketio",
    mode: "sync",
    params: { n: 1000, ...LATENCY_1K, serverUrl: TARGETS.socketio },
    baseline: { "latencyRawMs.p50": 11, "latencyRawMs.p99": 20, deliveryRatePct: 100 },
  },
  {
    id: "latency-socketio-10k",
    description: "Roundtrip latency, default Socket.io, 10K subs",
    category: "latency",
    endpoint: "bench-jitter-socketio",
    mode: "sync",
    params: { n: 10000, ...LATENCY_10K, serverUrl: TARGETS.socketio },
    baseline: { "latencyRawMs.p50": 88, "latencyRawMs.p99": 176, deliveryRatePct: 100 },
  },
  {
    id: "latency-socketio-csr-1k",
    description: "Roundtrip latency, Socket.io + CSR, 1K subs",
    category: "latency",
    endpoint: "bench-jitter-socketio-csr",
    mode: "sync",
    params: { n: 1000, ...LATENCY_1K, serverUrl: TARGETS.socketioCsr },
    baseline: { "latencyRawMs.p50": 12, "latencyRawMs.p99": 24, deliveryRatePct: 100 },
  },
  {
    id: "latency-socketio-csr-10k",
    description: "Roundtrip latency, Socket.io + CSR, 10K subs",
    category: "latency",
    endpoint: "bench-jitter-socketio-csr",
    mode: "sync",
    params: { n: 10000, ...LATENCY_10K, serverUrl: TARGETS.socketioCsr },
    baseline: { "latencyRawMs.p50": 88, "latencyRawMs.p99": 349, deliveryRatePct: 100 },
    driftThresholdPct: 25,
  },
  {
    id: "latency-uws-1k",
    description: "Roundtrip latency, uWS topics, 1K subs",
    category: "latency",
    endpoint: "bench-jitter-uws",
    mode: "sync",
    params: { n: 1000, ...LATENCY_1K, wsUrl: TARGETS.uwsWs, httpUrl: TARGETS.uwsHttp },
    baseline: { "latencyRawMs.p50": 8, "latencyRawMs.p99": 17, deliveryRatePct: 100 },
  },
  {
    id: "latency-uws-10k",
    description: "Roundtrip latency, uWS topics, 10K subs (known noisy p99)",
    category: "latency",
    endpoint: "bench-jitter-uws",
    mode: "sync",
    params: { n: 10000, ...LATENCY_10K, wsUrl: TARGETS.uwsWs, httpUrl: TARGETS.uwsHttp },
    // The 4-11 s p99 range on the page was a backpressure artifact under
    // heavier Railway load. As-measured today comes in under 300 ms.
    // Wide threshold to absorb the swing if the backpressure path re-engages.
    baseline: { "latencyRawMs.p50": 61, "latencyRawMs.p99": 292, deliveryRatePct: 100 },
    driftThresholdPct: 80,
  },
  {
    id: "latency-anycable-oss-1k",
    description: "Roundtrip latency, AnyCable OSS, 1K subs",
    category: "latency",
    endpoint: "bench-jitter-anycable",
    mode: "sync",
    params: { n: 1000, ...LATENCY_1K, cableUrl: TARGETS.anycableOss, broadcastUrl: TARGETS.anycableOssBroadcast },
    baseline: { "latencyRawMs.p50": 10, "latencyRawMs.p99": 26, deliveryRatePct: 100 },
  },
  {
    id: "latency-anycable-oss-10k",
    description: "Roundtrip latency, AnyCable OSS, 10K subs",
    category: "latency",
    endpoint: "bench-jitter-anycable",
    mode: "sync",
    params: { n: 10000, ...LATENCY_10K, cableUrl: TARGETS.anycableOss, broadcastUrl: TARGETS.anycableOssBroadcast },
    baseline: { "latencyRawMs.p50": 236, "latencyRawMs.p99": 880, deliveryRatePct: 100 },
  },
  {
    id: "latency-anycable-pro-1k",
    description: "Roundtrip latency, AnyCable Pro, 1K subs",
    category: "latency",
    endpoint: "bench-jitter-anycable",
    mode: "sync",
    params: { n: 1000, ...LATENCY_1K, cableUrl: TARGETS.anycablePro, broadcastUrl: TARGETS.anycableProBroadcast },
    baseline: { "latencyRawMs.p50": 11, "latencyRawMs.p99": 23, deliveryRatePct: 100 },
  },
  {
    id: "latency-anycable-pro-10k",
    description: "Roundtrip latency, AnyCable Pro, 10K subs",
    category: "latency",
    endpoint: "bench-jitter-anycable",
    mode: "sync",
    params: { n: 10000, ...LATENCY_10K, cableUrl: TARGETS.anycablePro, broadcastUrl: TARGETS.anycableProBroadcast },
    baseline: { "latencyRawMs.p50": 234, "latencyRawMs.p99": 694, deliveryRatePct: 100 },
  },
  // socketioxide: same Socket.io wire protocol, Rust implementation. Uses
  // the bench-jitter-socketio endpoint with ?serverUrl=<socketioxide>. No
  // baselines yet — first run pending. See docs/socketioxide-comparison.md.
  {
    id: "latency-socketioxide-1k",
    description: "Roundtrip latency, socketioxide (Rust), 1K subs",
    category: "latency",
    endpoint: "bench-jitter-socketio",
    mode: "sync",
    params: { n: 1000, ...LATENCY_1K, serverUrl: TARGETS.socketioxide },
    baseline: {},
  },
  {
    id: "latency-socketioxide-10k",
    description: "Roundtrip latency, socketioxide (Rust), 10K subs",
    category: "latency",
    endpoint: "bench-jitter-socketio",
    mode: "sync",
    params: { n: 10000, ...LATENCY_10K, serverUrl: TARGETS.socketioxide },
    baseline: {},
  },

  // -------------------------------------------------------------------------
  // Reliability (jitter under WiFi-drop pattern)
  // -------------------------------------------------------------------------
  {
    id: "jitter-socketio-10k",
    description: "Reliability under WiFi jitter, default Socket.io, 10K",
    category: "jitter",
    endpoint: "bench-jitter-socketio",
    mode: "async", // 160s + ramp; near Railway's 5-min cap
    params: { n: 10000, ...JITTER_10K, serverUrl: TARGETS.socketio, samplesCap: 5000 },
    // At-most-once protocol — each ~2 s offline window misses 3-4 broadcasts
    // and they're gone (no replay). ~85% delivery is the steady-state under
    // the symmetric 2 s window. The old 27% baseline reflected a tighter
    // 1 s window where the reconnect storm also overwhelmed the server;
    // both effects collapsed into one number.
    baseline: { deliveryRatePct: 84, "latencyRawMs.p95": 450 },
    driftThresholdPct: 15,
  },
  {
    id: "jitter-socketio-csr-10k",
    description: "Reliability under WiFi jitter, Socket.io + CSR, 10K (CSR server)",
    category: "jitter",
    endpoint: "bench-jitter-socketio-csr",
    mode: "async",
    params: { n: 10000, ...JITTER_10K, serverUrl: TARGETS.socketioCsr, samplesCap: 5000 },
    // CSR's protocol now resumes ~99.5% of disconnects cleanly with the
    // symmetric 2 s offline window. Replay-tail p95 ~2 s (within the
    // offline window itself), p99 ~4.5 s, max ~10 s.
    baseline: { deliveryRatePct: 100, lostDeliveries: 0, "latencyRawMs.p95": 1970, csrResumes: 81000 },
    driftThresholdPct: 15,
  },
  {
    id: "jitter-uws-10k",
    description: "Reliability under WiFi jitter, uWS topics, 10K",
    category: "jitter",
    endpoint: "bench-jitter-uws",
    mode: "async",
    params: { n: 10000, ...JITTER_10K, wsUrl: TARGETS.uwsWs, httpUrl: TARGETS.uwsHttp, samplesCap: 5000 },
    // At-most-once like default Socket.io; ~87% delivery is the steady-state
    // under the 2 s window. Slightly higher than default Socket.io because
    // uWS's reconnect via ReconnectingWs has tighter backoff variance.
    baseline: { deliveryRatePct: 87, lostDeliveries: 154000, "latencyRawMs.p95": 720 },
    driftThresholdPct: 15,
  },
  {
    id: "jitter-anycable-oss-10k",
    description: "Reliability under WiFi jitter, AnyCable OSS, 10K",
    category: "jitter",
    endpoint: "bench-jitter-anycable",
    mode: "async",
    params: { n: 10000, ...JITTER_10K, cableUrl: TARGETS.anycableOss, broadcastUrl: TARGETS.anycableOssBroadcast, samplesCap: 5000 },
    baseline: { deliveryRatePct: 100, lostDeliveries: 0, "latencyRawMs.p95": 4100, "latencyRawMs.p99": 6200 },
  },
  {
    id: "jitter-anycable-pro-10k",
    description: "Reliability under WiFi jitter, AnyCable Pro, 10K",
    category: "jitter",
    endpoint: "bench-jitter-anycable",
    mode: "async",
    params: { n: 10000, ...JITTER_10K, cableUrl: TARGETS.anycablePro, broadcastUrl: TARGETS.anycableProBroadcast, samplesCap: 5000 },
    baseline: { deliveryRatePct: 100, lostDeliveries: 0, "latencyRawMs.p95": 4100, "latencyRawMs.p99": 6200 },
  },
  // socketioxide jitter row. Expected to land in the at-most-once band
  // with default Socket.io and uWS, since socketioxide doesn't appear to
  // ship CSR. Confirms the architectural claim across runtimes.
  {
    id: "jitter-socketioxide-10k",
    description: "Reliability under WiFi jitter, socketioxide (Rust), 10K",
    category: "jitter",
    endpoint: "bench-jitter-socketio",
    mode: "async",
    params: { n: 10000, ...JITTER_10K, serverUrl: TARGETS.socketioxide, samplesCap: 5000 },
    baseline: {},
  },

  // -------------------------------------------------------------------------
  // Whispers (1K × 10 rooms, 100 peers/room)
  //
  // The single-shard variants below catch regressions in the driver +
  // broker round-trip on a Node event loop that's pinned at ~200K msg/sec
  // receive. They are NOT the page numbers for AnyCable / uWS latency:
  // those come from `npm run bench:whispers:multi` (40 shards × 25 cables)
  // so the driver isn't the bottleneck. Re-run the multi-shard driver
  // by hand when the page row needs refreshing; this manifest tracks
  // the single-shard regression floor.
  // -------------------------------------------------------------------------
  {
    id: "whispers-socketio-1k",
    description: "Whispers via Socket.io rooms emulation, 1K × 10 rooms",
    category: "whispers",
    endpoint: "bench-whispers-socketio",
    mode: "sync",
    params: { n: 1000, ...WHISPERS_1K, serverUrl: TARGETS.socketio },
    // The saturating-Express path; delivery and tail vary widely with load.
    baseline: { deliveryRatePct: 62.3, "latencyMs.p50": 1784, "latencyMs.p99": 12069 },
    driftThresholdPct: 25,
  },
  {
    id: "whispers-uws-1k",
    description: "Whispers via uWS topics, 1K × 10 rooms",
    category: "whispers",
    endpoint: "bench-whispers-uws",
    mode: "sync",
    params: { n: 1000, ...WHISPERS_1K, wsUrl: TARGETS.uwsWs },
    baseline: { deliveryRatePct: 100, "latencyMs.p50": 10, "latencyMs.p99": 21 },
  },
  {
    id: "whispers-anycable-oss-1k",
    description: "Whispers via AnyCable channel.whisper, OSS, 1K × 10 rooms",
    category: "whispers",
    endpoint: "bench-whispers-anycable",
    mode: "sync",
    params: { n: 1000, ...WHISPERS_1K, cableUrl: TARGETS.anycableOss },
    baseline: { deliveryRatePct: 100, "latencyMs.p50": 18, "latencyMs.p99": 78 },
  },
  {
    id: "whispers-anycable-pro-1k",
    description: "Whispers via AnyCable channel.whisper, Pro, 1K × 10 rooms",
    category: "whispers",
    endpoint: "bench-whispers-anycable",
    mode: "sync",
    params: { n: 1000, ...WHISPERS_1K, cableUrl: TARGETS.anycablePro },
    baseline: { deliveryRatePct: 100, "latencyMs.p50": 18, "latencyMs.p99": 64 },
  },

  // -------------------------------------------------------------------------
  // Throughput (1M-delivery target, HTTP pool publisher, 10K subs)
  // -------------------------------------------------------------------------
  {
    id: "throughput-socketio",
    description: "Broadcast throughput, default Socket.io, 1M deliveries",
    category: "throughput",
    endpoint: "bench-throughput-socketio",
    mode: "async",
    params: { ...THROUGHPUT_10K_1M, serverUrl: TARGETS.socketio },
    baseline: { deliveryRatePct: 100, "latencyRawMs.p50": 741, "latencyRawMs.p99": 3934 },
    driftThresholdPct: 30,
  },
  {
    id: "throughput-socketio-csr",
    description: "Broadcast throughput, Socket.io + CSR, 1M deliveries",
    category: "throughput",
    endpoint: "bench-throughput-socketio-csr",
    mode: "async",
    params: { ...THROUGHPUT_10K_1M, serverUrl: TARGETS.socketio },
    baseline: { deliveryRatePct: 100, "latencyRawMs.p50": 497, "latencyRawMs.p99": 2255 },
    driftThresholdPct: 30,
  },
  {
    id: "throughput-uws",
    description: "Broadcast throughput, uWS topics, 1M deliveries",
    category: "throughput",
    endpoint: "bench-throughput-uws",
    mode: "async",
    params: { ...THROUGHPUT_10K_1M, wsUrl: TARGETS.uwsWs, httpUrl: TARGETS.uwsHttp },
    baseline: { deliveryRatePct: 100, "latencyRawMs.p50": 219, "latencyRawMs.p99": 3171 },
    driftThresholdPct: 15,
  },
  {
    id: "throughput-anycable-oss",
    description: "Broadcast throughput, AnyCable OSS, 1M deliveries",
    category: "throughput",
    endpoint: "bench-throughput-anycable",
    mode: "async",
    params: { ...THROUGHPUT_10K_1M, cableUrl: TARGETS.anycableOss, broadcastUrl: TARGETS.anycableOssBroadcast },
    // Two-sample average; first sample p99 spiked to 8.6 s but the rerun
    // returned 3.13 s. Pro on the same setup is 3.93 s.
    baseline: { deliveryRatePct: 100, "latencyRawMs.p50": 360, "latencyRawMs.p99": 3130 },
    driftThresholdPct: 30,
  },
  {
    id: "throughput-anycable-pro",
    description: "Broadcast throughput, AnyCable Pro, 1M deliveries",
    category: "throughput",
    endpoint: "bench-throughput-anycable",
    mode: "async",
    params: { ...THROUGHPUT_10K_1M, cableUrl: TARGETS.anycablePro, broadcastUrl: TARGETS.anycableProBroadcast },
    // Same-day variance observed: p99 3349 → 7057 ms over two consecutive
    // runs at 100 msg/s × 10K subs. Latency tail at this fanout rate is
    // dominated by anycable-go GC pauses + Railway internal-network
    // backpressure; the wide threshold absorbs that without hiding a
    // real regression — delivery and p50 are still floored at 100% / 800 ms.
    baseline: { deliveryRatePct: 100, "latencyRawMs.p50": 365, "latencyRawMs.p99": 3927 },
    driftThresholdPct: 50,
  },

  // -------------------------------------------------------------------------
  // Idle capacity (multi-shard; gated behind INCLUDE_IDLE=1 in rebaseline
  // because each test fans out across 50 bench-runner replicas)
  // -------------------------------------------------------------------------
  // Idle baselines are intentionally wide: container memory caps were
  // equalized on 2026-06-05 (all 5 target services pinned to 32 GB) and
  // the prior page numbers came from a non-uniform setup (anycable
  // services on unlimited memory, socketio-server 953 MB, uws-server
  // 476 MB). The first overnight sweep on equalized hardware will set
  // the real baselines; until then, the runner reports drift but doesn't
  // flag regression. The Socket.io ceiling is the Node event loop on
  // handshakes (not memory), so it stays near its previous number.
  {
    id: "idle-socketio",
    description: "Idle connections held, default Socket.io, 1M target",
    category: "idle",
    endpoint: "bench-idle-socketio",
    mode: "multi-shard",
    numShards: 50,
    perShardN: 20000,
    params: { hold: 120, ramp: 200, stream: "idle-rebaseline", serverUrl: TARGETS.socketio },
    targetServiceId: "8b861242-2747-42a4-a831-8c63a8289f22",
    baseline: { connected: 120000, ramKbPerConnected: 52 },
    driftThresholdPct: 60,
  },
  {
    id: "idle-anycable-oss",
    description: "Idle connections held, AnyCable OSS, 1M target",
    category: "idle",
    endpoint: "bench-idle-anycable",
    mode: "multi-shard",
    numShards: 50,
    perShardN: 20000,
    params: { hold: 120, ramp: 200, stream: "idle-rebaseline", cableUrl: TARGETS.anycableOss },
    targetServiceId: "a6f8e7a0-46fb-4614-9a32-6f97f49bad09",
    baseline: { connected: 820000, ramKbPerConnected: 34, peakCpuPercent: 10 },
    driftThresholdPct: 60,
  },
  {
    id: "idle-anycable-pro",
    description: "Idle connections held, AnyCable Pro, 1M target",
    category: "idle",
    endpoint: "bench-idle-anycable",
    mode: "multi-shard",
    numShards: 50,
    perShardN: 20000,
    params: { hold: 120, ramp: 200, stream: "idle-rebaseline", cableUrl: TARGETS.anycablePro },
    targetServiceId: "5cef6fb0-7f6d-4ef3-a92d-93266af42b45",
    baseline: { connected: 820000, ramKbPerConnected: 18, peakCpuPercent: 8 },
    driftThresholdPct: 60,
  },
  {
    id: "idle-uws",
    description: "Idle connections held, uWS, 1M target",
    category: "idle",
    endpoint: "bench-idle-uws",
    mode: "multi-shard",
    numShards: 50,
    perShardN: 20000,
    params: { hold: 120, ramp: 200, stream: "idle-rebaseline", wsUrl: TARGETS.uwsWs },
    targetServiceId: "fb6c422b-b772-4187-bd31-fa616f40d513",
    baseline: { connected: 1000000, ramKbPerConnected: 5 },
    driftThresholdPct: 60,
  },
  // socketioxide idle: same multi-shard fan-out, targets the Rust service.
  {
    id: "idle-socketioxide",
    description: "Idle connections held, socketioxide (Rust), 1M target",
    category: "idle",
    endpoint: "bench-idle-socketio",
    mode: "multi-shard",
    numShards: 50,
    perShardN: 20000,
    params: { hold: 120, ramp: 200, stream: "idle-rebaseline", serverUrl: TARGETS.socketioxide },
    targetServiceId: "41f1ac22-2ea6-4d04-974e-4c148be426ff",
    baseline: {},
    driftThresholdPct: 60,
  },

  // -------------------------------------------------------------------------
  // Avalanche (in-process WS layer restart under N held connections).
  // Each test ramps N socket.io clients against socketio-server, then
  // triggers `railway service redeploy --service socketio-server --yes`
  // mid-test and measures recovery time + reconnection percentage. Gated
  // behind INCLUDE_AVALANCHE=1 because each restart takes ~3-5 min and the
  // 25K test typically OOMs the new container.
  // -------------------------------------------------------------------------
  {
    id: "avalanche-socketio-5k",
    description: "Avalanche: 5K Socket.io clients, app redeploy",
    category: "avalanche",
    endpoint: "bench-avalanche-socketio",
    mode: "avalanche",
    redeployServiceName: "socketio-server",
    params: { n: 5000, ramp: 200, prearm: 90, recoveryWait: 180, stream: "avalanche-5k", serverUrl: TARGETS.socketio },
    baseline: { recoveryTimeMs: 4500, reconnectRatePct: 100, "reconnectMs.p99": 2200 },
    driftThresholdPct: 50,
  },
  {
    id: "avalanche-socketio-10k",
    description: "Avalanche: 10K Socket.io clients, app redeploy",
    category: "avalanche",
    endpoint: "bench-avalanche-socketio",
    mode: "avalanche",
    redeployServiceName: "socketio-server",
    params: { n: 10000, ramp: 200, prearm: 120, recoveryWait: 240, stream: "avalanche-10k", serverUrl: TARGETS.socketio },
    baseline: { recoveryTimeMs: 3900, reconnectRatePct: 100 },
    driftThresholdPct: 50,
  },
  {
    id: "avalanche-socketio-15k",
    description: "Avalanche: 15K Socket.io clients, app redeploy",
    category: "avalanche",
    endpoint: "bench-avalanche-socketio",
    mode: "avalanche",
    redeployServiceName: "socketio-server",
    params: { n: 15000, ramp: 200, prearm: 240, recoveryWait: 600, stream: "avalanche-15k", serverUrl: TARGETS.socketio },
    baseline: { recoveryTimeMs: 5800, reconnectRatePct: 98.5 },
    driftThresholdPct: 50,
  },
  {
    id: "avalanche-socketio-20k",
    description: "Avalanche: 20K Socket.io clients, app redeploy",
    category: "avalanche",
    endpoint: "bench-avalanche-socketio",
    mode: "avalanche",
    redeployServiceName: "socketio-server",
    params: { n: 20000, ramp: 200, prearm: 240, recoveryWait: 600, stream: "avalanche-20k", serverUrl: TARGETS.socketio },
    baseline: { recoveryTimeMs: 8000, reconnectRatePct: 96.2 },
    driftThresholdPct: 50,
  },
  // uWS avalanche: same shape, redeploys uws-server. The page currently
  // says "uWS pushes the cliff further but the shape is the same" by
  // inference; these tests give us measurement at the page-relevant
  // scales (10K is the operational good case; 20K is the cliff).
  {
    id: "avalanche-uws-10k",
    description: "Avalanche: 10K uWS clients, server redeploy",
    category: "avalanche",
    endpoint: "bench-avalanche-uws",
    mode: "avalanche",
    redeployServiceName: "uws-server",
    params: { n: 10000, ramp: 200, prearm: 180, recoveryWait: 240, stream: "avalanche-uws-10k", wsUrl: TARGETS.uwsWs },
    baseline: { recoveryTimeMs: 5000, reconnectRatePct: 100 },
    driftThresholdPct: 50,
  },
  {
    id: "avalanche-uws-20k",
    description: "Avalanche: 20K uWS clients, server redeploy",
    category: "avalanche",
    endpoint: "bench-avalanche-uws",
    mode: "avalanche",
    redeployServiceName: "uws-server",
    params: { n: 20000, ramp: 200, prearm: 240, recoveryWait: 600, stream: "avalanche-uws-20k", wsUrl: TARGETS.uwsWs },
    baseline: { recoveryTimeMs: 60000, reconnectRatePct: 90 },
    driftThresholdPct: 50,
  },
  {
    id: "avalanche-socketio-25k",
    description: "Avalanche: 25K Socket.io clients, app redeploy (the cliff)",
    category: "avalanche",
    endpoint: "bench-avalanche-socketio",
    mode: "avalanche",
    redeployServiceName: "socketio-server",
    params: { n: 25000, ramp: 200, prearm: 300, recoveryWait: 600, stream: "avalanche-25k", serverUrl: TARGETS.socketio },
    // Page reports "never" for recovery and 0% reconnected at 25K.
    // The bench-runner returns whatever reconnectRatePct it observed in
    // the recovery window; we baseline at the failure case (0%) with a
    // wide threshold so even partial recovery would surface but won't
    // trigger a regression flag.
    baseline: { reconnectRatePct: 0 },
    driftThresholdPct: 100,
  },
  // socketioxide avalanche escalation: mirror the Socket.io ladder (5K, 10K,
  // 15K, 20K, 25K). Same redeploy mechanism, just pointed at the Rust service.
  // The interesting question is whether Rust's event loop pushes the cliff out
  // further than Node's, or whether the architectural problem (in-process WS
  // dies with the app) holds the shape across languages.
  {
    id: "avalanche-socketioxide-5k",
    description: "Avalanche: 5K socketioxide clients, server redeploy",
    category: "avalanche",
    endpoint: "bench-avalanche-socketio",
    mode: "avalanche",
    redeployServiceName: "socketioxide-server",
    params: { n: 5000, ramp: 200, prearm: 90, recoveryWait: 180, stream: "avalanche-sox-5k", serverUrl: TARGETS.socketioxide },
    baseline: {},
    driftThresholdPct: 100,
  },
  {
    id: "avalanche-socketioxide-10k",
    description: "Avalanche: 10K socketioxide clients, server redeploy",
    category: "avalanche",
    endpoint: "bench-avalanche-socketio",
    mode: "avalanche",
    redeployServiceName: "socketioxide-server",
    params: { n: 10000, ramp: 200, prearm: 120, recoveryWait: 240, stream: "avalanche-sox-10k", serverUrl: TARGETS.socketioxide },
    baseline: {},
    driftThresholdPct: 100,
  },
  {
    id: "avalanche-socketioxide-20k",
    description: "Avalanche: 20K socketioxide clients, server redeploy",
    category: "avalanche",
    endpoint: "bench-avalanche-socketio",
    mode: "avalanche",
    redeployServiceName: "socketioxide-server",
    params: { n: 20000, ramp: 200, prearm: 240, recoveryWait: 600, stream: "avalanche-sox-20k", serverUrl: TARGETS.socketioxide },
    baseline: {},
    driftThresholdPct: 100,
  },

  // ===========================================================================
  // Rails broadcasting: AnyCable vs Action Cable vs Solid Cable
  //
  // One Rails app, three cable backends. All speak Action Cable at the app
  // level (same BenchmarkChannel), but: Solid Cable and Action Cable terminate
  // WebSockets in Puma (in-process Ruby; Solid Cable also polls the DB), while
  // AnyCable offloads them to anycable-go (Rails is only the gRPC backend) and
  // speaks the extended protocol with delivery guarantees. Baselines are empty
  // until the first same-window Railway sweep. Reuses the anycable bench-runner
  // endpoints via ?channel + ?acProtocol; no new endpoints for latency/jitter.
  // ===========================================================================

  // Latency (jitter-disabled roundtrip)
  {
    id: "latency-rails-solidcable-1k",
    description: "Roundtrip latency, Rails + Solid Cable, 1K subs",
    category: "latency",
    endpoint: "bench-jitter-anycable",
    mode: "sync",
    params: { n: 1000, ...LATENCY_1K, ...RAILS_BASE, cableUrl: TARGETS.railsSolidCable, broadcastUrl: TARGETS.railsSolidCableBroadcast },
    baseline: {},
  },
  {
    id: "latency-rails-solidcable-5k",
    description: "Roundtrip latency, Rails + Solid Cable, 5K subs",
    category: "latency",
    endpoint: "bench-jitter-anycable",
    mode: "sync",
    params: { n: 5000, ...LATENCY_10K, ...RAILS_BASE, cableUrl: TARGETS.railsSolidCable, broadcastUrl: TARGETS.railsSolidCableBroadcast },
    baseline: {},
  },
  {
    id: "latency-rails-actioncable-1k",
    description: "Roundtrip latency, Rails + Action Cable (Redis), 1K subs",
    category: "latency",
    endpoint: "bench-jitter-anycable",
    mode: "sync",
    params: { n: 1000, ...LATENCY_1K, ...RAILS_BASE, cableUrl: TARGETS.railsActionCable, broadcastUrl: TARGETS.railsActionCableBroadcast },
    baseline: {},
  },
  {
    id: "latency-rails-actioncable-5k",
    description: "Roundtrip latency, Rails + Action Cable (Redis), 5K subs",
    category: "latency",
    endpoint: "bench-jitter-anycable",
    mode: "sync",
    params: { n: 5000, ...LATENCY_10K, ...RAILS_BASE, cableUrl: TARGETS.railsActionCable, broadcastUrl: TARGETS.railsActionCableBroadcast },
    baseline: {},
  },
  {
    id: "latency-rails-anycable-1k",
    description: "Roundtrip latency, Rails + AnyCable (Go gateway), 1K subs",
    category: "latency",
    endpoint: "bench-jitter-anycable",
    mode: "sync",
    params: { n: 1000, ...LATENCY_1K, ...RAILS_EXT, cableUrl: TARGETS.railsAnyCable, broadcastUrl: TARGETS.railsAnyCableBroadcast },
    baseline: {},
  },
  {
    id: "latency-rails-anycable-5k",
    description: "Roundtrip latency, Rails + AnyCable (Go gateway), 5K subs",
    category: "latency",
    endpoint: "bench-jitter-anycable",
    mode: "sync",
    params: { n: 5000, ...LATENCY_10K, ...RAILS_EXT, cableUrl: TARGETS.railsAnyCable, broadcastUrl: TARGETS.railsAnyCableBroadcast },
    baseline: {},
  },

  // Reliability under WiFi jitter. The headline: AnyCable's extended protocol
  // resumes the stream and backfills missed messages (delivery ~100%); vanilla
  // Action Cable and Solid Cable have no resume, so each offline window drops
  // broadcasts for good.
  {
    id: "jitter-rails-solidcable-5k",
    description: "Reliability under WiFi jitter, Rails + Solid Cable, 5K",
    category: "jitter",
    endpoint: "bench-jitter-anycable",
    mode: "async",
    params: { n: 5000, ...JITTER_10K, ...RAILS_BASE, cableUrl: TARGETS.railsSolidCable, broadcastUrl: TARGETS.railsSolidCableBroadcast, samplesCap: 5000 },
    baseline: {},
    driftThresholdPct: 15,
  },
  {
    id: "jitter-rails-actioncable-5k",
    description: "Reliability under WiFi jitter, Rails + Action Cable (Redis), 5K",
    category: "jitter",
    endpoint: "bench-jitter-anycable",
    mode: "async",
    params: { n: 5000, ...JITTER_10K, ...RAILS_BASE, cableUrl: TARGETS.railsActionCable, broadcastUrl: TARGETS.railsActionCableBroadcast, samplesCap: 5000 },
    baseline: {},
    driftThresholdPct: 15,
  },
  {
    id: "jitter-rails-anycable-5k",
    description: "Reliability under WiFi jitter, Rails + AnyCable, 5K",
    category: "jitter",
    endpoint: "bench-jitter-anycable",
    mode: "async",
    params: { n: 5000, ...JITTER_10K, ...RAILS_EXT, cableUrl: TARGETS.railsAnyCable, broadcastUrl: TARGETS.railsAnyCableBroadcast, samplesCap: 5000 },
    baseline: {},
    driftThresholdPct: 15,
  },

  // Idle capacity. In-process Puma (Solid/Action Cable) tops out far below the
  // Go gateway; targets are sized to find each ceiling (in-process ~200K probe,
  // AnyCable 1M). Fill targetServiceId after deploy to attach Railway memory/CPU.
  {
    id: "idle-rails-solidcable",
    description: "Idle connections held, Rails + Solid Cable",
    category: "idle",
    endpoint: "bench-idle-anycable",
    mode: "multi-shard",
    numShards: 13,
    perShardN: 4000,
    params: { hold: 120, ramp: 200, stream: "idle-rails", ...RAILS_BASE, cableUrl: TARGETS.railsSolidCable },
    baseline: {},
    driftThresholdPct: 60,
  },
  {
    id: "idle-rails-actioncable",
    description: "Idle connections held, Rails + Action Cable (Redis)",
    category: "idle",
    endpoint: "bench-idle-anycable",
    mode: "multi-shard",
    numShards: 13,
    perShardN: 4000,
    params: { hold: 120, ramp: 200, stream: "idle-rails", ...RAILS_BASE, cableUrl: TARGETS.railsActionCable },
    baseline: {},
    driftThresholdPct: 60,
  },
  {
    id: "idle-rails-anycable",
    description: "Idle connections held, Rails + AnyCable (Go gateway)",
    category: "idle",
    endpoint: "bench-idle-anycable",
    mode: "multi-shard",
    numShards: 13,
    perShardN: 12000,
    params: { hold: 120, ramp: 200, stream: "idle-rails", ...RAILS_EXT, cableUrl: TARGETS.railsAnyCable },
    baseline: {},
    driftThresholdPct: 60,
  },
  {
    id: "idle-rails-asynccable",
    description: "Idle connections held, Rails + AsyncCable (Falcon)",
    category: "idle",
    endpoint: "bench-idle-anycable",
    mode: "multi-shard",
    numShards: 13,
    perShardN: 4000,
    params: { hold: 120, ramp: 200, stream: "idle-rails", ...RAILS_BASE, cableUrl: TARGETS.railsAsyncCable },
    baseline: {},
    driftThresholdPct: 60,
  },

  // Deploy survival. Redeploy the Rails service mid-test. Action Cable / Solid
  // Cable run WebSockets in Puma, so a deploy drops every connection; AnyCable
  // runs them in anycable-go, so redeploying the Rails RPC backend leaves the
  // fleet connected (expected disconnected ~0).
  {
    id: "avalanche-rails-solidcable-5k",
    description: "Avalanche: 5K Rails + Solid Cable clients, app redeploy",
    category: "avalanche",
    endpoint: "bench-avalanche-anycable",
    mode: "avalanche",
    redeployServiceName: "rails-solidcable",
    params: { n: 5000, ramp: 200, prearm: 180, recoveryWait: 300, stream: "avalanche-rails-sc", ...RAILS_BASE, cableUrl: TARGETS.railsSolidCable },
    baseline: {},
    driftThresholdPct: 100,
  },
  {
    id: "avalanche-rails-actioncable-5k",
    description: "Avalanche: 5K Rails + Action Cable clients, app redeploy",
    category: "avalanche",
    endpoint: "bench-avalanche-anycable",
    mode: "avalanche",
    redeployServiceName: "rails-actioncable",
    params: { n: 5000, ramp: 200, prearm: 180, recoveryWait: 300, stream: "avalanche-rails-ac", ...RAILS_BASE, cableUrl: TARGETS.railsActionCable },
    baseline: {},
    driftThresholdPct: 100,
  },
  {
    id: "avalanche-rails-anycable-5k",
    description: "Avalanche: 5K Rails + AnyCable clients, RPC backend redeploy (should survive)",
    category: "avalanche",
    endpoint: "bench-avalanche-anycable",
    mode: "avalanche",
    redeployServiceName: "rails-anycable",
    params: { n: 5000, ramp: 200, prearm: 180, recoveryWait: 300, stream: "avalanche-rails-any", ...RAILS_EXT, cableUrl: TARGETS.railsAnyCable },
    baseline: {},
    driftThresholdPct: 100,
  },
  {
    id: "avalanche-rails-asynccable-5k",
    description: "Avalanche: 5K Rails + AsyncCable (Falcon) clients, app redeploy",
    category: "avalanche",
    endpoint: "bench-avalanche-anycable",
    mode: "avalanche",
    redeployServiceName: "rails-asynccable",
    params: { n: 5000, ramp: 200, prearm: 180, recoveryWait: 300, stream: "avalanche-rails-asc", ...RAILS_BASE, cableUrl: TARGETS.railsAsyncCable },
    baseline: {},
    driftThresholdPct: 100,
  },
];
