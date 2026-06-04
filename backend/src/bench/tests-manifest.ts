// Manifest of every test currently on the compare/socket-io page.
//
// `npm run bench:rebaseline` walks this list, hits each bench-runner
// endpoint, writes the result JSON to tmp/v1.6.14-bench-results/, and
// prints a delta vs `baseline`. Pick a subset with FILTER=<substring>.
//
// Baselines are the "as-measured today" snapshot, not the page numbers
// (the page numbers were taken under heavier Railway-shared infrastructure
// load; latency baselines were uniformly ~50% lower at this refresh).
// Update `baseline` here when the underlying setup changes or a real
// drift gets accepted as the new floor.

export type TestCategory =
  | "latency"
  | "jitter"
  | "whispers"
  | "throughput"
  | "idle";

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
  mode: "sync" | "async" | "multi-shard";
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
}

// Internal Railway targets used by the manifest. Bench-runners are on
// Railway's internal network, so these *.railway.internal URLs work
// from inside a bench-runner container.
const TARGETS = {
  socketio: "http://socketio-server.railway.internal:3000",
  // socketio-server-small is the CSR-enabled standalone Socket.io
  // (SOCKETIO_CSR=1 env at boot). The default socketio-server runs
  // without CSR; both modes can't coexist in one process.
  socketioCsr: "http://socketio-server-small.railway.internal:3000",
  uwsWs: "ws://uws-server.railway.internal:3000/ws",
  uwsHttp: "http://uws-server.railway.internal:3000",
  anycableOss: "ws://anycable-go.railway.internal:8080/cable",
  anycableOssBroadcast: "http://anycable-go.railway.internal:8080/_broadcast",
  anycablePro: "ws://anycable-go-pro.railway.internal:8080/cable",
  anycableProBroadcast:
    "http://anycable-go-pro.railway.internal:8080/_broadcast",
};

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
    // As-measured today the server can only absorb ~25% of the reconnect
    // storm; the page baseline of 87% was from a moment with more headroom
    // on the Railway-shared infra. Wide threshold so day-to-day variance
    // doesn't flag, but a real drop will still surface.
    baseline: { deliveryRatePct: 27.5, "latencyRawMs.p95": 290 },
    driftThresholdPct: 30,
  },
  {
    id: "jitter-socketio-csr-10k",
    description: "Reliability under WiFi jitter, Socket.io + CSR, 10K (CSR server)",
    category: "jitter",
    endpoint: "bench-jitter-socketio-csr",
    mode: "async",
    params: { n: 10000, ...JITTER_10K, serverUrl: TARGETS.socketioCsr, samplesCap: 5000 },
    // CSR's replay resumes ~20% of disconnects cleanly; the rest fall back
    // to "live from now" so 75% delivery, 0 in-gap loss, but the p95/p99
    // latency tail is huge because resumed messages carry replay delay.
    baseline: { deliveryRatePct: 75.5, lostDeliveries: 0, "latencyRawMs.p95": 80000, csrResumes: 15000 },
    driftThresholdPct: 30,
  },
  {
    id: "jitter-uws-10k",
    description: "Reliability under WiFi jitter, uWS topics, 10K",
    category: "jitter",
    endpoint: "bench-jitter-uws",
    mode: "async",
    params: { n: 10000, ...JITTER_10K, wsUrl: TARGETS.uwsWs, httpUrl: TARGETS.uwsHttp, samplesCap: 5000 },
    // No connect failures but many clients silently drop their highestSeq;
    // at-most-once + uws-server backpressure together. As-measured baseline.
    baseline: { deliveryRatePct: 34.5, lostDeliveries: 134000 },
    driftThresholdPct: 30,
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

  // -------------------------------------------------------------------------
  // Whispers (1K × 10 rooms, 100 peers/room)
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
    baseline: { deliveryRatePct: 100, "latencyRawMs.p50": 365, "latencyRawMs.p99": 3927 },
    driftThresholdPct: 20,
  },

  // -------------------------------------------------------------------------
  // Idle capacity (multi-shard; gated behind INCLUDE_IDLE=1 in rebaseline
  // because each test fans out across 50 bench-runner replicas)
  // -------------------------------------------------------------------------
  {
    id: "idle-socketio",
    description: "Idle connections held, default Socket.io, 1M target",
    category: "idle",
    endpoint: "bench-idle-socketio",
    mode: "multi-shard",
    numShards: 50,
    perShardN: 20000,
    params: { hold: 120, ramp: 200, stream: "idle-rebaseline", serverUrl: TARGETS.socketio },
    // Socket.io tops out around 120K because the Node event loop saturates
    // on handshakes long before reaching 1M.
    baseline: { connected: 119826 },
    driftThresholdPct: 20,
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
    baseline: { connected: 821877 },
    driftThresholdPct: 10,
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
    baseline: { connected: 822037 },
    driftThresholdPct: 10,
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
    // uWS is the only option that holds the full 1M and a bit more.
    baseline: { connected: 1018366 },
    driftThresholdPct: 10,
  },
];
