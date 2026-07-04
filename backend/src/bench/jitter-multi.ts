// Multi-shard jitter / latency benchmark.
//
// Fans out one logical jitter test across k bench-runner shards. Each shard
// runs the same params with per-shard N = TOTAL / k and a unique stream so
// publishers don't fight for the same fanout path. Results are merged using
// per-shard downsampled latency samples (lib/stats.ts:mergeJitterResults)
// so the reported p50/p95/p99 reflect the true union distribution.
//
// Solves the single-bench-runner saturation ceiling. Keep per-shard N near
// 250 for latency-accurate runs; a loaded runner inflates latency AND
// deflates delivery.
//
// Usage:
//   SHARDS=https://br-1.up.railway.app,https://br-2.up.railway.app,... \
//   TOTAL=10000 RAMP_PER_SEC=200 \
//   PROTOCOL=anycable \                          # or socketio | socketio-csr | uws
//   TOTAL_MESSAGES=120 INTERVAL_MS=500 \
//   JITTER_INTERVAL=15 JITTER_DURATION=1000 \
//   SAMPLES_CAP=5000 \                            # per shard; merged ~k× that
//     tsx src/bench/jitter-multi.ts
//
// Target overrides (CABLE_URL, BROADCAST_URL, CHANNEL, AC_PROTOCOL,
// RECONNECT_MODE, RECONNECT_BASE_MS, CLIENT_LIB, SERVER_URL, UWS_WS_URL,
// UWS_HTTP_URL) are forwarded via lib/core/multi-shard.ts — one mapping
// for every multi-shard driver.

import {
  ENDPOINTS,
  parseProtocol,
  parseShardUrls,
  protocolTargetQuery,
  runMultiShard,
} from "../lib/core/multi-shard.js";

const shardUrls = parseShardUrls();
const protocol = parseProtocol();

const totalClients = parseInt(process.env.TOTAL || "10000", 10);
const perShardN = Math.ceil(totalClients / shardUrls.length);
const totalMessages = parseInt(process.env.TOTAL_MESSAGES || "120", 10);
const intervalMs = parseInt(process.env.INTERVAL_MS || "500", 10);
const rampPerSec = parseInt(process.env.RAMP_PER_SEC || "200", 10);
const durationSec = parseInt(process.env.DURATION || "160", 10);
const jitterIntervalSec = parseInt(process.env.JITTER_INTERVAL || "15", 10);
const jitterDurationMs = parseInt(process.env.JITTER_DURATION || "1000", 10);
const samplesCap = parseInt(process.env.SAMPLES_CAP || "5000", 10);

const targetQuery = protocolTargetQuery(protocol);

console.log(
  `Multi-shard jitter: ${shardUrls.length} shards × ${perShardN} = ${shardUrls.length * perShardN} clients (target: ${totalClients})`,
);
console.log(
  `Protocol:  ${protocol}   Per-shard: msgs=${totalMessages} intervalMs=${intervalMs} ramp=${rampPerSec}/s duration=${durationSec}s   Samples: ${samplesCap}/shard`,
);
console.log("");

const run = await runMultiShard({
  testType: "jitter",
  protocol,
  endpoint: ENDPOINTS.jitter[protocol],
  shardUrls,
  // Unique stream per shard so each shard's publisher fans out only to its
  // own subscribers; a shared stream would break the deliveryRate math.
  shardQuery: (i, runStamp) => ({
    n: perShardN,
    msgs: totalMessages,
    interval: intervalMs,
    ramp: rampPerSec,
    duration: durationSec,
    jitter: jitterIntervalSec,
    jitterMs: jitterDurationMs,
    samplesCap,
    stream: `jitter-multi-${runStamp}-s${i + 1}`,
    ...targetQuery,
  }),
  validity: { perShardN, expectedDurationSec: durationSec },
  meta: {
    totalClients,
    perShardN,
    totalMessages,
    intervalMs,
    rampPerSec,
    durationSec,
    jitterIntervalSec,
    jitterDurationMs,
    samplesCap,
    targetQuery,
  },
});

process.exit(run.exitCode);
