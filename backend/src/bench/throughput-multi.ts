// Multi-shard throughput benchmark.
//
// Splits N subscribers across k shards; each shard publishes its own stream
// and receives its own fanout, keeping every runner under saturation while
// total server-side delivery work matches the single-shard test.
//
// Caveat: server-side per-broadcast overhead (e.g. AnyCable's broker write)
// happens k times more often than in single-shard, because each shard's
// publisher writes its own stream. Treat the numbers as "per-protocol
// fanout cost at N/k subs, parallelized across k streams" (see the
// footnote in the throughput table). Rate-matched: per-shard messages stay
// TOTAL_MESSAGES so the aggregate publish rate scales with k by design —
// state the shard count next to any published number.
//
// Usage:
//   SHARDS=https://br-1.up.railway.app,... \
//   TOTAL=10000 RAMP_PER_SEC=200 \
//   PROTOCOL=anycable \                    # or socketio | socketio-csr | uws
//   TOTAL_MESSAGES=100 INTERVAL_MS=10 \
//   PUBLISHER=pool PUBLISHER_CONCURRENCY=16 \
//   DRAIN_SEC=30 SAMPLES_CAP=5000 \
//     tsx src/bench/throughput-multi.ts
//
// Target overrides come from lib/core/multi-shard.ts (CABLE_URL,
// BROADCAST_URL, CHANNEL, AC_PROTOCOL, SERVER_URL, UWS_WS_URL, UWS_HTTP_URL).

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
const totalMessages = parseInt(process.env.TOTAL_MESSAGES || "100", 10);
const intervalMs = parseInt(process.env.INTERVAL_MS || "10", 10);
const rampPerSec = parseInt(process.env.RAMP_PER_SEC || "200", 10);
const drainSec = parseInt(process.env.DRAIN_SEC || "30", 10);
const samplesCap = parseInt(process.env.SAMPLES_CAP || "5000", 10);
const publisher = (process.env.PUBLISHER || "pool").trim();
const publisherConcurrency = parseInt(
  process.env.PUBLISHER_CONCURRENCY || "16",
  10,
);

const targetQuery = protocolTargetQuery(protocol);

console.log(
  `Multi-shard throughput: ${shardUrls.length} shards × ${perShardN} = ${shardUrls.length * perShardN} clients (target: ${totalClients})`,
);
console.log(
  `Protocol:  ${protocol}   Per-shard: total=${totalMessages} intervalMs=${intervalMs} ramp=${rampPerSec}/s drain=${drainSec}s   Publisher: ${publisher}×${publisherConcurrency}`,
);
console.log("");

const run = await runMultiShard({
  testType: "throughput",
  protocol,
  endpoint: ENDPOINTS.throughput[protocol],
  shardUrls,
  // Runner reads `intervalMs` (throughputParamsFromQuery). If this key ever
  // drifts, the params echo in the enqueue response fails the shard before
  // the run starts — that is the guard this exact line once needed.
  shardQuery: (i, runStamp) => ({
    n: perShardN,
    total: totalMessages,
    intervalMs,
    ramp: rampPerSec,
    drain: drainSec,
    publisher,
    publisherConcurrency,
    samplesCap,
    stream: `tp-multi-${runStamp}-s${i + 1}`,
    ...targetQuery,
  }),
  validity: { perShardN },
  meta: {
    totalClients,
    perShardN,
    totalMessages,
    intervalMs,
    rampPerSec,
    drainSec,
    publisher,
    publisherConcurrency,
    samplesCap,
    targetQuery,
  },
});

process.exit(run.exitCode);
