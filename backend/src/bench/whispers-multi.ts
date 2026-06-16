// Multi-shard whispers benchmark.
//
// Same workload as the single-shard whispers test (N clients × R rooms,
// fixed whisper interval), but each bench-runner shard handles N/k clients
// and the rooms span shards via a shared `roomPrefix`. The broker fans
// each whisper out to all peers regardless of which shard they live on.
//
// Why bother: at 1K clients on a single bench-runner, the test driver
// (Node event loop on one container) processes ~200K msg/sec of inbound
// whispers. AnyCable's @anycable/core client library has a heavier
// per-message decode + dispatch path than uWS's `ws.on("message")`,
// so the bench-runner clock measures library overhead, not broker
// latency. Distributing the receive load across k shards drops per-shard
// inbound rate to ~200K/k msg/sec and surfaces the broker's true
// latency. Same trick the multi-shard latency test uses (40×250).
//
// Usage:
//   SHARDS=https://br-1.up.railway.app,https://br-2.up.railway.app,... \
//   TOTAL=1000 ROOMS=10 INTERVAL_MS=500 DURATION=30 \
//   PROTOCOL=anycable  \           # or socketio | uws
//   SAMPLES_CAP=5000 \
//     tsx src/bench/whispers-multi.ts
//
// Per-protocol targets:
//   PROTOCOL=anycable → CABLE_URL=ws://anycable-go-pro.railway.internal:8080/cable
//   PROTOCOL=socketio → SERVER_URL=http://socketio-server.railway.internal:3000
//   PROTOCOL=uws      → UWS_WS_URL=ws://uws-server.railway.internal:3000/ws

import { writeFileSync } from "node:fs";
import { Agent, setGlobalDispatcher } from "undici";

import { runShards, type ShardSpec } from "../lib/core/shard-coordinator.js";
import { downsampleSorted, percentile } from "../lib/core/stats.js";
import type { WhispersResult } from "../lib/whispers-runner.js";

setGlobalDispatcher(
  new Agent({ headersTimeout: 30 * 60 * 1000, bodyTimeout: 30 * 60 * 1000 }),
);

const shardsCsv = process.env.SHARDS;
if (!shardsCsv) {
  console.error("SHARDS env var required (comma-separated bench-runner URLs)");
  process.exit(1);
}
const shardUrls = shardsCsv
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);
if (shardUrls.length === 0) {
  console.error("SHARDS must contain at least one URL");
  process.exit(1);
}

const totalClients = parseInt(process.env.TOTAL || "1000", 10);
const rooms = parseInt(process.env.ROOMS || "10", 10);
const whisperIntervalMs = parseInt(process.env.INTERVAL_MS || "500", 10);
const testDurationSec = parseInt(process.env.DURATION || "30", 10);
const payloadBytes = parseInt(process.env.PAYLOAD || "64", 10);
const rampPerSec = parseInt(process.env.RAMP_PER_SEC || "100", 10);
const samplesCap = parseInt(process.env.SAMPLES_CAP || "5000", 10);
const protocol = (process.env.PROTOCOL || "anycable").toLowerCase();

const PROTOCOL_TO_ENDPOINT: Record<string, string> = {
  anycable: "bench-whispers-anycable",
  socketio: "bench-whispers-socketio",
  uws: "bench-whispers-uws",
};
const endpoint = PROTOCOL_TO_ENDPOINT[protocol];
if (!endpoint) {
  console.error(
    `PROTOCOL must be one of: ${Object.keys(PROTOCOL_TO_ENDPOINT).join(", ")} (got "${protocol}")`,
  );
  process.exit(1);
}

const perShardN = Math.ceil(totalClients / shardUrls.length);
const realTotalClients = perShardN * shardUrls.length;

// Per-protocol target overrides forwarded to every shard.
const protocolQuery: Record<string, string> = {};
if (protocol === "anycable" && process.env.CABLE_URL) {
  protocolQuery.cableUrl = process.env.CABLE_URL;
}
if (protocol === "socketio" && process.env.SERVER_URL) {
  protocolQuery.serverUrl = process.env.SERVER_URL;
}
if (protocol === "uws" && process.env.UWS_WS_URL) {
  protocolQuery.wsUrl = process.env.UWS_WS_URL;
}

const runStamp = Date.now();
// Shared across all shards: every shard's local client `i % rooms` lands
// in the same broker room. So the broker fans out cross-shard; each
// shard's bench-runner only sees the receives for its own clients.
const roomPrefix = `whisper-multi-${runStamp}`;

console.log(
  `Multi-shard whispers: ${shardUrls.length} shards × ${perShardN} = ${realTotalClients} clients (target ${totalClients})`,
);
console.log(
  `Protocol: ${protocol}   Endpoint: /${endpoint}   Room prefix: ${roomPrefix}`,
);
console.log(
  `Per-shard: rooms=${rooms} interval=${whisperIntervalMs}ms duration=${testDurationSec}s payload=${payloadBytes}B ramp=${rampPerSec}/s`,
);
console.log(`Samples:   ${samplesCap} per shard`);
console.log("");

const shardSpecs: ShardSpec[] = shardUrls.map((url, i) => ({
  url,
  label: `shard-${i + 1}`,
  endpoint,
  query: {
    n: perShardN,
    rooms,
    ramp: rampPerSec,
    interval: whisperIntervalMs,
    duration: testDurationSec,
    payload: payloadBytes,
    roomPrefix,
    samplesCap,
    ...protocolQuery,
  },
}));

const startedAt = new Date();
const outcomes = await runShards<WhispersResult>(shardSpecs, {
  pollIntervalMs: 5000,
  pollLogLines: 30,
  printProgress: true,
});
const endedAt = new Date();

const successes = outcomes.filter(
  (o): o is typeof o & { result: WhispersResult } =>
    o.status === "done" && o.result !== undefined,
);
const failures = outcomes.filter((o) => o.status !== "done");

console.log("");
console.log(`=== Per-shard ===  (${successes.length} succeeded / ${outcomes.length})`);
for (const o of outcomes) {
  if (o.status === "done" && o.result) {
    const r = o.result;
    console.log(
      `  ${o.spec.label}: sent=${r.whispersSent} received=${r.whispersReceived} p99=${r.latencyMs.p99}ms`,
    );
  } else {
    console.log(`  ${o.spec.label}: FAILED — ${o.error || "unknown"}`);
  }
}

if (failures.length > 0) {
  console.log("");
  console.log(`!! ${failures.length} shard(s) failed; aggregate below covers the surviving ${successes.length}.`);
}

if (successes.length === 0) {
  console.error("All shards failed; no aggregate to report.");
  process.exit(1);
}

// -------------------------------------------------------------------------
// Aggregate
//
// For cross-shard whispers:
//   * each shard sees only its local clients (so per-shard `expectedReceived`
//     is wrong; it assumes peers are local-only),
//   * the true peer count for every room is `(realTotalClients / rooms) - 1`.
// Recompute expectedReceived at the coordinator:
//   expectedReceived = totalSent × (peersPerRoom_global)
// where peersPerRoom_global = (realTotalClients / rooms) - 1.

const totalSent = successes.reduce((s, o) => s + o.result.whispersSent, 0);
const totalReceived = successes.reduce(
  (s, o) => s + o.result.whispersReceived,
  0,
);
const totalInitiallyConnected = successes.reduce(
  (s, o) => s + o.result.initiallyConnected,
  0,
);

// Use the actual measured per-room peer count from connected clients, not
// the configured target, in case some shards lost connections during ramp.
const peersPerRoomGlobal = Math.max(0, totalInitiallyConnected / rooms - 1);
const expectedReceived = totalSent * peersPerRoomGlobal;
const deliveryRatePct =
  expectedReceived > 0
    ? Math.round((totalReceived / expectedReceived) * 10000) / 100
    : 0;

// Merge per-shard sorted samples into one distribution; recompute percentiles.
const mergedLatencies: number[] = [];
for (const o of successes) {
  const s = o.result.latencySamplesSorted;
  if (s) for (const v of s) mergedLatencies.push(v);
}
mergedLatencies.sort((a, b) => a - b);
const mergedSamples = downsampleSorted(mergedLatencies, samplesCap * 10);

const merged = {
  protocol,
  shards: successes.length,
  perShardN,
  totalClients: realTotalClients,
  initiallyConnected: totalInitiallyConnected,
  whisperIntervalMs,
  whispersSent: totalSent,
  whispersReceived: totalReceived,
  expectedReceived: Math.round(expectedReceived),
  deliveryRatePct,
  latencyMs: {
    p50: percentile(mergedLatencies, 50),
    p95: percentile(mergedLatencies, 95),
    p99: percentile(mergedLatencies, 99),
    max: percentile(mergedLatencies, 100),
  },
  latencySamples: mergedLatencies.length,
  totalElapsedMs: endedAt.getTime() - startedAt.getTime(),
};

console.log("");
console.log(`=== Aggregate (merged across ${successes.length} shards) ===`);
console.log(`  Clients connected: ${totalInitiallyConnected.toLocaleString()} / ${realTotalClients.toLocaleString()}`);
console.log(`  Per room peers:    ${(peersPerRoomGlobal + 1).toFixed(1)} (target ${realTotalClients / rooms})`);
console.log(`  Whispers sent:     ${totalSent.toLocaleString()}`);
console.log(`  Whispers received: ${totalReceived.toLocaleString()}`);
console.log(`  Expected received: ${Math.round(expectedReceived).toLocaleString()}`);
console.log(`  Delivery rate:     ${deliveryRatePct}%`);
console.log(
  `  Latency:           p50=${merged.latencyMs.p50}ms  p95=${merged.latencyMs.p95}ms  p99=${merged.latencyMs.p99}ms  max=${merged.latencyMs.max}ms  (n=${mergedLatencies.length})`,
);

const outPath = `whispers-multi-${protocol}-${shardUrls.length}x${perShardN}-${startedAt.toISOString().replace(/[:.]/g, "-")}.json`;
writeFileSync(
  outPath,
  JSON.stringify(
    {
      protocol,
      shards: shardUrls.length,
      perShardN,
      totalClients: realTotalClients,
      startedAt: startedAt.toISOString(),
      endedAt: endedAt.toISOString(),
      params: {
        totalClients,
        rooms,
        whisperIntervalMs,
        testDurationSec,
        payloadBytes,
        samplesCap,
      },
      perShard: outcomes.map((o) => ({
        label: o.spec.label,
        url: o.spec.url,
        status: o.status,
        jobId: o.jobId,
        durationMs: o.durationMs,
        result: o.result,
        error: o.error,
      })),
      merged: { ...merged, latencySamplesSorted: mergedSamples },
    },
    null,
    2,
  ),
);
console.log(`\nWrote merged JSON: ${outPath}`);

process.exit(deliveryRatePct < 99 ? 1 : 0);
