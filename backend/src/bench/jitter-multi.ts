// Multi-shard jitter / latency benchmark.
//
// Fans out one logical jitter test across k bench-runner shards. Each shard
// runs the same params with per-shard N = TOTAL / k and a unique stream so
// publishers don't fight for the same fanout path. Results are merged using
// per-shard downsampled latency samples (lib/stats.ts:mergeJitterResults)
// so the reported p50/p95/p99 reflect the true union distribution.
//
// Solves the single-bench-runner ~50K ceiling: 4 shards × 25K = 100K subs
// without saturating any single event loop.
//
// Usage:
//   SHARDS=https://br-1.up.railway.app,https://br-2.up.railway.app,... \
//   TOTAL=100000 RAMP_PER_SEC=200 \
//   PROTOCOL=anycable \                          # or socketio | socketio-csr | uws
//   TOTAL_MESSAGES=120 INTERVAL_MS=500 \
//   JITTER_INTERVAL=15 JITTER_DURATION=1000 \
//   SAMPLES_CAP=5000 \                            # per shard; merged ~k× that
//     tsx src/bench/jitter-multi.ts
//
// Per-protocol targets are picked up from the same env vars as the
// single-shard drivers (CABLE_URL, SERVER_URL, UWS_WS_URL, UWS_HTTP_URL)
// and forwarded to each shard as query params.

import { writeFileSync } from "node:fs";
import { Agent, setGlobalDispatcher } from "undici";

import { runShards, type ShardSpec } from "../lib/core/shard-coordinator.js";
import {
  formatHumanReport,
  mergeJitterResults,
  type JitterResult,
} from "../lib/core/stats.js";

// The coordinator does short polls, but the cumulative wait is bounded by
// the longest shard. Push the default fetch timeouts up so the enqueue +
// poll fetches never give up before Railway does.
setGlobalDispatcher(
  new Agent({ headersTimeout: 30 * 60 * 1000, bodyTimeout: 30 * 60 * 1000 }),
);

const shardsCsv = process.env.SHARDS;
if (!shardsCsv) {
  console.error(
    "SHARDS env var required (comma-separated bench-runner base URLs)",
  );
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

const totalClients = parseInt(process.env.TOTAL || "10000", 10);
const perShardN = Math.ceil(totalClients / shardUrls.length);
const totalMessages = parseInt(process.env.TOTAL_MESSAGES || "120", 10);
const intervalMs = parseInt(process.env.INTERVAL_MS || "500", 10);
const rampPerSec = parseInt(process.env.RAMP_PER_SEC || "200", 10);
const durationSec = parseInt(process.env.DURATION || "160", 10);
const jitterIntervalSec = parseInt(process.env.JITTER_INTERVAL || "15", 10);
const jitterDurationMs = parseInt(process.env.JITTER_DURATION || "1000", 10);
const samplesCap = parseInt(process.env.SAMPLES_CAP || "5000", 10);
const protocol = (process.env.PROTOCOL || "anycable").toLowerCase();

const PROTOCOL_TO_ENDPOINT: Record<string, string> = {
  anycable: "bench-jitter-anycable",
  socketio: "bench-jitter-socketio",
  "socketio-csr": "bench-jitter-socketio-csr",
  uws: "bench-jitter-uws",
};
const endpoint = PROTOCOL_TO_ENDPOINT[protocol];
if (!endpoint) {
  console.error(
    `PROTOCOL must be one of: ${Object.keys(PROTOCOL_TO_ENDPOINT).join(", ")} (got "${protocol}")`,
  );
  process.exit(1);
}

// Per-protocol target overrides forwarded to each shard. The bench-runner
// already supports these via its own query-string parsers.
const protocolQuery: Record<string, string> = {};
if (protocol === "anycable") {
  if (process.env.CABLE_URL) protocolQuery.cableUrl = process.env.CABLE_URL;
  if (process.env.BROADCAST_URL)
    protocolQuery.broadcastUrl = process.env.BROADCAST_URL;
  // Rails targets subscribe to a real channel over the base or extended
  // Action Cable wire protocol; the nodejs $pubsub targets leave these unset.
  if (process.env.CHANNEL) protocolQuery.channel = process.env.CHANNEL;
  if (process.env.AC_PROTOCOL) protocolQuery.acProtocol = process.env.AC_PROTOCOL;
}
if (protocol === "socketio" || protocol === "socketio-csr") {
  if (process.env.SERVER_URL) protocolQuery.serverUrl = process.env.SERVER_URL;
}
if (protocol === "uws") {
  if (process.env.UWS_WS_URL) protocolQuery.wsUrl = process.env.UWS_WS_URL;
  if (process.env.UWS_HTTP_URL) protocolQuery.httpUrl = process.env.UWS_HTTP_URL;
}

console.log(
  `Multi-shard jitter: ${shardUrls.length} shards × ${perShardN} = ${shardUrls.length * perShardN} clients (target: ${totalClients})`,
);
console.log(
  `Protocol:  ${protocol}   Endpoint: /${endpoint}   Stream prefix: jitter-multi-${Date.now()}`,
);
console.log(
  `Per-shard: msgs=${totalMessages} intervalMs=${intervalMs} ramp=${rampPerSec}/s duration=${durationSec}s`,
);
console.log(`Samples:   ${samplesCap} per shard`);
console.log("");

// Unique stream per shard so each shard's publisher fans out only to its
// own subscribers. If we shared a stream across shards, every shard's
// clients would receive all shards' publishes and the deliveryRate math
// would be wrong.
const runStamp = Date.now();
const shardSpecs: ShardSpec[] = shardUrls.map((url, i) => ({
  url,
  label: `shard-${i + 1}`,
  endpoint,
  query: {
    n: perShardN,
    msgs: totalMessages,
    interval: intervalMs,
    ramp: rampPerSec,
    duration: durationSec,
    jitter: jitterIntervalSec,
    jitterMs: jitterDurationMs,
    samplesCap,
    stream: `jitter-multi-${runStamp}-s${i + 1}`,
    ...protocolQuery,
  },
}));

const startedAt = new Date();
const outcomes = await runShards<JitterResult>(shardSpecs, {
  pollIntervalMs: 5000,
  pollLogLines: 30,
  printProgress: true,
});
const endedAt = new Date();

const successes = outcomes.filter(
  (o): o is typeof o & { result: JitterResult } =>
    o.status === "done" && o.result !== undefined,
);
const failures = outcomes.filter((o) => o.status !== "done");

console.log("");
console.log(`=== Per-shard ===  (${successes.length} succeeded / ${outcomes.length})`);
for (const o of outcomes) {
  if (o.status === "done" && o.result) {
    const r = o.result;
    console.log(
      `  ${o.spec.label}: clients=${r.clients} delivery=${r.deliveryRatePct}% p99=${r.latencyOverMinMs.p99}ms (${(o.durationMs / 1000).toFixed(1)}s)`,
    );
  } else {
    console.log(`  ${o.spec.label}: FAILED — ${o.error || "unknown"}`);
  }
}

if (failures.length > 0) {
  console.log("");
  console.log(`!! ${failures.length} shard(s) failed; reported merge covers the remaining ${successes.length}.`);
}

if (successes.length === 0) {
  console.error("All shards failed; no merged result to report.");
  process.exit(1);
}

const merged = mergeJitterResults(
  `${protocol}-multi-${shardUrls.length}x${perShardN}`,
  successes.map((o) => o.result),
);

console.log(formatHumanReport(`Merged (${shardUrls.length} shards)`, merged));

// Drop a JSON file for the manifest tooling (#23) to pick up later.
const outPath = `jitter-multi-${protocol}-${shardUrls.length}x${perShardN}-${startedAt.toISOString().replace(/[:.]/g, "-")}.json`;
writeFileSync(
  outPath,
  JSON.stringify(
    {
      protocol,
      shards: shardUrls.length,
      perShardN,
      totalClients: shardUrls.length * perShardN,
      startedAt: startedAt.toISOString(),
      endedAt: endedAt.toISOString(),
      params: {
        totalMessages,
        intervalMs,
        rampPerSec,
        durationSec,
        jitterIntervalSec,
        jitterDurationMs,
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
      merged,
    },
    null,
    2,
  ),
);
console.log(`\nWrote merged JSON: ${outPath}`);

process.exit(merged.lostDeliveries > 0 ? 1 : 0);
