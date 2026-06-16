// Multi-shard throughput benchmark — parallel to jitter-multi.ts.
//
// The single-shard throughput test at 10K subscribers saturates the
// bench-runner Node.js event loop on @anycable/core, socket.io-client,
// or WS frame parsing, depending on protocol. That saturation inflates
// every protocol's measured p99 by a comparable amount — but unevenly
// across protocols (worst for @anycable/core), which distorts the
// cross-protocol comparison.
//
// Fix: split N subscribers across k shards (k×N/k), each shard
// publishes its own stream and receives its own fanout. Per-shard
// runtime stays under saturation; total server-side delivery work
// matches the single-shard test (k × per-shard fanout = N total
// deliveries per broadcast cycle).
//
// Caveat: server-side per-broadcast overhead (e.g. AnyCable's broker
// write) happens k times more often than in single-shard, because each
// shard's publisher writes its own stream. For the page, treat the
// numbers here as "per-protocol fanout cost at 2500 subs, parallelized
// across k streams". They're closer to truth than single-shard 10K but
// not identical to "one fanout to 10K subs". See the footnote in the
// throughput table.
//
// Usage:
//   SHARDS=https://br-1.up.railway.app,https://br-2.up.railway.app,... \
//   TOTAL=10000 RAMP_PER_SEC=200 \
//   PROTOCOL=anycable \                    # or socketio | socketio-csr | uws
//   TOTAL_MESSAGES=100 INTERVAL_MS=10 \    # default 100 msgs × 10K subs = 1M
//   PUBLISHER=pool PUBLISHER_CONCURRENCY=16 \
//   DRAIN_SEC=30 SAMPLES_CAP=5000 \
//   CABLE_URL=... BROADCAST_URL=... SERVER_URL=... UWS_WS_URL=... UWS_HTTP_URL=... \
//     tsx src/bench/throughput-multi.ts

import { writeFileSync } from "node:fs";
import { Agent, setGlobalDispatcher } from "undici";

import { resultPath } from "../lib/core/results-dir.js";
import { runShards, type ShardSpec } from "../lib/core/shard-coordinator.js";
import {
  formatHumanReport,
  mergeJitterResults,
  type JitterResult,
} from "../lib/core/stats.js";

// Throughput runs are bounded by the slowest shard; bump fetch timeouts
// past Railway's proxy ceiling so the coordinator never times out before
// the work does.
setGlobalDispatcher(
  new Agent({ headersTimeout: 30 * 60 * 1000, bodyTimeout: 30 * 60 * 1000 }),
);

const shardsCsv = process.env.SHARDS;
if (!shardsCsv) {
  console.error("SHARDS env var required (comma-separated bench-runner base URLs)");
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
const protocol = (process.env.PROTOCOL || "anycable").toLowerCase();

const PROTOCOL_TO_ENDPOINT: Record<string, string> = {
  anycable: "bench-throughput-anycable",
  socketio: "bench-throughput-socketio",
  "socketio-csr": "bench-throughput-socketio-csr",
  uws: "bench-throughput-uws",
};
const endpoint = PROTOCOL_TO_ENDPOINT[protocol];
if (!endpoint) {
  console.error(
    `PROTOCOL must be one of: ${Object.keys(PROTOCOL_TO_ENDPOINT).join(", ")} (got "${protocol}")`,
  );
  process.exit(1);
}

// Per-protocol URL overrides, forwarded to each shard. Mirrors the
// surface that jitter-multi.ts exposes.
const protocolQuery: Record<string, string | number> = {};
if (protocol === "anycable") {
  if (process.env.CABLE_URL) protocolQuery.cableUrl = process.env.CABLE_URL;
  if (process.env.BROADCAST_URL)
    protocolQuery.broadcastUrl = process.env.BROADCAST_URL;
}
if (protocol === "socketio" || protocol === "socketio-csr") {
  if (process.env.SERVER_URL) protocolQuery.serverUrl = process.env.SERVER_URL;
}
if (protocol === "uws") {
  if (process.env.UWS_WS_URL) protocolQuery.wsUrl = process.env.UWS_WS_URL;
  if (process.env.UWS_HTTP_URL) protocolQuery.httpUrl = process.env.UWS_HTTP_URL;
}

console.log(
  `Multi-shard throughput: ${shardUrls.length} shards × ${perShardN} = ${shardUrls.length * perShardN} clients (target: ${totalClients})`,
);
console.log(
  `Protocol:  ${protocol}   Endpoint: /${endpoint}   Stream prefix: tp-multi-${Date.now()}`,
);
console.log(
  `Per-shard: total=${totalMessages} intervalMs=${intervalMs} ramp=${rampPerSec}/s drain=${drainSec}s`,
);
console.log(
  `Publisher: ${publisher} (concurrency ${publisherConcurrency})    Samples: ${samplesCap} per shard`,
);
console.log("");

// Each shard owns its own stream so its publisher's broadcasts fan out
// only to its own subs. Shared streams would inflate every shard's
// deliveryRate denominator with other shards' work.
const runStamp = Date.now();
const shardSpecs: ShardSpec[] = shardUrls.map((url, i) => ({
  url,
  label: `shard-${i + 1}`,
  endpoint,
  query: {
    n: perShardN,
    total: totalMessages,
    interval: intervalMs,
    ramp: rampPerSec,
    drain: drainSec,
    publisher,
    publisherConcurrency,
    samplesCap,
    stream: `tp-multi-${runStamp}-s${i + 1}`,
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
      `  ${o.spec.label}: clients=${r.clients} delivery=${r.deliveryRatePct}% p50=${r.latencyOverMinMs.p50}ms p99=${r.latencyOverMinMs.p99}ms (${(o.durationMs / 1000).toFixed(1)}s)`,
    );
  } else {
    console.log(`  ${o.spec.label}: FAILED — ${o.error || "unknown"}`);
  }
}

if (failures.length > 0) {
  console.log("");
  console.log(
    `!! ${failures.length} shard(s) failed; reported merge covers the remaining ${successes.length}.`,
  );
}

if (successes.length === 0) {
  console.error("All shards failed; no merged result to report.");
  process.exit(1);
}

// stats.ts:mergeJitterResults requires latencySamplesSorted on every
// shard. Sync-mode shards omit it. We pass samplesCap above so async
// shards include it, but log a friendly hint if a sync shard slipped
// through.
try {
  const merged = mergeJitterResults(
    `${protocol}-multi-${shardUrls.length}x${perShardN}`,
    successes.map((o) => o.result),
  );
  console.log(formatHumanReport(`Merged (${shardUrls.length} shards)`, merged));

  const outPath = resultPath(
    `throughput-multi-${protocol}-${shardUrls.length}x${perShardN}-${startedAt.toISOString().replace(/[:.]/g, "-")}.json`,
  );
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
          drainSec,
          publisher,
          publisherConcurrency,
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
} catch (e) {
  console.error(
    `\nMerge failed (${(e as Error).message}); per-shard summaries above are still valid.`,
  );
  process.exit(0);
}
