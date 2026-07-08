// Multi-shard idle-connection benchmark.
//
// Fans out to N bench-runner shards in parallel — each shard is a separate
// Railway container with its own source IP and outbound port pool, so
// they don't share the ~64K ephemeral-port ceiling that limits a single
// container. Total connections = sum of per-shard counts.
//
// Runs through the async job protocol (enqueue + poll) like the other
// multi drivers, so long ramps and holds never hit Railway's 5-minute
// edge timeout, and the params echo verifies each shard parsed what we
// sent before the run starts.
//
// After the shards finish, queries Railway metrics over the test window
// and prints ASCII charts of memory + CPU on the target service plus
// a CSV file for offline plotting.
//
// Usage:
//   SHARDS=https://bench-runner-2.up.railway.app,... \
//   PER_SHARD_N=10000 HOLD_SEC=120 RAMP_PER_SEC=200 \
//   TARGET=anycable \                # or socketio | uws
//   PROJECT_ID=<uuid> SERVICE_ID=<target-uuid> SERVICE_NAME=anycable-go \
//     tsx src/bench/idle-multi.ts
//
// Metrics env vars are optional — without PROJECT_ID and SERVICE_ID the
// script skips the chart and only reports aggregate counts.
//
// Publishing rule: a capacity number is only a server ceiling when the
// failure is on the server side. Shards that all stop at the same count
// below PER_SHARD_N hit the load generator's wall — the script flags this
// and such a run must be re-run with more shards, never published.

import { writeFileSync } from "fs";

import type { IdleResult } from "../lib/idle-runner.js";
import { fetchMetric, readRailwayToken } from "../lib/core/railway-api.js";
import { chart } from "../lib/core/chart.js";
import { parseShardUrls } from "../lib/core/multi-shard.js";
import { checkShardHealth } from "../lib/core/multi-shard.js";
import { resultPath } from "../lib/core/results-dir.js";
import { runShards, type ShardSpec } from "../lib/core/shard-coordinator.js";
import { percentile } from "../lib/core/stats.js";

const shardUrls = parseShardUrls();

const perShardN = parseInt(process.env.PER_SHARD_N || "10000", 10);
const holdSec = parseInt(process.env.HOLD_SEC || "120", 10);
const rampPerSec = parseInt(process.env.RAMP_PER_SEC || "200", 10);
const stream = process.env.STREAM || "idle-probe";

const target = (process.env.TARGET || "anycable").toLowerCase();
if (target !== "anycable" && target !== "socketio" && target !== "uws") {
  console.error(`TARGET must be "anycable", "socketio", or "uws" (got "${target}")`);
  process.exit(1);
}
const endpoint =
  target === "socketio"
    ? "bench-idle-socketio"
    : target === "uws"
      ? "bench-idle-uws"
      : "bench-idle-anycable";

// Target overrides, same names as every other driver.
const targetQuery: Record<string, string> = {};
if (target === "anycable") {
  if (process.env.CABLE_URL) targetQuery.cableUrl = process.env.CABLE_URL;
  if (process.env.CHANNEL) targetQuery.channel = process.env.CHANNEL;
  if (process.env.AC_PROTOCOL) targetQuery.acProtocol = process.env.AC_PROTOCOL;
}
if (target === "socketio" && process.env.SERVER_URL)
  targetQuery.serverUrl = process.env.SERVER_URL;
if (target === "uws" && process.env.UWS_WS_URL)
  targetQuery.wsUrl = process.env.UWS_WS_URL;

const totalTarget = perShardN * shardUrls.length;

console.log(
  `Idle multi-shard test: ${shardUrls.length} shards × ${perShardN} = ${totalTarget} connections`,
);
console.log(`Target: ${target}  Hold: ${holdSec}s  Ramp: ${rampPerSec}/s per shard\n`);

console.log(`Health sweep: ${shardUrls.length} shard(s)...`);
const health = await checkShardHealth(shardUrls);
const bad = health.filter((h) => !h.ok);
for (const h of bad) console.error(`  ✗ ${h.url}: ${h.detail}`);
if (bad.length > 0) {
  console.error(
    `${bad.length}/${shardUrls.length} shard(s) unhealthy. Fix or drop them from SHARDS before burning a run.`,
  );
  process.exit(1);
}
console.log(`  ✓ all ${shardUrls.length} shards healthy\n`);

// Ramp + hold bounds the wall clock; pad generously for connection retries.
const shardTimeoutMs = parseInt(
  process.env.SHARD_TIMEOUT_MS ||
    String((Math.ceil(perShardN / rampPerSec) + holdSec + 300) * 1000),
  10,
);

const specs: ShardSpec[] = shardUrls.map((url, i) => ({
  url,
  label: `shard-${i + 1}`,
  endpoint,
  query: {
    n: perShardN,
    hold: holdSec,
    ramp: rampPerSec,
    stream,
    shard: `shard-${i + 1}`,
    ...targetQuery,
  },
}));

const startedAt = new Date();
const startedAtIso = startedAt.toISOString();
console.log(`Test started at ${startedAtIso}\n`);

const outcomes = await runShards<IdleResult>(specs, {
  pollIntervalMs: 5000,
  pollLogLines: 10,
  printProgress: true,
  shardTimeoutMs,
});

const endedAt = new Date();
const endedAtIso = endedAt.toISOString();

// -------------------------------------------------------------------------
// Aggregate

const results: IdleResult[] = [];
const errors: { label: string; reason: string }[] = [];
for (const o of outcomes) {
  if (o.status === "done" && o.result) results.push(o.result);
  else errors.push({ label: o.spec.label, reason: (o.error || "unknown").slice(0, 200) });
}

const totals = results.reduce(
  (acc, r) => ({
    connected: acc.connected + r.connected,
    welcomed: acc.welcomed + r.welcomed,
    subscribed: acc.subscribed + r.subscribed,
    failed: acc.failed + r.failed,
  }),
  { connected: 0, welcomed: 0, subscribed: 0, failed: 0 },
);

if (errors.length > 0) {
  console.log(
    `\n${errors.length} shard(s) errored or timed out — totals below cover the ${results.length} surviving shard(s).`,
  );
}

console.log(`\n=== Aggregate ===`);
console.log(`  Connected:    ${totals.connected.toLocaleString()} / ${totalTarget.toLocaleString()}`);
console.log(`  Welcomed:     ${totals.welcomed.toLocaleString()}`);
console.log(`  Subscribed:   ${totals.subscribed.toLocaleString()}`);
console.log(`  Failed:       ${totals.failed.toLocaleString()}`);
console.log(`  Test window:  ${startedAtIso} → ${endedAtIso}`);

// Validity: the uniform-shard-ceiling signature. Every shard freezing at
// the same count below PER_SHARD_N is the load generator's ephemeral-port
// or event-loop wall (the "exactly 12,002 per shard" bug class), never a
// server ceiling. Name the stop condition or re-run with more shards.
let generatorLimited = false;
if (results.length >= 2) {
  const connected = results.map((r) => r.connected);
  const allEqual = connected.every((c) => Math.abs(c - connected[0]) <= 5);
  if (allEqual && connected[0] < perShardN * 0.99) {
    generatorLimited = true;
    console.log(
      `\n[FATAL] uniform-shard-ceiling: every shard stopped at ~${connected[0]} of ${perShardN} requested.`,
    );
    console.log(
      `  This is the load generator's wall, not the server's. Add shards or lower PER_SHARD_N; do not publish this as a capacity number.`,
    );
  }
}

// -------------------------------------------------------------------------
// Optional: Railway metrics + chart

const projectId = process.env.PROJECT_ID;
const serviceId = process.env.SERVICE_ID;
const serviceName = process.env.SERVICE_NAME || "anycable-go";

if (!projectId || !serviceId) {
  console.log(
    "\n(set PROJECT_ID and SERVICE_ID to chart memory/CPU on the target)",
  );
  process.exit(generatorLimited ? 2 : 0);
}

const token = readRailwayToken();

// Pad the window slightly on each side so we capture the ramp-up and tail.
const padMs = 30 * 1000;
const windowStart = new Date(startedAt.getTime() - padMs).toISOString();
const windowEnd = new Date(endedAt.getTime() + padMs).toISOString();

console.log(
  `\nFetching Railway metrics for ${serviceName} over [${windowStart}, ${windowEnd}]...`,
);

const [memPoints, cpuPoints] = await Promise.all([
  fetchMetric({
    token,
    projectId,
    serviceId,
    measurement: "MEMORY_USAGE_GB",
    startDate: windowStart,
    endDate: windowEnd,
  }),
  fetchMetric({
    token,
    projectId,
    serviceId,
    measurement: "CPU_USAGE",
    startDate: windowStart,
    endDate: windowEnd,
  }),
]);

if (memPoints.length === 0 && cpuPoints.length === 0) {
  console.log("(no metrics returned — wrong service id, or window too short?)");
  process.exit(generatorLimited ? 2 : 0);
}

// Convert to seconds-since-test-start.
const startUnix = Math.floor(startedAt.getTime() / 1000);
const memSeries = memPoints.map((p) => ({ tSec: p.ts - startUnix, value: p.value * 1024 }));
const cpuSeries = cpuPoints.map((p) => ({ tSec: p.ts - startUnix, value: p.value }));

console.log(
  `\n=== ${serviceName} during the test ===  (n=${memPoints.length} samples)\n`,
);

console.log(chart({ title: `Memory`, points: memSeries, height: 12, width: 60, yUnit: "MB" }));
console.log("");
console.log(chart({ title: `CPU (% of 1 vCPU)`, points: cpuSeries, height: 8, width: 60, yUnit: "%" }));

const memValues = memSeries.map((p) => p.value).sort((a, b) => a - b);
const cpuValues = cpuSeries.map((p) => p.value).sort((a, b) => a - b);

console.log(`\nMemory: peak=${percentile(memValues, 100).toFixed(0)} MB  p95=${percentile(memValues, 95).toFixed(0)} MB  avg=${(memValues.reduce((s, n) => s + n, 0) / Math.max(1, memValues.length)).toFixed(0)} MB`);
console.log(`CPU:    peak=${percentile(cpuValues, 100).toFixed(2)} %   p95=${percentile(cpuValues, 95).toFixed(2)} %   avg=${(cpuValues.reduce((s, n) => s + n, 0) / Math.max(1, cpuValues.length)).toFixed(2)} %`);

// RAM per connection while held: the metric that stays valid even when the
// fleet caps below the target scale (matched-scale efficiency).
if (totals.connected > 0 && memValues.length > 0) {
  const peakMb = percentile(memValues, 100);
  console.log(
    `RAM/conn (peak):   ${((peakMb * 1024) / totals.connected).toFixed(1)} KB across ${totals.connected.toLocaleString()} connections`,
  );
}

// CSV: tSec, mem_mb, cpu_pct (joined on closest sample timestamp).
const csvPath = resultPath(`idle-multi-${startedAt.toISOString().replace(/[:.]/g, "-")}.csv`);
const lines = ["t_sec,memory_mb,cpu_pct"];
const allTs = Array.from(
  new Set([...memSeries.map((p) => p.tSec), ...cpuSeries.map((p) => p.tSec)])
).sort((a, b) => a - b);
for (const t of allTs) {
  const m = memSeries.find((p) => p.tSec === t)?.value;
  const c = cpuSeries.find((p) => p.tSec === t)?.value;
  lines.push(`${t},${m !== undefined ? m.toFixed(0) : ""},${c !== undefined ? c.toFixed(2) : ""}`);
}
writeFileSync(csvPath, lines.join("\n") + "\n");
console.log(`\nWrote time-series CSV: ${csvPath}`);

process.exit(generatorLimited ? 2 : 0);
