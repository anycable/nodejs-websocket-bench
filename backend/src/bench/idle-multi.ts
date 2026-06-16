// Multi-shard idle-connection benchmark.
//
// Fans out to N bench-runner shards in parallel — each shard is a separate
// Railway container with its own source IP and outbound port pool, so
// they don't share the ~64K ephemeral-port ceiling that limits a single
// container. Total connections = sum of per-shard counts.
//
// After the shards finish, queries Railway metrics over the test window
// and prints ASCII charts of memory + CPU on the anycable-go service plus
// a CSV file (idle-multi-<timestamp>.csv) for offline plotting.
//
// Usage:
//   SHARDS=https://bench-runner-1.up.railway.app,https://bench-runner-2.up.railway.app,... \
//   PER_SHARD_N=25000 HOLD_SEC=120 RAMP_PER_SEC=200 \
//   PROJECT_ID=<uuid> SERVICE_ID=<anycable-go-uuid> SERVICE_NAME=anycable-go \
//     tsx src/bench/idle-multi.ts
//
// All metrics-related env vars are optional — if PROJECT_ID and SERVICE_ID
// aren't set, the script skips the chart and only reports aggregate counts.

import { writeFileSync } from "fs";
import { Agent, setGlobalDispatcher } from "undici";

import type { IdleResult } from "../lib/idle-runner.js";
import { fetchMetric, readRailwayToken } from "../lib/core/railway-api.js";
import { chart } from "../lib/core/chart.js";
import { resultPath } from "../lib/core/results-dir.js";
import { percentile } from "../lib/core/stats.js";

// Each shard responds only after its full ramp + hold completes — at
// 50K-per-shard with a 120s hold, that's ~5 minutes per request. Bump
// the default 5-min fetch headers timeout so the coordinator doesn't
// give up before the shards finish.
setGlobalDispatcher(
  new Agent({ headersTimeout: 30 * 60 * 1000, bodyTimeout: 30 * 60 * 1000 })
);

const shardCsv = process.env.SHARDS;
if (!shardCsv) {
  console.error("SHARDS env var is required (comma-separated bench-runner URLs)");
  process.exit(1);
}
const shardUrls = shardCsv.split(",").map((s) => s.trim()).filter(Boolean);
if (shardUrls.length === 0) {
  console.error("SHARDS must contain at least one URL");
  process.exit(1);
}

const perShardN = parseInt(process.env.PER_SHARD_N || "25000", 10);
const holdSec = parseInt(process.env.HOLD_SEC || "120", 10);
const rampPerSec = parseInt(process.env.RAMP_PER_SEC || "200", 10);
const stream = process.env.STREAM || "idle-probe";
// Optional override sent to each shard so the bench-runner targets a
// different anycable-go service (e.g. anycable-go-pro for the Pro variant).
const cableUrl = process.env.CABLE_URL;

// TARGET=socketio switches the test to /bench-idle-socketio (Node-based
// Socket.io). TARGET=uws targets /bench-idle-uws (uWebSockets.js).
// Defaults to anycable for backwards compatibility.
const target = (process.env.TARGET || "anycable").toLowerCase();
if (target !== "anycable" && target !== "socketio" && target !== "uws") {
  console.error(`TARGET must be "anycable", "socketio", or "uws" (got "${target}")`);
  process.exit(1);
}
// SERVER_URL overrides the Socket.io target (TARGET=socketio variant).
const socketioServerUrl = process.env.SERVER_URL;
// UWS_WS_URL overrides the uWS target (TARGET=uws variant).
const uwsWsUrl = process.env.UWS_WS_URL;

const totalTarget = perShardN * shardUrls.length;

console.log(
  `Idle multi-shard test: ${shardUrls.length} shards × ${perShardN} = ${totalTarget} connections`
);
console.log(`Hold:   ${holdSec}s  Ramp: ${rampPerSec}/s per shard\n`);
shardUrls.forEach((u, i) => console.log(`  shard-${i + 1}: ${u}`));
console.log("");

// Per-shard hard timeout — bumps fetch's headers timeout, but also acts as
// an absolute ceiling so one hung shard can't block the whole report.
const SHARD_TIMEOUT_MS = parseInt(process.env.SHARD_TIMEOUT_MS || "600000", 10);

async function runShard(url: string, label: string): Promise<IdleResult> {
  const qs = new URLSearchParams({
    n: String(perShardN),
    hold: String(holdSec),
    ramp: String(rampPerSec),
    stream,
    shard: label,
  });
  if (target === "anycable" && cableUrl) qs.set("cableUrl", cableUrl);
  if (target === "socketio" && socketioServerUrl) qs.set("serverUrl", socketioServerUrl);
  if (target === "uws" && uwsWsUrl) qs.set("wsUrl", uwsWsUrl);
  const endpoint =
    target === "socketio"
      ? "bench-idle-socketio"
      : target === "uws"
        ? "bench-idle-uws"
        : "bench-idle-anycable";

  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), SHARD_TIMEOUT_MS);
  try {
    const res = await fetch(`${url}/${endpoint}?${qs.toString()}`, {
      method: "POST",
      signal: ctrl.signal,
    });
    if (!res.ok) {
      throw new Error(`${label} returned ${res.status} ${res.statusText}`);
    }
    const result = (await res.json()) as IdleResult;
    // Stream this shard's outcome immediately so a later stall can't lose it.
    console.log(
      `  ✓ ${label}: connected=${result.connected} welcomed=${result.welcomed} subscribed=${result.subscribed} failed=${result.failed} ramp=${(result.rampElapsedMs / 1000).toFixed(1)}s`
    );
    return result;
  } catch (err) {
    console.log(
      `  ✗ ${label}: ${err instanceof Error ? err.message : String(err)}`
    );
    throw err;
  } finally {
    clearTimeout(t);
  }
}

const startedAt = new Date();
const startedAtIso = startedAt.toISOString();
console.log(`Test started at ${startedAtIso}\n`);

// Use allSettled: a single shard's HTTP failure (502, network glitch, etc.)
// shouldn't lose the other shards' results. We report partial-success and
// keep going so the metrics chart still has data.
const shardPromises = shardUrls.map((u, i) => runShard(u, `shard-${i + 1}`));
const settled = await Promise.allSettled(shardPromises);

const endedAt = new Date();
const endedAtIso = endedAt.toISOString();

// -------------------------------------------------------------------------
// Aggregate

const results: IdleResult[] = [];
const errors: { idx: number; reason: string }[] = [];
settled.forEach((s, i) => {
  if (s.status === "fulfilled") results.push(s.value);
  else errors.push({ idx: i + 1, reason: String(s.reason).slice(0, 200) });
});

const totals = results.reduce(
  (acc, r) => ({
    connected: acc.connected + r.connected,
    welcomed: acc.welcomed + r.welcomed,
    subscribed: acc.subscribed + r.subscribed,
    failed: acc.failed + r.failed,
  }),
  { connected: 0, welcomed: 0, subscribed: 0, failed: 0 }
);

// Per-shard outcomes were already streamed via runShard; no need to repeat.
if (errors.length > 0) {
  console.log(
    `\n${errors.length} shard(s) errored or timed out — totals below cover the ${results.length} surviving shard(s).`
  );
}

console.log(`\n=== Aggregate ===`);
console.log(`  Connected:    ${totals.connected.toLocaleString()} / ${totalTarget.toLocaleString()}`);
console.log(`  Welcomed:     ${totals.welcomed.toLocaleString()}`);
console.log(`  Subscribed:   ${totals.subscribed.toLocaleString()}`);
console.log(`  Failed:       ${totals.failed.toLocaleString()}`);
console.log(`  Test window:  ${startedAtIso} → ${endedAtIso}`);

// -------------------------------------------------------------------------
// Optional: Railway metrics + chart

const projectId = process.env.PROJECT_ID;
const serviceId = process.env.SERVICE_ID;
const serviceName = process.env.SERVICE_NAME || "anycable-go";

if (!projectId || !serviceId) {
  console.log(
    "\n(set PROJECT_ID and SERVICE_ID to chart memory/CPU on anycable-go)"
  );
  process.exit(0);
}

const token = readRailwayToken();

// Pad the window slightly on each side so we capture the ramp-up and tail.
const padMs = 30 * 1000;
const windowStart = new Date(startedAt.getTime() - padMs).toISOString();
const windowEnd = new Date(endedAt.getTime() + padMs).toISOString();

console.log(
  `\nFetching Railway metrics for ${serviceName} over [${windowStart}, ${windowEnd}]...`
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
  process.exit(0);
}

// Convert to seconds-since-test-start.
const startUnix = Math.floor(startedAt.getTime() / 1000);
const memSeries = memPoints.map((p) => ({ tSec: p.ts - startUnix, value: p.value * 1024 }));
const cpuSeries = cpuPoints.map((p) => ({ tSec: p.ts - startUnix, value: p.value }));

console.log(
  `\n=== ${serviceName} during the test ===  (n=${memPoints.length} samples)\n`
);

console.log(chart({ title: `Memory`, points: memSeries, height: 12, width: 60, yUnit: "MB" }));
console.log("");
console.log(chart({ title: `CPU (% of 1 vCPU)`, points: cpuSeries, height: 8, width: 60, yUnit: "%" }));

const memValues = memSeries.map((p) => p.value).sort((a, b) => a - b);
const cpuValues = cpuSeries.map((p) => p.value).sort((a, b) => a - b);

console.log(`\nMemory: peak=${percentile(memValues, 100).toFixed(0)} MB  p95=${percentile(memValues, 95).toFixed(0)} MB  avg=${(memValues.reduce((s, n) => s + n, 0) / Math.max(1, memValues.length)).toFixed(0)} MB`);
console.log(`CPU:    peak=${percentile(cpuValues, 100).toFixed(2)} %   p95=${percentile(cpuValues, 95).toFixed(2)} %   avg=${(cpuValues.reduce((s, n) => s + n, 0) / Math.max(1, cpuValues.length)).toFixed(2)} %`);

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
