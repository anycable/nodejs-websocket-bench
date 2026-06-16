// Diagnostic CLI: drives the traced AnyCable jitter test against the
// Railway-hosted bench-runner and writes the full per-cable trace to a
// JSON file. Run after kicking the bench-runner:
//
//   BENCH_RUNNER_URL=https://bench-runner-production.up.railway.app \
//   N=10000 TRACE_SAMPLE=100 \
//   tsx src/bench/jitter-anycable-trace.ts
//
// Optional: CABLE_URL and BROADCAST_URL override the default targets
// (e.g. point at anycable-go-pro).
//
// Output: backend/results/jitter-trace-<timestamp>.json
// (override with RESULTS_DIR=).

import { writeFileSync } from "fs";
import { Agent, setGlobalDispatcher } from "undici";

import { benchRunnerFetch } from "../lib/bench-runner-client.js";
import { resultPath } from "../lib/results-dir.js";

setGlobalDispatcher(
  new Agent({ headersTimeout: 30 * 60 * 1000, bodyTimeout: 30 * 60 * 1000 })
);

const benchRunnerUrl = process.env.BENCH_RUNNER_URL;
if (!benchRunnerUrl) {
  console.error("BENCH_RUNNER_URL is required");
  process.exit(1);
}
const n = parseInt(process.env.N || "10000", 10);
const duration = parseInt(process.env.DURATION || "160", 10);
const jitter = parseInt(process.env.JITTER_INTERVAL || "15", 10);
const jitterMs = parseInt(process.env.JITTER_DURATION || "1000", 10);
const msgs = parseInt(process.env.TOTAL_MESSAGES || "120", 10);
const interval = parseInt(process.env.INTERVAL_MS || "500", 10);
const ramp = parseInt(process.env.RAMP_RATE || "200", 10);
const traceSample = parseInt(process.env.TRACE_SAMPLE || "100", 10);
const cableUrl = process.env.CABLE_URL;
const broadcastUrl = process.env.BROADCAST_URL;

const qs = new URLSearchParams({
  n: String(n),
  duration: String(duration),
  jitter: String(jitter),
  jitterMs: String(jitterMs),
  msgs: String(msgs),
  interval: String(interval),
  ramp: String(ramp),
  traceSample: String(traceSample),
  stream: `jitter-trace-${Date.now()}`,
});
if (cableUrl) qs.set("cableUrl", cableUrl);
if (broadcastUrl) qs.set("broadcastUrl", broadcastUrl);

const startedAt = Date.now();
console.log(`POST ${benchRunnerUrl}/bench-jitter-anycable-traced?${qs.toString().slice(0, 80)}...`);
const res = await benchRunnerFetch(
  `${benchRunnerUrl}/bench-jitter-anycable-traced?${qs.toString()}`,
  { method: "POST" }
);
if (!res.ok) {
  console.error(`bench-runner returned ${res.status} ${res.statusText}`);
  process.exit(1);
}
type PhaseStats = { count?: number; p50: number; p95: number; p99: number; max: number };
const result = (await res.json()) as Record<string, unknown> & {
  trace: {
    sampleSize: number;
    aggregates: {
      reconnectMs: PhaseStats;
      channelResubMs: PhaseStats;
      replayLagMs: PhaseStats;
    };
    perMessageLatency: {
      direct: PhaseStats & { count: number };
      replay: PhaseStats & { count: number };
      directShareOfTotal: number;
    };
    cables: unknown[];
  };
};

const elapsedSec = ((Date.now() - startedAt) / 1000).toFixed(1);
console.log(`\nReceived ${elapsedSec}s after start.\n`);

console.log(`Headline (run-wide):`);
console.log(`  delivery rate:       ${result.deliveryRatePct}%`);
console.log(`  lost deliveries:     ${result.lostDeliveries}`);
console.log(`  jitter events:       ${result.jitterEvents}`);
console.log(`  latency p50/p95/p99: ${(result.latencyRawMs as { p50: number }).p50}/${(result.latencyRawMs as { p95: number }).p95}/${(result.latencyRawMs as { p99: number }).p99} ms`);

const a = result.trace.aggregates;
console.log(`\nPer-cycle phases (sampled across ${result.trace.cables.length} cables):`);
console.log(`  reconnect       (terminate → cable connect):   p50=${a.reconnectMs.p50}ms  p95=${a.reconnectMs.p95}ms  p99=${a.reconnectMs.p99}ms  max=${a.reconnectMs.max}ms`);
console.log(`  channel resub   (cable connect → channel up):  p50=${a.channelResubMs.p50}ms  p95=${a.channelResubMs.p95}ms  p99=${a.channelResubMs.p99}ms  max=${a.channelResubMs.max}ms`);
console.log(`  replay lag      (channel up → first msg):       p50=${a.replayLagMs.p50}ms  p95=${a.replayLagMs.p95}ms  p99=${a.replayLagMs.p99}ms  max=${a.replayLagMs.max}ms`);

const m = result.trace.perMessageLatency;
console.log(`\nPer-message latency (split by direct vs replay):`);
console.log(`  direct  ${m.direct.count.toLocaleString()} msgs  (${(m.directShareOfTotal * 100).toFixed(1)}% of sample)`);
console.log(`           p50=${m.direct.p50}ms  p95=${m.direct.p95}ms  p99=${m.direct.p99}ms  max=${m.direct.max}ms`);
console.log(`  replay  ${m.replay.count.toLocaleString()} msgs`);
console.log(`           p50=${m.replay.p50}ms  p95=${m.replay.p95}ms  p99=${m.replay.p99}ms  max=${m.replay.max}ms`);

const path = resultPath(`jitter-trace-${new Date().toISOString().replace(/[:.]/g, "-")}.json`);
writeFileSync(path, JSON.stringify(result, null, 2));
console.log(`\nWrote full trace: ${path}`);
