// CLI: drive the uWS jitter test against the Railway-hosted bench-runner.
//
//   BENCH_RUNNER_URL=https://bench-runner-production.up.railway.app \
//   N=10000 \
//   tsx src/bench/jitter-uws.ts
//
// Optional: UWS_WS_URL and UWS_HTTP_URL override the bench-runner's defaults
// (e.g. point at uws-server-small to share the small-box numbers).

import { Agent, setGlobalDispatcher } from "undici";

import { benchRunnerFetch } from "../lib/bench-runner-client.js";

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
const wsUrl = process.env.UWS_WS_URL;
const httpUrl = process.env.UWS_HTTP_URL;

const qs = new URLSearchParams({
  n: String(n),
  duration: String(duration),
  jitter: String(jitter),
  jitterMs: String(jitterMs),
  msgs: String(msgs),
  interval: String(interval),
  ramp: String(ramp),
  stream: `jitter-uws-${Date.now()}`,
});
if (wsUrl) qs.set("wsUrl", wsUrl);
if (httpUrl) qs.set("httpUrl", httpUrl);

const startedAt = Date.now();
console.log(`POST ${benchRunnerUrl}/bench-jitter-uws?${qs.toString().slice(0, 80)}...`);
const res = await benchRunnerFetch(`${benchRunnerUrl}/bench-jitter-uws?${qs.toString()}`, {
  method: "POST",
});
if (!res.ok) {
  console.error(`bench-runner returned ${res.status} ${res.statusText}`);
  process.exit(1);
}
const result = (await res.json()) as Record<string, unknown> & {
  latencyRawMs: { p50: number; p95: number; p99: number; max: number };
  latencyOverMinMs: { p50: number; p95: number; p99: number; max: number; skewFloor: number };
};
const elapsedSec = ((Date.now() - startedAt) / 1000).toFixed(1);
console.log(`\nReceived ${elapsedSec}s after start.\n`);
console.log(`delivery rate:        ${result.deliveryRatePct}%`);
console.log(`lost deliveries:      ${result.lostDeliveries}`);
console.log(`jitter events:        ${result.jitterEvents}`);
console.log(
  `latency raw p50/p95/p99/max:  ${result.latencyRawMs.p50}/${result.latencyRawMs.p95}/${result.latencyRawMs.p99}/${result.latencyRawMs.max} ms`
);
console.log(
  `latency over-min p99 (skew=${result.latencyOverMinMs.skewFloor}ms): ${result.latencyOverMinMs.p99} ms`
);
console.log(`runner peak RSS:      ${result.runnerPeakRssMb} MB`);
