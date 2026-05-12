// CLI: drive the AnyCable throughput test against the Railway-hosted
// bench-runner.
//
// Sweep the rate by setting INTERVAL_MS (= 1000 / target msg-per-sec):
//   INTERVAL_MS=1000  → 1 msg/sec      (mild)
//   INTERVAL_MS=100   → 10 msg/sec     (moderate)
//   INTERVAL_MS=10    → 100 msg/sec    (aggressive)
//   INTERVAL_MS=1     → 1000 msg/sec   (stress)
//
//   BENCH_RUNNER_URL=https://bench-runner-production.up.railway.app \
//   N=10000 TOTAL=100 INTERVAL_MS=10 \
//   tsx src/bench/throughput-anycable.ts

import { Agent, setGlobalDispatcher } from "undici";

setGlobalDispatcher(
  new Agent({ headersTimeout: 30 * 60 * 1000, bodyTimeout: 30 * 60 * 1000 })
);

const benchRunnerUrl = process.env.BENCH_RUNNER_URL;
if (!benchRunnerUrl) {
  console.error("BENCH_RUNNER_URL is required");
  process.exit(1);
}

const n = parseInt(process.env.N || "10000", 10);
const total = parseInt(process.env.TOTAL || "100", 10);
const intervalMs = parseInt(process.env.INTERVAL_MS || "100", 10);
const ramp = parseInt(process.env.RAMP_RATE || "200", 10);
const drain = parseInt(process.env.DRAIN_SEC || "30", 10);
const cableUrl = process.env.CABLE_URL;
const broadcastUrl = process.env.BROADCAST_URL;

const qs = new URLSearchParams({
  n: String(n),
  total: String(total),
  intervalMs: String(intervalMs),
  ramp: String(ramp),
  drain: String(drain),
  stream: `tp-ac-${Date.now()}`,
});
if (cableUrl) qs.set("cableUrl", cableUrl);
if (broadcastUrl) qs.set("broadcastUrl", broadcastUrl);

const startedAt = Date.now();
console.log(`POST ${benchRunnerUrl}/bench-throughput-anycable?n=${n}&total=${total}&intervalMs=${intervalMs}`);
const res = await fetch(`${benchRunnerUrl}/bench-throughput-anycable?${qs.toString()}`, {
  method: "POST",
});
if (!res.ok) {
  console.error(`bench-runner returned ${res.status} ${res.statusText}`);
  process.exit(1);
}
const result = (await res.json()) as Record<string, unknown> & {
  deliveryRatePct: number;
  outboundDeliveriesPerSec: number;
  usefulDeliveriesPerSec: number;
  targetRateMsgPerSec: number;
  latencyRawMs: { p50: number; p95: number; p99: number; max: number };
};
const elapsedSec = ((Date.now() - startedAt) / 1000).toFixed(1);
console.log(`\nReceived ${elapsedSec}s after start.\n`);
console.log(`target rate:              ${result.targetRateMsgPerSec} msg/sec`);
console.log(`outbound deliveries/sec:  ${result.outboundDeliveriesPerSec.toLocaleString()}`);
console.log(`useful deliveries/sec:    ${result.usefulDeliveriesPerSec.toLocaleString()}  (× delivery rate)`);
console.log(`delivery rate:            ${result.deliveryRatePct}%`);
console.log(
  `latency raw p50/p95/p99/max:  ${result.latencyRawMs.p50}/${result.latencyRawMs.p95}/${result.latencyRawMs.p99}/${result.latencyRawMs.max} ms`
);
console.log(`runner peak RSS:          ${result.runnerPeakRssMb} MB`);
