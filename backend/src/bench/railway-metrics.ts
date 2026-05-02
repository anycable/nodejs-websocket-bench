// Pull memory and CPU metrics for a Railway service over a time window.
//
// Usage:
//   PROJECT_ID=<uuid> SERVICE_ID=<uuid> SERVICE_NAME=anycable-go \
//   START_DATE=2026-04-30T19:34:00Z END_DATE=2026-04-30T19:38:00Z \
//   tsx src/bench/railway-metrics.ts
//
// Auth: looks for `RAILWAY_TOKEN` first, falls back to `~/.railway/config.json`
// (the file the `railway` CLI writes after `railway login`).
//
// The script samples each metric at SAMPLE_RATE-second granularity (default
// 30s, the API minimum for short windows) and prints peak, average, and p95
// over the window. Useful for "what was the server doing while the
// benchmark ran" reports.

import { percentile } from "../lib/stats.js";
import { fetchMetric, readRailwayToken, DataPoint } from "../lib/railway-api.js";

const projectId = process.env.PROJECT_ID;
const serviceId = process.env.SERVICE_ID;
const serviceName = process.env.SERVICE_NAME || serviceId;
const startDate = process.env.START_DATE;
const endDate = process.env.END_DATE || new Date().toISOString();
const sampleRate = parseInt(process.env.SAMPLE_RATE || "30", 10);

if (!projectId || !serviceId || !startDate) {
  console.error("PROJECT_ID, SERVICE_ID, and START_DATE are required");
  process.exit(1);
}

const token = readRailwayToken();

function summarize(label: string, points: DataPoint[], unit: string, scale = 1) {
  if (!points.length) {
    console.log(`${label.padEnd(16)} no data`);
    return;
  }
  const values = points.map((p) => p.value * scale).sort((a, b) => a - b);
  const avg = values.reduce((s, n) => s + n, 0) / values.length;
  const p50 = percentile(values, 50);
  const p95 = percentile(values, 95);
  const peak = percentile(values, 100);
  console.log(
    `${label.padEnd(16)} avg=${avg.toFixed(2)}${unit}  p50=${p50.toFixed(2)}${unit}  p95=${p95.toFixed(2)}${unit}  peak=${peak.toFixed(2)}${unit}  (n=${values.length})`
  );
}

const [mem, cpu] = await Promise.all([
  fetchMetric({
    token,
    projectId,
    serviceId,
    measurement: "MEMORY_USAGE_GB",
    startDate,
    endDate,
    sampleRate,
  }),
  fetchMetric({
    token,
    projectId,
    serviceId,
    measurement: "CPU_USAGE",
    startDate,
    endDate,
    sampleRate,
  }),
]);

console.log(`\n=== Railway metrics — ${serviceName} ===`);
console.log(`Window: ${startDate} → ${endDate}`);
summarize("Memory", mem, " MB", 1024); // GB → MB
summarize("CPU", cpu, " %"); // percentage of 1 vCPU
