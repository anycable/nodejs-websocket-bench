// Pull memory and CPU metrics for a Railway service over a time window.
//
// Usage:
//   PROJECT_ID=<uuid> SERVICE_ID=<uuid> SERVICE_NAME=socketio-server \
//   START_DATE=2026-04-30T19:34:00Z END_DATE=2026-04-30T19:38:00Z \
//   tsx src/bench/railway-metrics.ts
//
// Auth: looks for `RAILWAY_TOKEN` first, falls back to `~/.railway/config.json`
// (the file the `railway` CLI writes after `railway login`). The token is read
// only on the local machine and never written to the repo.
//
// The script samples each metric at 10-second granularity and prints peak,
// average, and p95 over the window. Useful for "what was the server doing
// while we ran the benchmark" — see README § "Server-side resource usage".

import { readFileSync } from "fs";
import { homedir } from "os";

function readToken(): string {
  if (process.env.RAILWAY_TOKEN) return process.env.RAILWAY_TOKEN;
  const cfg = JSON.parse(readFileSync(`${homedir()}/.railway/config.json`, "utf-8"));
  return cfg.user.token;
}

const projectId = process.env.PROJECT_ID;
const serviceId = process.env.SERVICE_ID;
const serviceName = process.env.SERVICE_NAME || serviceId;
const startDate = process.env.START_DATE;
const endDate = process.env.END_DATE || new Date().toISOString();

if (!projectId || !serviceId || !startDate) {
  console.error("PROJECT_ID, SERVICE_ID, and START_DATE are required");
  process.exit(1);
}

const token = readToken();

interface DataPoint {
  ts: number; // unix seconds
  value: number;
}

// Railway's API enforces a minimum sampleRateSeconds (empirically 30s as of
// 2026-04). Override via SAMPLE_RATE for longer windows where coarser sampling
// is acceptable.
const sampleRate = parseInt(process.env.SAMPLE_RATE || "30");

async function fetchMetric(measurement: string): Promise<DataPoint[]> {
  const query = `
    query Metrics($projectId: String!, $serviceId: String!, $start: DateTime!, $end: DateTime!, $measurement: MetricMeasurement!, $sampleRate: Int!) {
      metrics(
        projectId: $projectId
        serviceId: $serviceId
        startDate: $start
        endDate: $end
        measurements: [$measurement]
        sampleRateSeconds: $sampleRate
      ) {
        measurement
        values { ts value }
      }
    }
  `;
  const res = await fetch("https://backboard.railway.com/graphql/v2", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      query,
      variables: { projectId, serviceId, start: startDate, end: endDate, measurement, sampleRate },
    }),
  });
  const json: any = await res.json();
  if (json.errors) {
    console.error(`Failed to fetch ${measurement}:`, JSON.stringify(json.errors, null, 2));
    return [];
  }
  const series = json.data?.metrics?.[0];
  return series?.values ?? [];
}

function summarize(label: string, points: DataPoint[], unit: string, scale = 1) {
  if (!points.length) {
    console.log(`${label.padEnd(16)} no data`);
    return;
  }
  const values = points.map((p) => p.value * scale).sort((a, b) => a - b);
  const avg = values.reduce((s, n) => s + n, 0) / values.length;
  const p50 = values[Math.floor((values.length - 1) * 0.5)];
  const p95 = values[Math.floor((values.length - 1) * 0.95)];
  const peak = values[values.length - 1];
  console.log(
    `${label.padEnd(16)} avg=${avg.toFixed(2)}${unit}  p50=${p50.toFixed(2)}${unit}  p95=${p95.toFixed(2)}${unit}  peak=${peak.toFixed(2)}${unit}  (n=${values.length})`
  );
}

const [mem, cpu] = await Promise.all([
  fetchMetric("MEMORY_USAGE_GB"), // GB → multiply by 1024 for MB
  fetchMetric("CPU_USAGE"), // percentage of 1 vCPU (>100 means multi-core saturation)
]);

console.log(`\n=== Railway metrics — ${serviceName} ===`);
console.log(`Window: ${startDate} → ${endDate}`);
summarize("Memory", mem, " MB", 1024);
summarize("CPU", cpu, " %");
