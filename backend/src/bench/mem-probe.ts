// Standalone Railway memory probe (avoids idle-multi's undici dispatcher gzip
// bug). Prints peak MEMORY_USAGE_GB for a service over the last WINDOW_MIN
// minutes. Usage:
//   SERVICE_ID=<uuid> [WINDOW_MIN=10] tsx src/bench/mem-probe.ts
import { fetchMetric, readRailwayToken } from "../lib/core/railway-api.js";

const projectId = process.env.PROJECT_ID || "fd842a43-8d78-48c0-879f-4b5311c8c004";
const serviceId = process.env.SERVICE_ID;
if (!serviceId) {
  console.error("SERVICE_ID required");
  process.exit(1);
}
const windowMin = parseInt(process.env.WINDOW_MIN || "10", 10);
const end = new Date();
const start = new Date(end.getTime() - windowMin * 60 * 1000);

const token = readRailwayToken();
const points = await fetchMetric({
  token,
  projectId,
  serviceId,
  startDate: start.toISOString(),
  endDate: end.toISOString(),
  measurement: "MEMORY_USAGE_GB",
  sampleRate: 15,
});
if (!points.length) {
  console.log("no data");
  process.exit(0);
}
const vals = points.map((p) => p.value);
const peak = Math.max(...vals);
const last = vals[vals.length - 1];
console.log(
  `samples=${vals.length} peakGB=${peak.toFixed(3)} lastGB=${last.toFixed(3)} minGB=${Math.min(...vals).toFixed(3)}`
);
