// One-off helper: fetch Railway memory + CPU for the test windows we ran
// today and print a per-run summary. Read-only — no test execution.
//
// Usage:
//   tsx src/bench/fetch-jitter-metrics.ts

import { fetchMetric, readRailwayToken } from "../lib/railway-api.js";
import { percentile } from "../lib/stats.js";

const PROJECT_ID = "fd842a43-8d78-48c0-879f-4b5311c8c004";
const SVC = {
  oss: "a6f8e7a0-46fb-4614-9a32-6f97f49bad09",
  pro: "5cef6fb0-7f6d-4ef3-a92d-93266af42b45",
  sioSmall: "d04e5e6c-a990-45cb-a05f-646ba8c30794",
  sio: "8b861242-2747-42a4-a831-8c63a8289f22",
};

interface Window {
  label: string;
  serviceId: string;
  serviceName: string;
  start: string;
  end: string;
}

const windows: Window[] = [
  {
    label: "AnyCable OSS jitter 10K",
    serviceId: SVC.oss,
    serviceName: "anycable-go",
    start: "2026-05-04T23:35:41Z",
    end: "2026-05-04T23:39:18Z",
  },
  {
    label: "AnyCable Pro jitter 10K (run #2)",
    serviceId: SVC.pro,
    serviceName: "anycable-go-pro",
    start: "2026-05-04T23:49:09Z",
    end: "2026-05-04T23:52:46Z",
  },
  {
    label: "AnyCable Pro jitter 10K (run #3)",
    serviceId: SVC.pro,
    serviceName: "anycable-go-pro",
    start: "2026-05-05T01:40:01Z",
    end: "2026-05-05T01:43:39Z",
  },
  {
    label: "AnyCable Pro jitter 10K (run #4, fresh redeploy)",
    serviceId: SVC.pro,
    serviceName: "anycable-go-pro",
    start: "2026-05-05T04:52:44Z",
    end: "2026-05-05T04:56:22Z",
  },
  // Socket.io small (1 vCPU / 0.5 GB) avalanche scaling — first batch.
  // Script wrote CSV at 18:58:14Z; total ramp+restart+recovery+settle for
  // 5K/10K/15K ≈ 27 min, so window starts ~18:31:00Z.
  {
    label: "Socket.io small — avalanche 5K/10K/15K (0.5 GB cap)",
    serviceId: SVC.sioSmall,
    serviceName: "socketio-server-csr",
    start: "2026-05-04T18:30:00Z",
    end: "2026-05-04T18:59:30Z",
  },
  // Second batch on the same box: 20K/25K/30K. Script ended 23:20:45Z;
  // budget ≈ 32 min including extra ramp time and the 25K cliff.
  {
    label: "Socket.io small — avalanche 20K/25K/30K (cliff at 25K)",
    serviceId: SVC.sioSmall,
    serviceName: "socketio-server-csr",
    start: "2026-05-04T22:48:00Z",
    end: "2026-05-04T23:21:30Z",
  },
  // 25K cleanup confirm run.
  {
    label: "Socket.io small — 25K cliff confirm",
    serviceId: SVC.sioSmall,
    serviceName: "socketio-server-csr",
    start: "2026-05-04T23:21:00Z",
    end: "2026-05-04T23:27:00Z",
  },
  // 32 GB / 32 vCPU socketio-server jitter — CSR mode.
  {
    label: "Socket.io+CSR jitter 10K (32 GB box)",
    serviceId: SVC.sio,
    serviceName: "socketio-server",
    start: "2026-05-05T04:26:54Z",
    end: "2026-05-05T04:30:32Z",
  },
  // 32 GB / 32 vCPU socketio-server jitter — default mode (after toggle).
  {
    label: "Socket.io default jitter 10K (32 GB box)",
    serviceId: SVC.sio,
    serviceName: "socketio-server",
    start: "2026-05-05T04:31:57Z",
    end: "2026-05-05T04:35:35Z",
  },
];

const token = readRailwayToken();

function pad(s: string, n: number, right = false) {
  return right ? s.padStart(n) : s.padEnd(n);
}

console.log(
  `${pad("run", 36)}  ${pad("mem peak", 10, true)}  ${pad("mem p95", 9, true)}  ${pad("mem avg", 9, true)}  ${pad("cpu peak", 9, true)}  ${pad("cpu p95", 9, true)}  ${pad("cpu avg", 9, true)}`
);
console.log("-".repeat(110));

for (const w of windows) {
  // Pad each side a bit so we don't miss the ramp-in / tail.
  const startTs = new Date(w.start).getTime() - 30_000;
  const endTs = new Date(w.end).getTime() + 30_000;
  const start = new Date(startTs).toISOString();
  const end = new Date(endTs).toISOString();

  const [memPoints, cpuPoints] = await Promise.all([
    fetchMetric({
      token,
      projectId: PROJECT_ID,
      serviceId: w.serviceId,
      measurement: "MEMORY_USAGE_GB",
      startDate: start,
      endDate: end,
    }),
    fetchMetric({
      token,
      projectId: PROJECT_ID,
      serviceId: w.serviceId,
      measurement: "CPU_USAGE",
      startDate: start,
      endDate: end,
    }),
  ]);

  // GB → MB for readability.
  const mem = memPoints.map((p) => p.value * 1024).sort((a, b) => a - b);
  const cpu = cpuPoints.map((p) => p.value).sort((a, b) => a - b);

  const fmtMem = (v: number) => `${v.toFixed(0)} MB`;
  const fmtCpu = (v: number) => `${v.toFixed(2)} %`;

  if (mem.length === 0 && cpu.length === 0) {
    console.log(`${pad(w.label, 36)}  (no metrics for this window)`);
    continue;
  }

  console.log(
    `${pad(w.label, 36)}  ` +
      `${pad(fmtMem(percentile(mem, 100)), 10, true)}  ` +
      `${pad(fmtMem(percentile(mem, 95)), 9, true)}  ` +
      `${pad(fmtMem(mem.reduce((s, n) => s + n, 0) / Math.max(1, mem.length)), 9, true)}  ` +
      `${pad(fmtCpu(percentile(cpu, 100)), 9, true)}  ` +
      `${pad(fmtCpu(percentile(cpu, 95)), 9, true)}  ` +
      `${pad(fmtCpu(cpu.reduce((s, n) => s + n, 0) / Math.max(1, cpu.length)), 9, true)}`
  );
}
