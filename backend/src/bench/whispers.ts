// Whispers driver — calls bench-runner with the chosen protocol and
// writes the result JSON.
//
// Usage:
//   BENCH_RUNNER_URL=https://bench-runner-production.up.railway.app \
//   PROTOCOL=anycable NUM_CLIENTS=10000 ROOMS=100 INTERVAL_MS=100 DURATION_SEC=30 \
//     tsx src/bench/whispers.ts
//
// PROTOCOL=anycable | socketio

import { writeFileSync } from "fs";
import { Agent, setGlobalDispatcher } from "undici";

setGlobalDispatcher(
  new Agent({ headersTimeout: 30 * 60 * 1000, bodyTimeout: 30 * 60 * 1000 }),
);

const benchRunnerUrl = process.env.BENCH_RUNNER_URL;
if (!benchRunnerUrl) {
  console.error("BENCH_RUNNER_URL is required");
  process.exit(1);
}
const protocol = (process.env.PROTOCOL || "anycable").toLowerCase();
if (protocol !== "anycable" && protocol !== "socketio") {
  console.error("PROTOCOL must be anycable or socketio");
  process.exit(1);
}

const n = parseInt(process.env.NUM_CLIENTS || "1000", 10);
const rooms = parseInt(process.env.ROOMS || "10", 10);
const rampPerSec = parseInt(process.env.RAMP_PER_SEC || "100", 10);
const intervalMs = parseInt(process.env.INTERVAL_MS || "100", 10);
const durationSec = parseInt(process.env.DURATION_SEC || "30", 10);
const payloadBytes = parseInt(process.env.PAYLOAD_BYTES || "64", 10);

const tag = `whispers-${protocol}-${new Date().toISOString().replace(/[:.]/g, "-")}`;
const outFile = `${tag}.json`;

console.log(`Whispers benchmark (${protocol})`);
console.log(`  bench-runner: ${benchRunnerUrl}`);
console.log(
  `  N=${n}  rooms=${rooms}  ramp=${rampPerSec}/s  interval=${intervalMs}ms  duration=${durationSec}s  payload=${payloadBytes}B`,
);
console.log(`  output: ${outFile}\n`);

const qs = new URLSearchParams({
  n: String(n),
  rooms: String(rooms),
  ramp: String(rampPerSec),
  interval: String(intervalMs),
  duration: String(durationSec),
  payload: String(payloadBytes),
});

const url = `${benchRunnerUrl}/bench-whispers-${protocol}?${qs.toString()}`;
console.log(`POST ${url}\n`);

const startedAt = Date.now();
const res = await fetch(url, { method: "POST" });
if (!res.ok) {
  console.error(`bench-runner returned HTTP ${res.status}`);
  process.exit(1);
}
const result = await res.json();
const elapsedSec = ((Date.now() - startedAt) / 1000).toFixed(1);

console.log(`\nReceived ${elapsedSec}s after start.\n`);
console.log(`=== Result ===`);
console.log(JSON.stringify(result, null, 2));
writeFileSync(outFile, JSON.stringify(result, null, 2));
console.log(`\nSaved: ${outFile}`);
