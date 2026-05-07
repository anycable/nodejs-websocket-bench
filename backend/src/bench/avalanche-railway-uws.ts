// Avalanche benchmark — uWebSockets.js on Railway.
//
// Drives the bench-runner's /bench-avalanche-uws endpoint. The bench-runner
// connects N clients, then waits in its prearm window for the operator
// to trigger a redeploy of the uws-server (or uws-server-small) service:
//
//   railway redeploy -s uws-server --yes
//
// Once the first disconnect arrives, the bench-runner counts how many
// clients come back and how fast. Same methodology as the Socket.io
// avalanche test — the only thing changing is the server under test.
//
// Usage:
//   BENCH_RUNNER_URL=https://bench-runner-production.up.railway.app \
//   N=25000 \
//   tsx src/bench/avalanche-railway-uws.ts

import { Agent, setGlobalDispatcher } from "undici";

setGlobalDispatcher(
  new Agent({ headersTimeout: 30 * 60 * 1000, bodyTimeout: 30 * 60 * 1000 })
);

const benchRunnerUrl = process.env.BENCH_RUNNER_URL;
if (!benchRunnerUrl) {
  console.error("BENCH_RUNNER_URL is required");
  process.exit(1);
}
const n = parseInt(process.env.N || "5000", 10);
const ramp = parseInt(process.env.RAMP_RATE || "200", 10);
const prearm = parseInt(process.env.PREARM_SEC || "120", 10);
const recoveryWait = parseInt(process.env.RECOVERY_WAIT_SEC || "180", 10);
const wsUrl = process.env.UWS_WS_URL;

const qs = new URLSearchParams({
  n: String(n),
  ramp: String(ramp),
  prearm: String(prearm),
  recoveryWait: String(recoveryWait),
  stream: `avalanche-uws-${Date.now()}`,
});
if (wsUrl) qs.set("wsUrl", wsUrl);

console.log(`POST ${benchRunnerUrl}/bench-avalanche-uws?n=${n} ramp=${ramp}/s prearm=${prearm}s`);
console.log(`\n>>> Trigger restart during the prearm window:`);
console.log(`    railway redeploy -s uws-server --yes`);
console.log(`    (or: -s uws-server-small for the small-box test)\n`);

const startedAt = Date.now();
const res = await fetch(`${benchRunnerUrl}/bench-avalanche-uws?${qs.toString()}`, {
  method: "POST",
});
if (!res.ok) {
  console.error(`bench-runner returned ${res.status} ${res.statusText}`);
  process.exit(1);
}
const result = (await res.json()) as Record<string, unknown> & {
  reconnectMs: { p50: number; p95: number; p99: number; max: number };
};
const elapsedSec = ((Date.now() - startedAt) / 1000).toFixed(1);
console.log(`\nReceived ${elapsedSec}s after start.\n`);
console.log(`Initially connected:  ${result.initiallyConnected}`);
console.log(`Disconnected:         ${result.disconnected}`);
console.log(`Reconnected:          ${result.reconnected} (${result.reconnectRatePct}%)`);
console.log(`Never reconnected:    ${result.neverReconnected} (${result.neverReconnectedPct}%)`);
console.log(`Recovery time (95%):  ${result.recoveryTimeMs}ms`);
console.log(
  `Reconnect p50/p95/p99/max: ${result.reconnectMs.p50}/${result.reconnectMs.p95}/${result.reconnectMs.p99}/${result.reconnectMs.max} ms`
);
