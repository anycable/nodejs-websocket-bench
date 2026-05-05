// Multi-scale avalanche stress test.
//
// Runs the deploy-avalanche scenario at several client-count scales,
// triggering `railway restart` on the Socket.io service from this
// machine while a bench-runner shard holds the WebSocket fleet. The
// goal is to surface how Socket.io's recovery curve degrades with
// scale — the architectural cliff Vladimir asked us to find.
//
// Usage:
//   BENCH_RUNNER_URL=https://bench-runner-production.up.railway.app \
//   RAILWAY_SERVICE=socketio-server \
//   SCALES=1000,2500,5000,10000,20000 \
//     tsx src/bench/avalanche-multi.ts
//
// Requirements:
//   - The local `railway` CLI authenticated and linked to the project.
//   - The bench-runner deployed with /bench-avalanche-socketio (new
//     endpoint added in this PR).
//
// Each scale runs sequentially (we wait between runs for the box to
// settle). The final aggregated table is printed and written to a CSV.

import { spawnSync } from "child_process";
import { writeFileSync } from "fs";
import { Agent, setGlobalDispatcher } from "undici";

import type { AvalancheResult } from "../lib/avalanche-runner.js";

// Same long timeout as idle-multi; each scale's POST blocks until the
// avalanche cycle finishes on the bench-runner.
setGlobalDispatcher(
  new Agent({ headersTimeout: 30 * 60 * 1000, bodyTimeout: 30 * 60 * 1000 })
);

const benchRunnerUrl = process.env.BENCH_RUNNER_URL;
if (!benchRunnerUrl) {
  console.error("BENCH_RUNNER_URL is required");
  process.exit(1);
}
const railwayService = process.env.RAILWAY_SERVICE || "socketio-server";
// Override the Socket.io target the bench-runner shard connects to. Useful
// when testing a smaller, capped-memory socketio-server instead of the
// regular one (e.g. socketio-server-small.railway.internal:3000).
const serverUrl = process.env.SERVER_URL;

const scales = (process.env.SCALES || "1000,2500,5000,10000,20000")
  .split(",")
  .map((s) => parseInt(s.trim(), 10))
  .filter((n) => Number.isFinite(n) && n > 0);

const rampPerSec = parseInt(process.env.RAMP_PER_SEC || "200", 10);
const prearmSec = parseInt(process.env.PREARM_SEC || "120", 10);
const recoveryWaitSec = parseInt(process.env.RECOVERY_WAIT_SEC || "180", 10);
const settleBetweenSec = parseInt(process.env.SETTLE_BETWEEN_SEC || "60", 10);

console.log(`Avalanche multi-scale stress test`);
console.log(`  bench-runner:     ${benchRunnerUrl}`);
console.log(`  railway service:  ${railwayService} (redeploy trigger)`);
if (serverUrl) console.log(`  socket.io target: ${serverUrl} (override)`);
console.log(`  scales:           ${scales.join(", ")}`);
console.log(`  ramp:             ${rampPerSec}/s`);
console.log(`  prearm window:    ${prearmSec}s`);
console.log(`  recovery wait:    ${recoveryWaitSec}s`);
console.log(`  settle between:   ${settleBetweenSec}s\n`);

interface ScaleRow extends AvalancheResult {
  scale: number;
}

const results: ScaleRow[] = [];

function railwayRestart(): boolean {
  // We use `redeploy --json` rather than `restart --yes` for two reasons:
  //   (1) the CLI's `restart --yes` returns success without actually
  //       triggering anything (verified empirically — same deployment
  //       id, no new "Starting Container" log line);
  //   (2) `--json` keeps the CLI non-interactive and makes the call
  //       block until Railway acknowledges the new deployment id.
  console.log(`  > railway redeploy -s ${railwayService} --yes --json`);
  const r = spawnSync(
    "railway",
    ["redeploy", "-s", railwayService, "--yes", "--json"],
    { stdio: "inherit" }
  );
  return r.status === 0;
}

async function runOneScale(n: number): Promise<ScaleRow | null> {
  console.log(`\n=== ${n.toLocaleString()} clients ===`);

  // Estimated time the bench-runner needs to finish ramp + 5s settle.
  // We trigger the restart shortly after that.
  const rampSec = Math.ceil(n / rampPerSec);
  const triggerInSec = rampSec + 8; // ramp + settle (~5) + small buffer

  const qs = new URLSearchParams({
    n: String(n),
    ramp: String(rampPerSec),
    prearm: String(prearmSec),
    recoveryWait: String(recoveryWaitSec),
    stream: `avalanche-${n}`,
  });
  if (serverUrl) qs.set("serverUrl", serverUrl);

  const startedAt = Date.now();
  const responsePromise = fetch(
    `${benchRunnerUrl}/bench-avalanche-socketio?${qs.toString()}`,
    { method: "POST" }
  );

  // Wait for the bench-runner to finish ramping before triggering the
  // restart. The bench-runner will catch the disconnect storm via the
  // listeners attached at socket-creation time.
  console.log(`  (ramping ${n} clients, ~${rampSec}s — restart in ${triggerInSec}s)`);
  await new Promise((r) => setTimeout(r, triggerInSec * 1000));

  if (!railwayRestart()) {
    console.error(`  ! railway restart failed; skipping scale ${n}`);
    try {
      // Wait for the bench-runner to time out / return so we don't
      // leak the fetch.
      await responsePromise;
    } catch {
      /* ignore */
    }
    return null;
  }

  let result: AvalancheResult;
  try {
    const res = await responsePromise;
    if (!res.ok) {
      console.error(`  ! bench-runner returned ${res.status} ${res.statusText}`);
      return null;
    }
    result = (await res.json()) as AvalancheResult;
  } catch (err) {
    console.error(`  ! request error: ${err instanceof Error ? err.message : err}`);
    return null;
  }

  console.log(
    `  recovery=${result.recoveryTimeMs}ms  reconnect-rate=${result.reconnectRatePct}%  never-reconnected=${result.neverReconnected} (${result.neverReconnectedPct}%)`
  );
  console.log(
    `  reconnect ms: p50=${result.reconnectMs.p50}  p95=${result.reconnectMs.p95}  p99=${result.reconnectMs.p99}  max=${result.reconnectMs.max}`
  );

  return { scale: n, ...result };
}

for (const n of scales) {
  const row = await runOneScale(n);
  if (row) results.push(row);

  if (n !== scales[scales.length - 1]) {
    console.log(`  (settling ${settleBetweenSec}s before next scale...)`);
    await new Promise((r) => setTimeout(r, settleBetweenSec * 1000));
  }
}

// -------------------------------------------------------------------------
// Final scaling report

console.log(`\n=== Avalanche scaling — Socket.io single instance ===\n`);
console.log(
  `  scale     connected   disconnected   recovery_ms   p50   p95   p99   never  never%`
);
for (const r of results) {
  console.log(
    `  ${String(r.scale).padStart(8)}    ${String(r.initiallyConnected).padStart(8)}    ${String(r.disconnected).padStart(10)}     ${String(r.recoveryTimeMs).padStart(8)}   ${String(r.reconnectMs.p50).padStart(4)}  ${String(r.reconnectMs.p95).padStart(4)}  ${String(r.reconnectMs.p99).padStart(4)}  ${String(r.neverReconnected).padStart(5)}  ${String(r.neverReconnectedPct).padStart(5)}%`
  );
}

const csvPath = `avalanche-multi-${new Date().toISOString().replace(/[:.]/g, "-")}.csv`;
const lines = [
  "scale,initially_connected,disconnected,reconnected,recovery_ms,reconnect_p50_ms,reconnect_p95_ms,reconnect_p99_ms,reconnect_max_ms,never_reconnected,never_reconnected_pct,disconnect_spread_ms,total_elapsed_ms",
];
for (const r of results) {
  lines.push(
    `${r.scale},${r.initiallyConnected},${r.disconnected},${r.reconnected},${r.recoveryTimeMs},${r.reconnectMs.p50},${r.reconnectMs.p95},${r.reconnectMs.p99},${r.reconnectMs.max},${r.neverReconnected},${r.neverReconnectedPct},${r.disconnectSpreadMs},${r.totalElapsedMs}`
  );
}
writeFileSync(csvPath, lines.join("\n") + "\n");
console.log(`\nWrote ${csvPath}`);
