// Multi-shard avalanche driver for uWebSockets.js.
//
// The single-shard /bench-avalanche-uws endpoint caps out at ~65K clients
// (Node single-process WS limit on the bench-runner). Beyond that we have
// to drive the load from multiple bench-runner shards in parallel — each
// container has its own outbound port pool and its own event loop, so 8
// shards × 25K can comfortably push 200K reconnecting clients at the
// target uws-server-small without saturating any one driver.
//
// Coordination is time-based: every shard runs the same /bench-avalanche-uws
// with the same N/ramp/prearm parameters, so they all finish ramping +
// settling at roughly the same wall-clock moment. The coordinator waits
// `triggerInSec` (= ramp + small buffer), then fires a single
// serviceInstanceRedeploy mutation against uws-server-small. Disconnects
// land on every shard simultaneously, each shard counts its own
// reconnects, and we aggregate at the end.
//
// Usage:
//   SHARDS=https://bench-runner-2-production.up.railway.app,...,https://bench-runner-9-production.up.railway.app \
//   PER_SHARD_N=25000 \
//   TARGET_SERVICE_ID=<uws-server-small svc uuid> \
//   TARGET_ENV_ID=<environment uuid> \
//     tsx src/bench/avalanche-multi-uws.ts

import { readFileSync } from "fs";
import { homedir } from "os";
import { Agent, setGlobalDispatcher } from "undici";

import { percentile } from "../lib/stats.js";
import type { AvalancheUwsResult } from "../lib/avalanche-uws.js";

setGlobalDispatcher(
  new Agent({ headersTimeout: 30 * 60 * 1000, bodyTimeout: 30 * 60 * 1000 })
);

const shardCsv = process.env.SHARDS;
if (!shardCsv) {
  console.error("SHARDS env var is required (comma-separated bench-runner URLs)");
  process.exit(1);
}
const shardUrls = shardCsv.split(",").map((s) => s.trim()).filter(Boolean);
if (shardUrls.length === 0) {
  console.error("SHARDS must contain at least one URL");
  process.exit(1);
}

const perShardN = parseInt(process.env.PER_SHARD_N || "25000", 10);
const rampPerSec = parseInt(process.env.RAMP_PER_SEC || "200", 10);
const prearmSec = parseInt(process.env.PREARM_SEC || "240", 10);
const recoveryWaitSec = parseInt(process.env.RECOVERY_WAIT_SEC || "300", 10);
const wsUrl = process.env.UWS_WS_URL || "ws://uws-server-small.railway.internal:3000/ws";

const targetServiceId = process.env.TARGET_SERVICE_ID;
const targetEnvId = process.env.TARGET_ENV_ID;
if (!targetServiceId || !targetEnvId) {
  console.error("TARGET_SERVICE_ID and TARGET_ENV_ID env vars are required");
  process.exit(1);
}

function readRailwayToken(): string {
  if (process.env.RAILWAY_TOKEN) return process.env.RAILWAY_TOKEN;
  const cfg = JSON.parse(readFileSync(`${homedir()}/.railway/config.json`, "utf-8"));
  return cfg.user.token;
}

const totalTarget = perShardN * shardUrls.length;

// Per-shard request needs to outlive: ramp + settle + prearm + recovery
// + a generous buffer for the swap and tear-down. Reasonable default:
// (ramp_sec + 5 + prearm + recovery + 90) seconds.
const rampSec = Math.ceil(perShardN / rampPerSec);
const shardTimeoutMs = parseInt(
  process.env.SHARD_TIMEOUT_MS || String((rampSec + 5 + prearmSec + recoveryWaitSec + 90) * 1000),
  10
);

console.log(`Multi-shard avalanche test: ${shardUrls.length} shards × ${perShardN} = ${totalTarget} clients`);
console.log(`  ramp: ${rampPerSec}/s per shard (~${rampSec}s)`);
console.log(`  prearm: ${prearmSec}s   recoveryWait: ${recoveryWaitSec}s`);
console.log(`  shard timeout: ${(shardTimeoutMs / 1000).toFixed(0)}s`);
console.log(`  uWS target: ${wsUrl}`);
console.log(`  redeploy target: ${targetServiceId}\n`);

interface ShardOutcome {
  label: string;
  ok: boolean;
  reason?: string;
  result?: AvalancheUwsResult;
}

async function runShard(url: string, label: string): Promise<ShardOutcome> {
  const qs = new URLSearchParams({
    n: String(perShardN),
    ramp: String(rampPerSec),
    prearm: String(prearmSec),
    recoveryWait: String(recoveryWaitSec),
    stream: `avalanche-uws-multi-${label}-${Date.now()}`,
    wsUrl,
  });

  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), shardTimeoutMs);
  try {
    const res = await fetch(`${url}/bench-avalanche-uws?${qs.toString()}`, {
      method: "POST",
      signal: ctrl.signal,
    });
    if (!res.ok) {
      console.log(`  ✗ ${label}: ${res.status} ${res.statusText}`);
      return { label, ok: false, reason: `HTTP ${res.status}` };
    }
    const result = (await res.json()) as AvalancheUwsResult;
    console.log(
      `  ✓ ${label}: connected=${result.initiallyConnected} reconnected=${result.reconnected} (${result.reconnectRatePct}%) recovery=${result.recoveryTimeMs}ms`
    );
    return { label, ok: true, result };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.log(`  ✗ ${label}: ${msg}`);
    return { label, ok: false, reason: msg };
  } finally {
    clearTimeout(t);
  }
}

async function fireRedeploy(): Promise<boolean> {
  const token = readRailwayToken();
  const res = await fetch("https://backboard.railway.com/graphql/v2", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      query:
        "mutation R($id: String!, $env: String!) { serviceInstanceRedeploy(serviceId: $id, environmentId: $env) }",
      variables: { id: targetServiceId, env: targetEnvId },
    }),
  });
  const data = (await res.json()) as {
    data?: { serviceInstanceRedeploy?: boolean };
    errors?: unknown[];
  };
  if (data.errors) {
    console.error("  ! redeploy mutation failed:", JSON.stringify(data.errors));
    return false;
  }
  return data.data?.serviceInstanceRedeploy === true;
}

const startedAt = new Date();
console.log(`Test started at ${startedAt.toISOString()}\n`);

// Kick off all shards in parallel — they each ramp their own slice.
const shardPromises = shardUrls.map((u, i) => runShard(u, `shard-${i + 1}`));

// Wait for ramp + settle to finish on all shards before triggering the
// redeploy. We add ~10 s buffer to absorb shard startup variance.
const triggerInSec = rampSec + 10;
console.log(`(ramping in parallel; firing redeploy in ${triggerInSec}s)`);
await new Promise((r) => setTimeout(r, triggerInSec * 1000));

console.log(`\n>>> Firing redeploy on uws-server-small...`);
const ok = await fireRedeploy();
if (!ok) {
  console.error("Redeploy mutation failed; bailing.");
  // Still wait for shards to time out so the process doesn't hang.
  await Promise.allSettled(shardPromises);
  process.exit(1);
}
console.log(`Redeploy triggered. Awaiting shard results (this may take ${recoveryWaitSec + 90}s+)...\n`);

const settled = await Promise.allSettled(shardPromises);

const endedAt = new Date();
console.log(`\nTest ended at ${endedAt.toISOString()}`);
console.log(`Total elapsed: ${((endedAt.getTime() - startedAt.getTime()) / 1000).toFixed(1)}s`);

// Aggregate
const ok_results: AvalancheUwsResult[] = [];
const errors: string[] = [];
settled.forEach((s) => {
  if (s.status === "fulfilled") {
    if (s.value.ok && s.value.result) ok_results.push(s.value.result);
    else errors.push(`${s.value.label}: ${s.value.reason}`);
  } else {
    errors.push(String(s.reason));
  }
});

const totals = ok_results.reduce(
  (acc, r) => ({
    initiallyConnected: acc.initiallyConnected + r.initiallyConnected,
    reconnected: acc.reconnected + r.reconnected,
    neverReconnected: acc.neverReconnected + r.neverReconnected,
    disconnected: acc.disconnected + r.disconnected,
  }),
  { initiallyConnected: 0, reconnected: 0, neverReconnected: 0, disconnected: 0 }
);

// Aggregated reconnect distribution: take per-shard percentile midpoints
// and report the mean — quick approximation, full per-cable timing would
// need each shard to return its raw distribution which doubles payload.
const p50s = ok_results.map((r) => r.reconnectMs.p50).sort((a, b) => a - b);
const p95s = ok_results.map((r) => r.reconnectMs.p95).sort((a, b) => a - b);
const p99s = ok_results.map((r) => r.reconnectMs.p99).sort((a, b) => a - b);
const recoveryMs = ok_results.map((r) => r.recoveryTimeMs).sort((a, b) => a - b);

console.log(`\n=== Aggregate (across ${ok_results.length}/${shardUrls.length} surviving shards) ===`);
console.log(`  Target connections:     ${totalTarget.toLocaleString()}`);
console.log(`  Initially connected:    ${totals.initiallyConnected.toLocaleString()}`);
console.log(`  Disconnected events:    ${totals.disconnected.toLocaleString()}`);
console.log(`  Reconnected:            ${totals.reconnected.toLocaleString()}`);
console.log(
  `  Reconnect rate:         ${
    totals.initiallyConnected > 0
      ? ((totals.reconnected / totals.initiallyConnected) * 100).toFixed(2)
      : "—"
  }%`
);
console.log(`  Never reconnected:      ${totals.neverReconnected.toLocaleString()}`);
console.log(
  `  Per-shard recovery ms:  median=${percentile(recoveryMs, 50)}  p95=${percentile(recoveryMs, 95)}  max=${percentile(recoveryMs, 100)}`
);
console.log(
  `  Per-shard reconnect:    median p50=${percentile(p50s, 50)}  p95=${percentile(p95s, 50)}  p99=${percentile(p99s, 50)}`
);

if (errors.length > 0) {
  console.log(`\n${errors.length} shard(s) errored or timed out:`);
  for (const e of errors) console.log(`  - ${e}`);
}

// JSON dump for offline analysis
const dump = {
  startedAt: startedAt.toISOString(),
  endedAt: endedAt.toISOString(),
  config: { perShardN, rampPerSec, prearmSec, recoveryWaitSec, wsUrl, totalTarget, shards: shardUrls.length },
  totals,
  per_shard: ok_results,
  errors,
};
import("fs").then((fs) => {
  const path = `avalanche-multi-uws-${startedAt.toISOString().replace(/[:.]/g, "-")}.json`;
  fs.writeFileSync(path, JSON.stringify(dump, null, 2));
  console.log(`\nWrote ${path}`);
});
