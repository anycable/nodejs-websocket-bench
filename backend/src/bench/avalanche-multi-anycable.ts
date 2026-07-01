// Multi-shard avalanche driver for AnyCable / Rails Action Cable targets.
//
// Mirrors avalanche-multi-uws.ts but drives the /bench-avalanche-anycable
// endpoint, so the reconnect storm after a real Railway redeploy is generated
// by MANY bench-runner shards in parallel instead of one (a single Node
// process reconnecting thousands of cables is itself load-generator-limited).
// Each shard ramps PER_SHARD_N clients to the same Rails target (channel +
// acProtocol + cableUrl), then the coordinator fires ONE serviceInstanceRedeploy
// against TARGET_SERVICE_ID; every shard sees the disconnect storm at the same
// wall-clock moment, counts its own reconnects, and we aggregate.
//
// prearm/recovery are kept short by default so the blocking shard request stays
// under Railway's ~5-minute public-proxy timeout (reconnect-to-95% is ~8s in
// practice, so a 120s recovery window is ample).
//
// Usage:
//   SHARDS=https://bench-runner-2-production.up.railway.app,... \
//   PER_SHARD_N=250 \
//   CABLE_URL=ws://rails-actioncable.railway.internal:3000/cable \
//   CHANNEL=BenchmarkChannel AC_PROTOCOL=actioncable-v1-json \
//   TARGET_SERVICE_ID=<svc uuid> TARGET_ENV_ID=<env uuid> \
//     tsx src/bench/avalanche-multi-anycable.ts

import { readFileSync, writeFileSync } from "fs";
import { homedir } from "os";
import { Agent, setGlobalDispatcher } from "undici";

import { percentile } from "../lib/core/stats.js";
import { resultPath } from "../lib/core/results-dir.js";

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

const perShardN = parseInt(process.env.PER_SHARD_N || "250", 10);
const rampPerSec = parseInt(process.env.RAMP_PER_SEC || "200", 10);
const prearmSec = parseInt(process.env.PREARM_SEC || "60", 10);
const recoveryWaitSec = parseInt(process.env.RECOVERY_WAIT_SEC || "120", 10);

const cableUrl = process.env.CABLE_URL;
if (!cableUrl) {
  console.error("CABLE_URL env var is required");
  process.exit(1);
}
const channel = process.env.CHANNEL;
const acProtocol = process.env.AC_PROTOCOL;
const clientLib = process.env.CLIENT_LIB;

const targetServiceId = process.env.TARGET_SERVICE_ID;
const targetEnvId = process.env.TARGET_ENV_ID;
if (!targetServiceId || !targetEnvId) {
  console.error("TARGET_SERVICE_ID and TARGET_ENV_ID env vars are required");
  process.exit(1);
}

const bearerToken = process.env.BENCH_RUNNER_TOKEN;

function readRailwayToken(): string {
  if (process.env.RAILWAY_TOKEN) return process.env.RAILWAY_TOKEN;
  const cfg = JSON.parse(readFileSync(`${homedir()}/.railway/config.json`, "utf-8"));
  return cfg.user.token;
}

interface AvalancheShardResult {
  initiallyConnected: number;
  disconnected: number;
  reconnected: number;
  reconnectRatePct: number;
  neverReconnected: number;
  recoveryTimeMs: number;
  reconnectMs: { p50: number; p95: number; p99: number; max: number };
}

const totalTarget = perShardN * shardUrls.length;
const rampSec = Math.ceil(perShardN / rampPerSec);
const shardTimeoutMs = parseInt(
  process.env.SHARD_TIMEOUT_MS ||
    String((rampSec + 5 + prearmSec + recoveryWaitSec + 90) * 1000),
  10
);

console.log(`Multi-shard AnyCable/Rails avalanche: ${shardUrls.length} shards × ${perShardN} = ${totalTarget} clients`);
console.log(`  ramp: ${rampPerSec}/s per shard (~${rampSec}s)`);
console.log(`  prearm: ${prearmSec}s   recoveryWait: ${recoveryWaitSec}s   shard timeout: ${(shardTimeoutMs / 1000).toFixed(0)}s`);
console.log(`  target: ${cableUrl}  channel=${channel} proto=${acProtocol}`);
console.log(`  redeploy service: ${targetServiceId}\n`);

interface ShardOutcome {
  label: string;
  ok: boolean;
  reason?: string;
  result?: AvalancheShardResult;
}

async function runShard(url: string, label: string): Promise<ShardOutcome> {
  const qs = new URLSearchParams({
    n: String(perShardN),
    ramp: String(rampPerSec),
    prearm: String(prearmSec),
    recoveryWait: String(recoveryWaitSec),
    stream: `avalanche-ac-multi-${label}-${Date.now()}`,
    cableUrl: cableUrl as string,
  });
  if (channel) qs.set("channel", channel);
  if (acProtocol) qs.set("acProtocol", acProtocol);
  if (clientLib) qs.set("clientLib", clientLib);

  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), shardTimeoutMs);
  try {
    const res = await fetch(`${url}/bench-avalanche-anycable?${qs.toString()}`, {
      method: "POST",
      headers: bearerToken ? { Authorization: `Bearer ${bearerToken}` } : {},
      signal: ctrl.signal,
    });
    if (!res.ok) {
      console.log(`  ✗ ${label}: ${res.status} ${res.statusText}`);
      return { label, ok: false, reason: `HTTP ${res.status}` };
    }
    const result = (await res.json()) as AvalancheShardResult;
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
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
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

const shardPromises = shardUrls.map((u, i) => runShard(u, `shard-${i + 1}`));

const triggerInSec = rampSec + 10;
console.log(`(ramping in parallel; firing redeploy in ${triggerInSec}s)`);
await new Promise((r) => setTimeout(r, triggerInSec * 1000));

console.log(`\n>>> Firing redeploy on ${targetServiceId}...`);
const ok = await fireRedeploy();
if (!ok) {
  console.error("Redeploy mutation failed; bailing.");
  await Promise.allSettled(shardPromises);
  process.exit(1);
}
console.log(`Redeploy triggered. Awaiting shard results...\n`);

const settled = await Promise.allSettled(shardPromises);
const endedAt = new Date();
console.log(`\nTest ended at ${endedAt.toISOString()}`);
console.log(`Total elapsed: ${((endedAt.getTime() - startedAt.getTime()) / 1000).toFixed(1)}s`);

const ok_results: AvalancheShardResult[] = [];
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

const recoveryMs = ok_results.map((r) => r.recoveryTimeMs).sort((a, b) => a - b);
const p50s = ok_results.map((r) => r.reconnectMs.p50).sort((a, b) => a - b);
const p95s = ok_results.map((r) => r.reconnectMs.p95).sort((a, b) => a - b);
const p99s = ok_results.map((r) => r.reconnectMs.p99).sort((a, b) => a - b);

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
  `  Recovery (time to 95%): median=${percentile(recoveryMs, 50)}ms  p95=${percentile(recoveryMs, 95)}ms  max=${percentile(recoveryMs, 100)}ms`
);
console.log(
  `  Per-shard reconnect:    median p50=${percentile(p50s, 50)}  p95=${percentile(p95s, 50)}  p99=${percentile(p99s, 50)}`
);

if (errors.length > 0) {
  console.log(`\n${errors.length} shard(s) errored or timed out:`);
  for (const e of errors) console.log(`  - ${e}`);
}

const dump = {
  startedAt: startedAt.toISOString(),
  endedAt: endedAt.toISOString(),
  config: { perShardN, rampPerSec, prearmSec, recoveryWaitSec, cableUrl, channel, acProtocol, totalTarget, shards: shardUrls.length },
  totals,
  per_shard: ok_results,
  errors,
};
const path = resultPath(`avalanche-multi-anycable-${startedAt.toISOString().replace(/[:.]/g, "-")}.json`);
writeFileSync(path, JSON.stringify(dump, null, 2));
console.log(`\nWrote ${path}`);
