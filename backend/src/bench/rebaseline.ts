// Walk the tests manifest, run each against bench-runner, write JSON, and
// print a delta-vs-baseline report.
//
// Usage:
//   BENCH_RUNNER_URL=https://bench-runner-production.up.railway.app \
//     npm run bench:rebaseline
//
//   FILTER=latency-anycable      # only matching IDs
//   FILTER=jitter,whispers       # comma-separated category match
//   DRY_RUN=1                    # print plan, don't hit the network
//   OUTPUT_DIR=tmp/v1.6.14-bench-results  # JSON output (default)
//
// What lands per test:
//   - `${OUTPUT_DIR}/${id}.json`            the latest result (overwrites)
//   - `${OUTPUT_DIR}/runs/{ts}/${id}.json`  same result, timestamped (kept)
//   - A line in the terminal showing baseline → current with % drift
//   - Color: green within threshold, yellow above, red if delivery dropped
//
// History dir lets `npm run bench:rebaseline:history` show trend across the
// last N runs without rerunning anything.
//
// Exit code 1 if any test regressed (drift > threshold OR deliveryRate < 99).

import { writeFileSync, mkdirSync, existsSync } from "node:fs";
import { join } from "node:path";
import { Agent, setGlobalDispatcher } from "undici";

import { tests, type TestSpec } from "./tests-manifest.js";
import { runShards, type ShardSpec } from "../lib/shard-coordinator.js";
import { fetchMetric, readRailwayToken } from "../lib/railway-api.js";

// Railway project that hosts the bench targets. Hardcoded because it's
// stable across runs; can override with PROJECT_ID for a different env.
const RAILWAY_PROJECT_ID =
  process.env.PROJECT_ID || "fd842a43-8d78-48c0-879f-4b5311c8c004";

// Each test enqueue + poll cycle is fast; the long wait is on the server.
// Pad timeouts so a slow Railway moment doesn't lose a result.
setGlobalDispatcher(
  new Agent({ headersTimeout: 30 * 60 * 1000, bodyTimeout: 30 * 60 * 1000 }),
);

const benchRunnerDefault = process.env.BENCH_RUNNER_URL;
if (!benchRunnerDefault) {
  console.error("BENCH_RUNNER_URL is required (default bench-runner base URL)");
  process.exit(1);
}

const filter = (process.env.FILTER || "").trim();
const dryRun = process.env.DRY_RUN === "1";
const includeIdle = process.env.INCLUDE_IDLE === "1";
const outputDir =
  process.env.OUTPUT_DIR || join(process.cwd(), "..", "..", "tmp", "v1.6.14-bench-results");

// Bench-runner URL pool for multi-shard tests. Defaults to the 50
// production bench-runner replicas; override with comma-separated list.
// The first replica is named `bench-runner` (no -1 suffix); -2..-50 follow.
const benchRunnerUrls = process.env.BENCH_RUNNER_URLS
  ? process.env.BENCH_RUNNER_URLS.split(",")
      .map((s) => s.trim())
      .filter(Boolean)
  : Array.from({ length: 50 }, (_, i) =>
      i === 0
        ? "https://bench-runner-production.up.railway.app"
        : `https://bench-runner-${i + 1}-production.up.railway.app`,
    );

// Color helpers. Off when stdout isn't a TTY (CI, pipes) so the report
// stays grep-friendly. Use ANSI directly; no chalk dep needed.
const useColor = process.stdout.isTTY === true;
const c = {
  reset: useColor ? "\x1b[0m" : "",
  bold: useColor ? "\x1b[1m" : "",
  dim: useColor ? "\x1b[2m" : "",
  green: useColor ? "\x1b[32m" : "",
  yellow: useColor ? "\x1b[33m" : "",
  red: useColor ? "\x1b[31m" : "",
  cyan: useColor ? "\x1b[36m" : "",
};

// FILTER matches when its value is a substring of the id OR a comma-
// separated category. Empty filter = run everything.
function matchesFilter(spec: TestSpec): boolean {
  if (!filter) return true;
  const parts = filter
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);
  for (const p of parts) {
    if (spec.id.toLowerCase().includes(p)) return true;
    if (spec.category === p) return true;
  }
  return false;
}

// Idle tests are slow (~4 min each, 50 shards in parallel). Gate behind
// INCLUDE_IDLE=1 so the everyday rebaseline doesn't take 20+ min on them.
const selected = tests.filter((t) => {
  if (!matchesFilter(t)) return false;
  if (t.category === "idle" && !includeIdle && !filter.includes("idle"))
    return false;
  return true;
});
if (selected.length === 0) {
  console.error(`No tests matched FILTER="${filter}"`);
  process.exit(1);
}

if (!existsSync(outputDir)) mkdirSync(outputDir, { recursive: true });

// Per-run history dir. Filenames per test go in here alongside the latest
// JSON in outputDir, so a `runs/` subdir under the same output root has
// the rebaseline history. The timestamp uses a sortable, filename-safe
// format so directory listings come back in chronological order.
const runTimestamp = new Date()
  .toISOString()
  .replace(/[:.]/g, "-")
  .replace("T", "T")
  .slice(0, 19);
const historyDir = join(outputDir, "runs", runTimestamp);
mkdirSync(historyDir, { recursive: true });

console.log(
  `${c.bold}Rebaselining ${selected.length} test(s)${c.reset}${
    filter ? ` (filter="${filter}")` : ""
  }`,
);
console.log(`Default bench-runner: ${benchRunnerDefault}`);
console.log(`Output dir:           ${outputDir}`);
console.log(`Run timestamp:        ${runTimestamp}\n`);

if (dryRun) {
  console.log(`${c.dim}-- DRY_RUN: showing plan, not running --${c.reset}\n`);
  for (const t of selected) {
    console.log(`  ${t.id}  (${t.category}, ${t.mode})  →  ${t.endpoint}`);
  }
  process.exit(0);
}

interface DeltaRow {
  field: string;
  baseline: number | string;
  current: number | string | undefined;
  deltaPct?: number;
  status: "ok" | "drift" | "regress" | "missing";
}

// Extract a dotted path from a nested object: "latencyRawMs.p99" → result.latencyRawMs.p99.
function readPath(obj: unknown, path: string): unknown {
  return path.split(".").reduce<unknown>((acc, key) => {
    if (acc && typeof acc === "object" && key in (acc as Record<string, unknown>)) {
      return (acc as Record<string, unknown>)[key];
    }
    return undefined;
  }, obj);
}

function classify(spec: TestSpec, field: string, baseline: unknown, current: unknown): DeltaRow {
  const baseStr = typeof baseline === "number" ? baseline : String(baseline);
  const curStr = current === undefined ? undefined : typeof current === "number" ? current : String(current);

  if (current === undefined || current === null) {
    return { field, baseline: baseStr, current: curStr, status: "missing" };
  }
  if (typeof baseline === "number" && typeof current === "number" && baseline > 0) {
    const deltaPct = ((current - baseline) / baseline) * 100;
    const threshold = spec.driftThresholdPct ?? 5;
    // deliveryRate is special: dropping is bad, but rising is fine.
    if (field === "deliveryRatePct" && current < baseline - 1) {
      return { field, baseline: baseStr, current: curStr, deltaPct, status: "regress" };
    }
    const status: DeltaRow["status"] =
      Math.abs(deltaPct) <= threshold ? "ok" : "drift";
    return { field, baseline: baseStr, current: curStr, deltaPct, status };
  }
  // Non-numeric: just compare strings.
  const ok = String(baseline) === String(current);
  return { field, baseline: baseStr, current: curStr, status: ok ? "ok" : "drift" };
}

function formatDelta(row: DeltaRow): string {
  const tag =
    row.status === "ok"
      ? `${c.green}ok${c.reset}     `
      : row.status === "drift"
        ? `${c.yellow}drift${c.reset}  `
        : row.status === "regress"
          ? `${c.red}regress${c.reset}`
          : `${c.dim}missing${c.reset}`;
  const pct =
    row.deltaPct === undefined
      ? ""
      : ` (${row.deltaPct >= 0 ? "+" : ""}${row.deltaPct.toFixed(1)}%)`;
  return `      ${tag}  ${row.field.padEnd(28)}  ${String(row.baseline).padStart(10)} → ${String(row.current ?? "n/a").padStart(10)}${pct}`;
}

// For multi-shard tests, the bench-runner endpoint must return a shape with
// numeric counts we can sum. The idle endpoints return IdleResult; we also
// attach Railway metrics (peak memory, CPU, derived RAM/conn) when the
// manifest entry has a targetServiceId set.
interface IdleLikeResult {
  connected: number;
  welcomed?: number;
  subscribed?: number;
  failed?: number;
  // Populated from Railway metrics when targetServiceId is set:
  peakMemoryMb?: number;
  peakCpuPercent?: number;
  ramKbPerConnected?: number;
}

// Pull memory + CPU peak from Railway metrics over the test window. Returns
// the values if successful, undefined fields if unavailable (no token, no
// projectId, or empty metric response). Failures don't block the test.
async function fetchTargetMetrics(
  serviceId: string,
  startedAt: Date,
  endedAt: Date,
): Promise<{ peakMemoryMb?: number; peakCpuPercent?: number }> {
  let token: string;
  try {
    token = readRailwayToken();
  } catch {
    return {};
  }
  // Pad the window so the ramp-in and tail are captured even if the test
  // returned just before a metric sample point landed.
  const padMs = 30 * 1000;
  const windowStart = new Date(startedAt.getTime() - padMs).toISOString();
  const windowEnd = new Date(endedAt.getTime() + padMs).toISOString();
  try {
    const [memPoints, cpuPoints] = await Promise.all([
      fetchMetric({
        token,
        projectId: RAILWAY_PROJECT_ID,
        serviceId,
        measurement: "MEMORY_USAGE_GB",
        startDate: windowStart,
        endDate: windowEnd,
      }),
      fetchMetric({
        token,
        projectId: RAILWAY_PROJECT_ID,
        serviceId,
        measurement: "CPU_USAGE",
        startDate: windowStart,
        endDate: windowEnd,
      }),
    ]);
    const peakMemGb = memPoints.length > 0
      ? Math.max(...memPoints.map((p) => p.value))
      : undefined;
    const peakCpu = cpuPoints.length > 0
      ? Math.max(...cpuPoints.map((p) => p.value))
      : undefined;
    return {
      peakMemoryMb: peakMemGb !== undefined ? Math.round(peakMemGb * 1024) : undefined,
      peakCpuPercent: peakCpu !== undefined ? Number(peakCpu.toFixed(2)) : undefined,
    };
  } catch {
    return {};
  }
}

// Fan a multi-shard test across `spec.numShards` bench-runner replicas via
// the shard-coordinator and return the merged result. Each shard runs N/k
// clients against the same target server, so the aggregate connected count
// is what the page reports.
async function runMultiShard(spec: TestSpec): Promise<IdleLikeResult> {
  if (!spec.numShards || !spec.perShardN) {
    throw new Error(`${spec.id}: multi-shard mode needs numShards + perShardN`);
  }
  if (benchRunnerUrls.length < spec.numShards) {
    throw new Error(
      `${spec.id}: needs ${spec.numShards} shards but BENCH_RUNNER_URLS only has ${benchRunnerUrls.length}`,
    );
  }
  const shards: ShardSpec[] = benchRunnerUrls
    .slice(0, spec.numShards)
    .map((url, i) => ({
      url,
      label: `s${i + 1}`,
      endpoint: spec.endpoint,
      query: {
        n: spec.perShardN!,
        shard: `s${i + 1}`,
        ...Object.fromEntries(
          Object.entries(spec.params).map(([k, v]) => [k, String(v)]),
        ),
      },
    }));

  // Use async on each shard via the coordinator. printProgress off because
  // 50-shard log streaming is noisy; the per-shard outcomes get printed below.
  const outcomes = await runShards<IdleLikeResult>(shards, {
    pollIntervalMs: 10_000,
    printProgress: false,
    shardTimeoutMs: 15 * 60 * 1000,
  });

  const totals: IdleLikeResult = {
    connected: 0,
    welcomed: 0,
    subscribed: 0,
    failed: 0,
  };
  let shardErrors = 0;
  for (const o of outcomes) {
    if (o.status === "done" && o.result) {
      totals.connected += o.result.connected ?? 0;
      totals.welcomed = (totals.welcomed ?? 0) + (o.result.welcomed ?? 0);
      totals.subscribed = (totals.subscribed ?? 0) + (o.result.subscribed ?? 0);
      totals.failed = (totals.failed ?? 0) + (o.result.failed ?? 0);
    } else {
      shardErrors++;
    }
  }
  if (shardErrors > 0) {
    console.log(
      `      ${c.yellow}${shardErrors} of ${outcomes.length} shard(s) failed; totals cover the rest${c.reset}`,
    );
  }
  return totals;
}

// Wraps runMultiShard to also fetch + attach Railway metrics for the target.
async function runMultiShardWithMetrics(
  spec: TestSpec,
): Promise<IdleLikeResult> {
  const startedAt = new Date();
  const totals = await runMultiShard(spec);
  const endedAt = new Date();
  if (spec.targetServiceId) {
    const metrics = await fetchTargetMetrics(
      spec.targetServiceId,
      startedAt,
      endedAt,
    );
    if (metrics.peakMemoryMb !== undefined) {
      totals.peakMemoryMb = metrics.peakMemoryMb;
      if (totals.connected > 0) {
        totals.ramKbPerConnected = Number(
          ((metrics.peakMemoryMb * 1024) / totals.connected).toFixed(2),
        );
      }
    }
    if (metrics.peakCpuPercent !== undefined) {
      totals.peakCpuPercent = metrics.peakCpuPercent;
    }
  }
  return totals;
}

// Enqueue + (poll if async). Returns the raw result JSON.
async function runTest(spec: TestSpec, baseUrl: string): Promise<unknown> {
  if (spec.mode === "multi-shard") {
    return runMultiShardWithMetrics(spec);
  }

  const qs = new URLSearchParams();
  if (spec.mode === "async") qs.set("async", "1");
  for (const [k, v] of Object.entries(spec.params)) qs.set(k, String(v));
  const url = `${baseUrl}/${spec.endpoint}?${qs.toString()}`;

  const res = await fetch(url, { method: "POST" });
  if (!res.ok) {
    throw new Error(`enqueue HTTP ${res.status} ${res.statusText}`);
  }
  const body = (await res.json()) as Record<string, unknown>;

  if (spec.mode === "sync") return body;

  const jobId = body.jobId as string | undefined;
  if (!jobId) throw new Error("async response missing jobId");

  // Poll. Cap roughly at 30 min; almost everything is well under that.
  const deadline = Date.now() + 30 * 60 * 1000;
  let lastLog = "";
  while (Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 5000));
    const pollRes = await fetch(`${baseUrl}/jobs/${jobId}?logLines=10`);
    if (!pollRes.ok) {
      // Transient HTTP errors during a long bench shouldn't kill the run.
      continue;
    }
    const poll = (await pollRes.json()) as {
      status: string;
      result?: unknown;
      error?: string;
      logTail?: string[];
      durationMs?: number;
    };
    if (poll.logTail && poll.logTail.length > 0) {
      const newest = poll.logTail[poll.logTail.length - 1];
      if (newest && newest !== lastLog) {
        process.stdout.write(`      ${c.dim}${newest.slice(0, 80)}${c.reset}\n`);
        lastLog = newest;
      }
    }
    if (poll.status === "done") return poll.result;
    if (poll.status === "failed") throw new Error(poll.error || "job failed");
  }
  throw new Error("poll timed out after 30 min");
}

const startedAt = new Date();
let okCount = 0;
let driftCount = 0;
let regressCount = 0;
const failures: { spec: TestSpec; error: string }[] = [];

for (const spec of selected) {
  const baseUrl = spec.benchRunner || benchRunnerDefault;
  const startedMs = Date.now();
  console.log(
    `${c.cyan}▶${c.reset} ${c.bold}${spec.id}${c.reset}  ${c.dim}${spec.category}/${spec.mode} via ${spec.endpoint}${c.reset}`,
  );
  try {
    const result = await runTest(spec, baseUrl);
    const elapsed = ((Date.now() - startedMs) / 1000).toFixed(1);

    // Write raw result to disk. Latest copy at `outputDir/${id}.json`
    // (overwritten) so manual jq queries find it where they expect;
    // historical copy at `outputDir/runs/${ts}/${id}.json` for trend.
    const json = JSON.stringify(result, null, 2);
    const outPath = join(outputDir, `${spec.id}.json`);
    writeFileSync(outPath, json);
    writeFileSync(join(historyDir, `${spec.id}.json`), json);

    // Compute deltas vs baseline.
    const rows: DeltaRow[] = [];
    let testRegressed = false;
    let testDrifted = false;
    for (const [field, base] of Object.entries(spec.baseline)) {
      const current = readPath(result, field);
      const row = classify(spec, field, base, current);
      rows.push(row);
      if (row.status === "regress") testRegressed = true;
      if (row.status === "drift") testDrifted = true;
    }

    const summary =
      testRegressed
        ? `${c.red}REGRESS${c.reset}`
        : testDrifted
          ? `${c.yellow}drift${c.reset}`
          : `${c.green}ok${c.reset}`;
    console.log(`      ${summary}  ${elapsed}s  →  ${outPath}`);
    for (const row of rows) console.log(formatDelta(row));

    if (testRegressed) regressCount++;
    else if (testDrifted) driftCount++;
    else okCount++;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.log(`      ${c.red}FAILED${c.reset}  ${msg}`);
    failures.push({ spec, error: msg });
  }
  console.log("");
}

const endedAt = new Date();
const totalSec = ((endedAt.getTime() - startedAt.getTime()) / 1000).toFixed(1);

console.log(`${c.bold}=== Summary ===${c.reset}`);
console.log(
  `  ${c.green}ok:${c.reset}      ${okCount}` +
    `  ${c.yellow}drift:${c.reset}  ${driftCount}` +
    `  ${c.red}regress:${c.reset} ${regressCount}` +
    `  ${c.red}failed:${c.reset} ${failures.length}` +
    `  total: ${selected.length}`,
);
console.log(`  elapsed: ${totalSec}s`);
if (failures.length > 0) {
  console.log(`\n${c.red}Failures:${c.reset}`);
  for (const f of failures) console.log(`  ${f.spec.id}: ${f.error}`);
}

// Exit non-zero only on regression or hard failure.
process.exit(regressCount > 0 || failures.length > 0 ? 1 : 0);
