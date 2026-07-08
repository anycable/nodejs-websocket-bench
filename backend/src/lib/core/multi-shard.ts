// The one multi-shard primitive.
//
// Every multi-shard driver (jitter, throughput, idle, whispers, avalanche)
// used to hand-roll the same four things: SHARDS parsing, the protocol →
// target-override query mapping, the fan-out/poll loop, and result merging.
// The mapping was the dangerous one — the channel/acProtocol passthrough
// gap was rediscovered in three drivers, and a param-name mismatch once ran
// a whole throughput suite at the default rate. This module is the single
// place all of that lives; drivers reduce to "pick endpoint, pick per-shard
// params, pick merge".
//
// It also runs the validity checks (lib/core/validity.ts) on every result
// and writes them into the result JSON, so a run that measured the rig
// instead of the server flags itself.

import { writeFileSync } from "node:fs";
import { Agent, setGlobalDispatcher } from "undici";

import { benchRunnerFetch } from "./bench-runner-client.js";
import { resultPath } from "./results-dir.js";
import { runShards, type ShardOutcome, type ShardSpec } from "./shard-coordinator.js";
import {
  formatHumanReport,
  mergeJitterResults,
  type JitterResult,
} from "./stats.js";
import {
  formatValidityReport,
  hasFatal,
  validateResult,
  validateShardSet,
  type ValidityContext,
  type ValidityFlag,
} from "./validity.js";

// Enqueue/poll fetches are short, but old runners that ignore ?async=1
// block the POST for the whole test. Pad timeouts so mixed fleets work.
setGlobalDispatcher(
  new Agent({ headersTimeout: 30 * 60 * 1000, bodyTimeout: 30 * 60 * 1000 }),
);

// ---------------------------------------------------------------------------
// Shared env parsing

export function parseShardUrls(): string[] {
  const csv = process.env.SHARDS;
  if (!csv) {
    console.error(
      "SHARDS env var required (comma-separated bench-runner base URLs)",
    );
    process.exit(1);
  }
  const urls = csv
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  if (urls.length === 0) {
    console.error("SHARDS must contain at least one URL");
    process.exit(1);
  }
  return urls;
}

export type Protocol = "anycable" | "socketio" | "socketio-csr" | "uws";

export function parseProtocol(): Protocol {
  const p = (process.env.PROTOCOL || "anycable").toLowerCase();
  if (p !== "anycable" && p !== "socketio" && p !== "socketio-csr" && p !== "uws") {
    console.error(
      `PROTOCOL must be one of: anycable, socketio, socketio-csr, uws (got "${p}")`,
    );
    process.exit(1);
  }
  return p;
}

// THE protocol → target-override mapping. Reads the same env vars as the
// single-shard drivers and forwards them as query params. Add a new
// override here once and every multi-shard driver gets it — this exact
// list was previously re-implemented (incompletely) per driver.
export function protocolTargetQuery(protocol: Protocol): Record<string, string> {
  const q: Record<string, string> = {};
  if (protocol === "anycable") {
    if (process.env.CABLE_URL) q.cableUrl = process.env.CABLE_URL;
    if (process.env.BROADCAST_URL) q.broadcastUrl = process.env.BROADCAST_URL;
    // Rails targets subscribe to a real channel over the base or extended
    // Action Cable wire protocol; nodejs $pubsub targets leave these unset.
    if (process.env.CHANNEL) q.channel = process.env.CHANNEL;
    if (process.env.AC_PROTOCOL) q.acProtocol = process.env.AC_PROTOCOL;
    // Reconnect matrix: default = each client's stock backoff (real UX),
    // tuned = the uniform aggressive profile (server resume ceiling).
    if (process.env.RECONNECT_MODE) q.reconnectMode = process.env.RECONNECT_MODE;
    if (process.env.RECONNECT_BASE_MS)
      q.reconnectBaseMs = process.env.RECONNECT_BASE_MS;
    // CLIENT_LIB=actioncable drives @rails/actioncable instead of
    // @anycable/core (Action Cable / Solid Cable / Async::Cable targets).
    if (process.env.CLIENT_LIB) q.clientLib = process.env.CLIENT_LIB;
    if (process.env.NATS_URL) q.natsUrl = process.env.NATS_URL;
    if (process.env.NATS_SUBJECT) q.natsSubject = process.env.NATS_SUBJECT;
  }
  if (protocol === "socketio" || protocol === "socketio-csr") {
    if (process.env.SERVER_URL) q.serverUrl = process.env.SERVER_URL;
  }
  if (protocol === "uws") {
    if (process.env.UWS_WS_URL) q.wsUrl = process.env.UWS_WS_URL;
    if (process.env.UWS_HTTP_URL) q.httpUrl = process.env.UWS_HTTP_URL;
  }
  return q;
}

export const ENDPOINTS: Record<string, Record<Protocol, string>> = {
  jitter: {
    anycable: "bench-jitter-anycable",
    socketio: "bench-jitter-socketio",
    "socketio-csr": "bench-jitter-socketio-csr",
    uws: "bench-jitter-uws",
  },
  throughput: {
    anycable: "bench-throughput-anycable",
    socketio: "bench-throughput-socketio",
    "socketio-csr": "bench-throughput-socketio-csr",
    uws: "bench-throughput-uws",
  },
  whispers: {
    anycable: "bench-whispers-anycable",
    socketio: "bench-whispers-socketio",
    "socketio-csr": "bench-whispers-socketio",
    uws: "bench-whispers-uws",
  },
  idle: {
    anycable: "bench-idle-anycable",
    socketio: "bench-idle-socketio",
    "socketio-csr": "bench-idle-socketio",
    uws: "bench-idle-uws",
  },
  avalanche: {
    anycable: "bench-avalanche-anycable",
    socketio: "bench-avalanche-socketio",
    "socketio-csr": "bench-avalanche-socketio",
    uws: "bench-avalanche-uws",
  },
};

// ---------------------------------------------------------------------------
// Health precheck

export interface ShardHealth {
  url: string;
  ok: boolean;
  detail: string;
}

// GET /health on every shard before enqueueing anything. Catches dead
// shards, missing domains, and (via a follow-up authenticated probe)
// stale tokens and pre-async images — before the run, not during.
export async function checkShardHealth(urls: string[]): Promise<ShardHealth[]> {
  return Promise.all(
    urls.map(async (url): Promise<ShardHealth> => {
      try {
        const res = await fetch(`${url}/health`, {
          signal: AbortSignal.timeout(10_000),
        });
        if (!res.ok) return { url, ok: false, detail: `health HTTP ${res.status}` };
        const body = (await res.json()) as { authRequired?: boolean };
        // Authenticated probe: /jobs/nope should be a JSON 404 with a valid
        // token (401 = bad/missing token; HTML 404 = pre-async old image).
        const probe = await benchRunnerFetch(`${url}/jobs/nope`, {
          signal: AbortSignal.timeout(10_000),
        });
        if (probe.status === 401) {
          return { url, ok: false, detail: "token rejected (401) — BENCH_RUNNER_TOKEN mismatch" };
        }
        if (probe.status !== 404) {
          return { url, ok: false, detail: `job probe HTTP ${probe.status}` };
        }
        // New images answer the probe with JSON ({"error":"job not found"});
        // pre-async images fall through to Express's HTML 404. Key on the
        // body, not the content-type header (Railway's edge drops it on
        // some HTTP/1.1 responses).
        const probeBody = await probe.text();
        try {
          JSON.parse(probeBody);
        } catch {
          return { url, ok: false, detail: "job probe returned non-JSON 404 — old image without the async job API" };
        }
        return {
          url,
          ok: true,
          detail: body.authRequired ? "ok (auth on)" : "ok (AUTH OFF)",
        };
      } catch (err) {
        return {
          url,
          ok: false,
          detail: err instanceof Error ? err.message : String(err),
        };
      }
    }),
  );
}

// ---------------------------------------------------------------------------
// The primitive

export interface MultiShardConfig {
  // Used in labels and the result filename: "jitter" | "throughput" | ...
  testType: string;
  protocol: Protocol;
  endpoint: string;
  shardUrls: string[];
  // Per-shard query params. `i` is 0-based shard index; `runStamp` is one
  // timestamp shared by the whole run (use it to build unique streams).
  shardQuery: (i: number, runStamp: number) => Record<string, string | number>;
  // Merge per-shard JitterResults. Defaults to mergeJitterResults.
  merge?: (label: string, results: JitterResult[]) => JitterResult;
  // Context for the validity checks.
  validity?: ValidityContext;
  // Extra fields recorded into the result JSON (test params, notes).
  meta?: Record<string, unknown>;
  // Skip the pre-run health sweep (already done externally).
  skipHealthCheck?: boolean;
  shardTimeoutMs?: number;
}

export interface MultiShardRun {
  outcomes: ShardOutcome<JitterResult>[];
  successes: JitterResult[];
  merged: JitterResult | null;
  flags: ValidityFlag[];
  outPath: string | null;
  // Suggested process exit code: 0 clean, 1 lost deliveries or failed
  // shards, 2 fatal validity flags (do not publish).
  exitCode: number;
}

export async function runMultiShard(cfg: MultiShardConfig): Promise<MultiShardRun> {
  const k = cfg.shardUrls.length;
  const runStamp = Date.now();
  const label = `${cfg.protocol}-${cfg.testType}-multi-${k}`;

  if (!cfg.skipHealthCheck) {
    console.log(`Health sweep: ${k} shard(s)...`);
    const health = await checkShardHealth(cfg.shardUrls);
    const bad = health.filter((h) => !h.ok);
    for (const h of bad) console.error(`  ✗ ${h.url}: ${h.detail}`);
    if (bad.length > 0) {
      console.error(
        `${bad.length}/${k} shard(s) unhealthy. Fix or drop them from SHARDS before burning a run.`,
      );
      process.exit(1);
    }
    console.log(`  ✓ all ${k} shards healthy`);
  }

  const specs: ShardSpec[] = cfg.shardUrls.map((url, i) => ({
    url,
    label: `shard-${i + 1}`,
    endpoint: cfg.endpoint,
    query: cfg.shardQuery(i, runStamp),
  }));

  const startedAt = new Date();
  const outcomes = await runShards<JitterResult>(specs, {
    pollIntervalMs: 5000,
    pollLogLines: 30,
    printProgress: true,
    shardTimeoutMs: cfg.shardTimeoutMs,
  });
  const endedAt = new Date();

  const successOutcomes = outcomes.filter(
    (o): o is typeof o & { result: JitterResult } =>
      o.status === "done" && o.result !== undefined,
  );
  const successes = successOutcomes.map((o) => o.result);
  const failures = outcomes.filter((o) => o.status !== "done");

  console.log("");
  console.log(`=== Per-shard ===  (${successes.length} succeeded / ${outcomes.length})`);
  for (const o of outcomes) {
    if (o.status === "done" && o.result) {
      const r = o.result;
      console.log(
        `  ${o.spec.label}: clients=${r.clients} delivery=${r.deliveryRatePct}% p50=${r.latencyOverMinMs.p50}ms p99=${r.latencyOverMinMs.p99}ms (${(o.durationMs / 1000).toFixed(1)}s)`,
      );
    } else {
      console.log(`  ${o.spec.label}: FAILED — ${o.error || "unknown"}`);
    }
  }
  if (failures.length > 0) {
    console.log(
      `\n!! ${failures.length} shard(s) failed; the merge covers the remaining ${successes.length}. A partial fleet is a different test — rerun unless this was expected.`,
    );
  }
  if (successes.length === 0) {
    console.error("All shards failed; nothing to merge.");
    return {
      outcomes,
      successes,
      merged: null,
      flags: [],
      outPath: null,
      exitCode: 1,
    };
  }

  // Merge. Sync-mode shards lack latencySamplesSorted and make
  // mergeJitterResults throw; per-shard results above stay valid.
  let merged: JitterResult | null = null;
  try {
    const mergeFn = cfg.merge ?? mergeJitterResults;
    merged = mergeFn(label, successes);
    console.log(formatHumanReport(`Merged (${successes.length} shards)`, merged));
  } catch (e) {
    console.error(
      `\nMerge failed (${(e as Error).message}); per-shard summaries above are still valid.`,
    );
  }

  // Validity: per-shard set checks + merged-result checks.
  const flags: ValidityFlag[] = [
    ...validateShardSet(successes, cfg.validity),
    ...(merged ? validateResult(merged, cfg.validity) : []),
  ];
  console.log(formatValidityReport(flags));

  const outPath = resultPath(
    `${cfg.testType}-multi-${cfg.protocol}-${k}shards-${startedAt.toISOString().replace(/[:.]/g, "-")}.json`,
  );
  writeFileSync(
    outPath,
    JSON.stringify(
      {
        testType: cfg.testType,
        protocol: cfg.protocol,
        endpoint: cfg.endpoint,
        shards: k,
        startedAt: startedAt.toISOString(),
        endedAt: endedAt.toISOString(),
        meta: cfg.meta ?? {},
        validity: flags,
        perShard: outcomes.map((o) => ({
          label: o.spec.label,
          url: o.spec.url,
          status: o.status,
          jobId: o.jobId,
          durationMs: o.durationMs,
          result: o.result,
          error: o.error,
        })),
        merged,
      },
      null,
      2,
    ),
  );
  console.log(`\nWrote result JSON: ${outPath}`);

  const exitCode = hasFatal(flags)
    ? 2
    : failures.length > 0 || (merged?.lostDeliveries ?? 0) > 0
      ? 1
      : 0;
  return { outcomes, successes, merged, flags, outPath, exitCode };
}
