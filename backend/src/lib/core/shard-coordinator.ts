// Driver-side helper for fanning out one bench across k bench-runner shards.
//
// Each shard is a separate Railway container with its own outbound port pool,
// memory, and event loop. Total work = sum of per-shard work; effective
// scale ceiling becomes k × per-shard-ceiling. Used by the jitter, latency,
// and (eventually) whispers / throughput multi-shard drivers.
//
// Protocol: each bench-runner endpoint already supports `?async=1` (see
// lib/job-queue.ts). We POST to each shard with that flag, collect jobIds,
// and poll GET /jobs/:id on each until status=done|failed. Long bench-runner
// runs no longer hit Railway's 5-min HTTP edge timeout because every request
// here is short.
//
// Result merging is the caller's concern — the coordinator only returns the
// per-shard outcomes. See bench/jitter-multi.ts for how to merge JitterResult.

import { Agent, setGlobalDispatcher } from "undici";

import { benchRunnerFetch } from "./bench-runner-client.js";

// New-style bench-runners respond to enqueue POSTs sub-second; poll GETs
// are similarly fast. But OLD bench-runners (pre-async-mode) ignore
// ?async=1 and block the POST for the full test duration. Pad timeouts
// generously so we can stay backwards-compatible with the older fleet,
// matching what bench/idle-multi.ts uses for the same reason.
setGlobalDispatcher(
  new Agent({
    headersTimeout: 30 * 60 * 1000,
    bodyTimeout: 30 * 60 * 1000,
  }),
);

export interface ShardSpec {
  // Bench-runner base URL (no trailing slash). e.g. https://bench-runner-1.up.railway.app
  url: string;
  // Short label used in progress logs. e.g. "shard-1".
  label: string;
  // Endpoint path. e.g. "bench-jitter-anycable" (no leading slash).
  endpoint: string;
  // Query string params for this shard. Numbers get stringified.
  query: Record<string, string | number>;
}

export interface ShardOutcome<T> {
  spec: ShardSpec;
  jobId: string;
  status: "done" | "failed";
  // Whichever the bench-runner returned. Caller picks the merge strategy.
  result?: T;
  error?: string;
  // Wall-clock from enqueue response to terminal poll.
  durationMs: number;
  // Final ring-buffer tail from the bench-runner, useful for diagnostics.
  finalLog: string[];
}

export interface RunShardsOptions {
  // How often to poll each shard's /jobs/:id. Default 5s — enough that we
  // don't drown Railway, fast enough to catch finish within one cycle.
  pollIntervalMs?: number;
  // How many log lines to fetch per poll. Default 30.
  pollLogLines?: number;
  // Print streaming progress lines as logTails change. Default true.
  printProgress?: boolean;
  // Per-shard absolute ceiling. If a shard hasn't reported done by then,
  // we abandon polling on it and mark it failed. Default 60 min.
  shardTimeoutMs?: number;
  // New-style runners echo {effectiveParams, unknownParams} in the 202
  // enqueue response. By default a shard fails fast when the runner
  // reports it would ignore a param we sent, or when an echoed value
  // differs from what we sent — both mean the test is about to run with
  // different parameters than the driver believes. Set true only for a
  // deliberate mixed-fleet run.
  allowParamMismatch?: boolean;
}

interface ShardState<T> {
  spec: ShardSpec;
  jobId?: string;
  enqueueError?: string;
  status: "pending" | "running" | "done" | "failed";
  result?: T;
  error?: string;
  startedAt: number;
  endedAt?: number;
  lastLogTail: string[];
  lastPrintedLogIdx: number;
}

// Compare what we sent against what the runner says it parsed. Only keys
// present under the same name on both sides are compared — parser-side
// names sometimes differ (msgs → totalMessages), and the unknown-params
// check covers keys the runner doesn't read at all.
function paramEchoMismatches(
  sent: Record<string, string | number>,
  effective: Record<string, unknown>,
): string[] {
  const mismatches: string[] = [];
  for (const [k, v] of Object.entries(sent)) {
    if (!(k in effective)) continue;
    const echoed = effective[k];
    if (echoed === undefined || echoed === null) continue;
    if (String(echoed) !== String(v)) {
      mismatches.push(`${k}: sent ${v}, runner parsed ${String(echoed)}`);
    }
  }
  return mismatches;
}

// Enqueue returns one of three shapes:
//   - {jobId}     new-style async bench-runner; caller polls /jobs/:id
//   - {syncResult} pre-async bench-runner ignored ?async=1 and returned
//                  the full result inline. Treat as already-done.
//   - {error}     network / HTTP failure, or a param the runner would
//                 ignore / misparse (fail fast before wasting the run).
async function enqueueShard(
  shard: ShardSpec,
  allowParamMismatch: boolean,
): Promise<
  { jobId: string } | { syncResult: unknown } | { error: string }
> {
  const qs = new URLSearchParams();
  qs.set("async", "1");
  for (const [k, v] of Object.entries(shard.query)) {
    qs.set(k, String(v));
  }
  const url = `${shard.url}/${shard.endpoint}?${qs.toString()}`;
  try {
    const res = await benchRunnerFetch(url, { method: "POST" });
    if (!res.ok) {
      return { error: `enqueue HTTP ${res.status} ${res.statusText}` };
    }
    const body = (await res.json()) as Record<string, unknown>;
    if (typeof body.jobId === "string") {
      // Params echo verification (new-style runners only). A shard that
      // would silently ignore or default a param is failed here, before
      // the fleet burns a full run measuring the wrong configuration.
      const unknown = Array.isArray(body.unknownParams)
        ? (body.unknownParams as string[])
        : undefined;
      if (unknown === undefined) {
        console.log(
          `  ! ${shard.label}: runner does not echo params (old image?) — param verification skipped`,
        );
      } else if (unknown.length > 0) {
        const msg = `runner ignores query params: ${unknown.join(", ")} (endpoint /${shard.endpoint} does not read them; check the key names)`;
        if (!allowParamMismatch) return { error: msg };
        console.log(`  ! ${shard.label}: ${msg}`);
      }
      if (body.effectiveParams && typeof body.effectiveParams === "object") {
        const mismatches = paramEchoMismatches(
          shard.query,
          body.effectiveParams as Record<string, unknown>,
        );
        if (mismatches.length > 0) {
          const msg = `param echo mismatch: ${mismatches.join("; ")}`;
          if (!allowParamMismatch) return { error: msg };
          console.log(`  ! ${shard.label}: ${msg}`);
        }
      }
      return { jobId: body.jobId };
    }
    // Old bench-runner: returned the full sync result instead of {jobId}.
    // Anything with a result-shaped body counts; the caller decides what
    // fields it cares about.
    return { syncResult: body };
  } catch (err) {
    return { error: err instanceof Error ? err.message : String(err) };
  }
}

interface JobStatusResponse {
  id: string;
  status: "running" | "done" | "failed";
  result?: unknown;
  error?: string;
  logTail: string[];
  durationMs: number;
}

async function pollShard(
  shard: ShardSpec,
  jobId: string,
  logLines: number,
): Promise<JobStatusResponse | { error: string }> {
  const url = `${shard.url}/jobs/${jobId}?logLines=${logLines}`;
  try {
    const res = await benchRunnerFetch(url);
    if (!res.ok) {
      return { error: `poll HTTP ${res.status} ${res.statusText}` };
    }
    return (await res.json()) as JobStatusResponse;
  } catch (err) {
    return { error: err instanceof Error ? err.message : String(err) };
  }
}

// Print only the log lines we haven't seen yet, prefixed with the shard label.
// Lets multiple shards stream progress to one terminal without spam-rewriting.
function printNewLog<T>(state: ShardState<T>, logTail: string[]): void {
  // logTail is "last N lines"; we tracked how many we already printed by
  // saving the line text at lastPrintedLogIdx. Find that line in the new
  // tail and print everything after it. Falls back to printing all if not
  // found (shouldn't happen unless ring-buffer overflowed between polls).
  if (state.lastLogTail.length === 0) {
    for (const line of logTail) {
      console.log(`  [${state.spec.label}] ${line}`);
    }
    state.lastLogTail = [...logTail];
    return;
  }
  const lastSeen = state.lastLogTail[state.lastLogTail.length - 1];
  const idx = logTail.lastIndexOf(lastSeen);
  const fresh = idx === -1 ? logTail : logTail.slice(idx + 1);
  for (const line of fresh) {
    console.log(`  [${state.spec.label}] ${line}`);
  }
  state.lastLogTail = [...logTail];
}

export async function runShards<T>(
  shards: ShardSpec[],
  opts: RunShardsOptions = {},
): Promise<ShardOutcome<T>[]> {
  const pollIntervalMs = opts.pollIntervalMs ?? 5000;
  const pollLogLines = opts.pollLogLines ?? 30;
  const printProgress = opts.printProgress ?? true;
  const shardTimeoutMs = opts.shardTimeoutMs ?? 60 * 60 * 1000;
  const allowParamMismatch = opts.allowParamMismatch ?? false;

  console.log(`Enqueuing ${shards.length} shard(s)...`);
  const states: ShardState<T>[] = shards.map((s) => ({
    spec: s,
    status: "pending",
    startedAt: Date.now(),
    lastLogTail: [],
    lastPrintedLogIdx: -1,
  }));

  await Promise.all(
    states.map(async (state) => {
      const r = await enqueueShard(state.spec, allowParamMismatch);
      if ("error" in r) {
        state.enqueueError = r.error;
        state.status = "failed";
        state.error = r.error;
        state.endedAt = Date.now();
        console.log(`  ✗ ${state.spec.label}: enqueue failed: ${r.error}`);
        return;
      }
      if ("syncResult" in r) {
        // Old bench-runner returned the full result inline; no jobId, no
        // polling needed. Mark done immediately. The bench-runner already
        // held the connection for the entire test duration (Railway's
        // 5-min HTTP cap permitting).
        state.status = "done";
        state.result = r.syncResult as T;
        state.endedAt = Date.now();
        console.log(
          `  ✓ ${state.spec.label}: sync result in ${((state.endedAt - state.startedAt) / 1000).toFixed(1)}s`,
        );
        return;
      }
      state.jobId = r.jobId;
      state.status = "running";
      console.log(`  → ${state.spec.label}: jobId=${r.jobId}`);
    }),
  );

  // Poll loop. Each cycle hits every still-running shard once in parallel.
  while (states.some((s) => s.status === "running")) {
    await new Promise((r) => setTimeout(r, pollIntervalMs));

    await Promise.all(
      states.map(async (state) => {
        if (state.status !== "running") return;
        if (!state.jobId) return;

        if (Date.now() - state.startedAt > shardTimeoutMs) {
          state.status = "failed";
          state.error = `shard timeout after ${Math.round(shardTimeoutMs / 1000)}s`;
          state.endedAt = Date.now();
          console.log(`  ✗ ${state.spec.label}: ${state.error}`);
          return;
        }

        const r = await pollShard(state.spec, state.jobId, pollLogLines);
        if ("error" in r) {
          // Transient poll failure — log but keep going. We only mark the
          // shard failed if it stays unreachable till shardTimeoutMs.
          if (printProgress) {
            console.log(`  ? ${state.spec.label}: poll error (${r.error})`);
          }
          return;
        }
        if (printProgress && r.logTail.length > 0) {
          printNewLog(state, r.logTail);
        }
        if (r.status === "done") {
          state.status = "done";
          state.result = r.result as T;
          state.endedAt = Date.now();
          console.log(
            `  ✓ ${state.spec.label}: done in ${(r.durationMs / 1000).toFixed(1)}s`,
          );
        } else if (r.status === "failed") {
          state.status = "failed";
          state.error = r.error || "unknown error";
          state.endedAt = Date.now();
          console.log(`  ✗ ${state.spec.label}: ${state.error}`);
        }
      }),
    );
  }

  return states.map((s) => ({
    spec: s.spec,
    jobId: s.jobId ?? "",
    status: s.status === "done" ? "done" : "failed",
    result: s.result,
    error: s.error,
    durationMs: (s.endedAt ?? Date.now()) - s.startedAt,
    finalLog: s.lastLogTail,
  }));
}
