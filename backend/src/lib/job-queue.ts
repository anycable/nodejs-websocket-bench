// Async job model for bench-runner endpoints.
//
// Long-running benches (>5 min) die at Railway's edge proxy if the HTTP
// response is held open. Solution: enqueue → return {jobId} → poll
// GET /jobs/:id until status=done. The work runs in-process on the same
// bench-runner; the job map is in-memory (no Redis, no persistence — a
// container restart drops in-flight jobs, which is fine for benches we
// can always rerun).
//
// Log capture uses AsyncLocalStorage to tee stdout into the running
// job's ring buffer without changing any runner signatures. Railway
// logs remain the source of truth; the job's logTail is just enough
// for a driver to differentiate "running and progressing" from "stuck".

import { AsyncLocalStorage } from "node:async_hooks";
import { randomBytes } from "node:crypto";

export type JobStatus = "running" | "done" | "failed";

export interface Job {
  id: string;
  status: JobStatus;
  startedAt: number;
  endedAt?: number;
  // Last LOG_CAP stdout lines emitted while this job was the current ALS
  // context. Older lines are dropped on overflow.
  log: string[];
  result?: unknown;
  error?: string;
}

// Per-job stdout history cap. 500 lines covers a ramp-to-50K test at the
// existing "ramped X/N" cadence (~50 ramp lines + summary lines) with
// headroom; bigger benches just keep the most recent.
const LOG_CAP = 500;

// Sweep finished jobs after this long so a long-running bench-runner
// doesn't accumulate memory across many test runs.
const TTL_MS = 30 * 60 * 1000;

const jobs = new Map<string, Job>();
const als = new AsyncLocalStorage<{ jobId: string }>();

// Tee process.stdout.write into the currently-running job's ring buffer.
// We don't suppress the original write — Railway's log stream stays the
// source of truth for everything (errors, stderr, multi-job interleave).
// The job log is a convenience for the driver, not a replacement.
let teed = false;
function installStdoutTee() {
  if (teed) return;
  teed = true;
  const origWrite = process.stdout.write.bind(process.stdout);
  process.stdout.write = ((chunk: unknown, encoding?: unknown, cb?: unknown) => {
    const ctx = als.getStore();
    if (ctx) {
      const job = jobs.get(ctx.jobId);
      if (job) {
        const text =
          typeof chunk === "string"
            ? chunk
            : Buffer.isBuffer(chunk)
              ? chunk.toString("utf8")
              : String(chunk);
        for (const line of text.split(/\r?\n/)) {
          if (!line) continue;
          job.log.push(line);
          if (job.log.length > LOG_CAP) job.log.shift();
        }
      }
    }
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return (origWrite as any)(chunk, encoding, cb);
  }) as typeof process.stdout.write;
}
installStdoutTee();

function newJobId(): string {
  return randomBytes(6).toString("hex");
}

// Start `fn` in the background and return its jobId immediately.
// The Express request handler should respond with 202 + {jobId} as soon as
// this returns. The fn's awaits inherit the ALS context so any console.log
// during the run tees into the job's log buffer.
export function startJob<T>(fn: () => Promise<T>): string {
  const id = newJobId();
  const job: Job = { id, status: "running", startedAt: Date.now(), log: [] };
  jobs.set(id, job);
  als.run({ jobId: id }, () => {
    // Fire-and-forget: we own the result via the closure, not the caller.
    fn().then(
      (result) => {
        job.status = "done";
        job.endedAt = Date.now();
        job.result = result;
      },
      (err: unknown) => {
        job.status = "failed";
        job.endedAt = Date.now();
        job.error =
          err instanceof Error ? err.stack || err.message : String(err);
        // Echo to stderr so Railway logs still surface the failure clearly.
        console.error(`[job ${id}] failed:`, err);
      },
    );
  });
  return id;
}

export function getJob(id: string): Job | undefined {
  return jobs.get(id);
}

// Periodic cleanup: drop done/failed jobs older than TTL so memory doesn't
// grow with each test. .unref() keeps the timer from blocking process exit.
setInterval(() => {
  const cutoff = Date.now() - TTL_MS;
  for (const [id, job] of jobs) {
    if (job.endedAt && job.endedAt < cutoff) jobs.delete(id);
  }
}, 60_000).unref();
