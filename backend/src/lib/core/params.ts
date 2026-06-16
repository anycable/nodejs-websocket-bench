// Param parsing for jitter benches — env-var-based for local CLI scripts,
// query-string-based for the Railway bench-runner endpoints. Defaults
// match what the README documents as the canonical run.

import type { Request } from "express";

export interface JitterParams {
  n: number;
  durationSec: number;
  jitterIntervalSec: number;
  jitterDurationMs: number;
  totalMessages: number;
  intervalMs: number;
  rampPerSec: number;
  stream: string;
  // When set, the runner includes a downsampled sorted latency-samples
  // array in the result. The multi-shard coordinator turns this on so
  // it can recompute true merged percentiles across shards.
  samplesCap?: number;
}

const DEFAULT_LOCAL: JitterParams = {
  n: 50,
  durationSec: 150,
  jitterIntervalSec: 15,
  jitterDurationMs: 1000,
  totalMessages: 600,
  intervalMs: 200,
  rampPerSec: 50,
  stream: "benchmark",
};

const DEFAULT_RUNNER: JitterParams = {
  n: 1000,
  durationSec: 160,
  jitterIntervalSec: 15,
  jitterDurationMs: 1000,
  totalMessages: 120,
  intervalMs: 500,
  rampPerSec: 200,
  stream: "bench",
};

function intEnv(name: string, fallback: number): number {
  const v = process.env[name];
  if (!v) return fallback;
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : fallback;
}

export function paramsFromEnv(): JitterParams {
  const samplesCapEnv = parseInt(process.env.SAMPLES_CAP || "", 10);
  return {
    n: intEnv("NUM_CLIENTS", DEFAULT_LOCAL.n),
    durationSec: intEnv("DURATION", DEFAULT_LOCAL.durationSec),
    jitterIntervalSec: intEnv("JITTER_INTERVAL", DEFAULT_LOCAL.jitterIntervalSec),
    jitterDurationMs: intEnv("JITTER_DURATION", DEFAULT_LOCAL.jitterDurationMs),
    totalMessages: intEnv("TOTAL_MESSAGES", DEFAULT_LOCAL.totalMessages),
    intervalMs: intEnv("INTERVAL_MS", DEFAULT_LOCAL.intervalMs),
    rampPerSec: intEnv("RAMP_RATE", DEFAULT_LOCAL.rampPerSec),
    stream: process.env.STREAM || DEFAULT_LOCAL.stream,
    samplesCap: Number.isFinite(samplesCapEnv) && samplesCapEnv > 0
      ? samplesCapEnv
      : undefined,
  };
}

function intQuery(req: Request, name: string, fallback: number): number {
  const v = req.query[name];
  if (typeof v !== "string") return fallback;
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : fallback;
}

export function paramsFromQuery(req: Request): JitterParams {
  const stream = typeof req.query.stream === "string"
    ? req.query.stream
    : `bench-${Date.now()}`;
  const samplesCap = intQuery(req, "samplesCap", 0);
  return {
    n: intQuery(req, "n", DEFAULT_RUNNER.n),
    durationSec: intQuery(req, "duration", DEFAULT_RUNNER.durationSec),
    jitterIntervalSec: intQuery(req, "jitter", DEFAULT_RUNNER.jitterIntervalSec),
    jitterDurationMs: intQuery(req, "jitterMs", DEFAULT_RUNNER.jitterDurationMs),
    totalMessages: intQuery(req, "msgs", DEFAULT_RUNNER.totalMessages),
    intervalMs: intQuery(req, "interval", DEFAULT_RUNNER.intervalMs),
    rampPerSec: intQuery(req, "ramp", DEFAULT_RUNNER.rampPerSec),
    stream,
    samplesCap: samplesCap > 0 ? samplesCap : undefined,
  };
}
