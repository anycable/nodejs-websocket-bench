// Tiny leveled logger. Existing call sites mostly print "[label] message"
// to stdout/stderr; this wraps that pattern with a level filter so we
// can silence the verbose paths (ramp progress, per-message publish
// failures) without rewriting every call site.
//
// Set BENCH_LOG_LEVEL to one of: silent, error, warn, info (default), debug.
// `info` keeps the one-shot lifecycle lines ("all ramped", "result:");
// `debug` adds the per-1K ramp progress; `warn`/`error` mute info+debug.
//
// Use ".child(label)" so each runner gets its own prefix without
// threading the label through every callsite.

type Level = "silent" | "error" | "warn" | "info" | "debug";

const ORDER: Record<Level, number> = {
  silent: 0,
  error: 1,
  warn: 2,
  info: 3,
  debug: 4,
};

function envLevel(): Level {
  const v = (process.env.BENCH_LOG_LEVEL || "info").toLowerCase();
  return (v in ORDER ? v : "info") as Level;
}

let currentLevel: Level = envLevel();

export function setLogLevel(level: Level): void {
  currentLevel = level;
}

export function getLogLevel(): Level {
  return currentLevel;
}

function shouldEmit(at: Level): boolean {
  return ORDER[at] <= ORDER[currentLevel];
}

export interface Logger {
  error(msg: string): void;
  warn(msg: string): void;
  info(msg: string): void;
  debug(msg: string): void;
  child(suffix: string): Logger;
}

function format(label: string, msg: string): string {
  return label ? `[${label}] ${msg}` : msg;
}

function makeLogger(label: string): Logger {
  return {
    error(msg) { if (shouldEmit("error")) console.error(format(label, msg)); },
    warn(msg) { if (shouldEmit("warn")) console.warn(format(label, msg)); },
    info(msg) { if (shouldEmit("info")) console.log(format(label, msg)); },
    debug(msg) { if (shouldEmit("debug")) console.log(format(label, msg)); },
    child(suffix) { return makeLogger(label ? `${label}:${suffix}` : suffix); },
  };
}

export const log = makeLogger("");
