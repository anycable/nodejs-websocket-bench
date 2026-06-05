// Show how each manifest test's headline numbers have moved across the
// last N rebaseline runs. Reads tmp/v1.6.14-bench-results/runs/{ts}/{id}.json,
// pulls the same dotted-path fields the manifest's `baseline` block lists,
// and prints a per-test row showing the trend.
//
// Usage:
//   npm run bench:rebaseline:history
//
//   LAST=10                              # how many runs to load (default 5)
//   FILTER=jitter,whispers               # subset
//   OUTPUT_DIR=tmp/v1.6.14-bench-results # match rebaseline's output dir
//
// Output is one block per test, with rows for each tracked field showing
// the values in chronological order plus a direction arrow.

import { readdirSync, readFileSync, statSync, existsSync } from "node:fs";
import { join } from "node:path";

import { tests, type TestSpec } from "./tests-manifest.js";

const outputDir =
  process.env.OUTPUT_DIR ||
  join(process.cwd(), "..", "..", "tmp", "v1.6.14-bench-results");
const last = Math.max(1, parseInt(process.env.LAST || "5", 10) || 5);
const filter = (process.env.FILTER || "").trim();

const runsRoot = join(outputDir, "runs");
if (!existsSync(runsRoot)) {
  console.error(`No history found at ${runsRoot}. Run \`npm run bench:rebaseline\` first.`);
  process.exit(1);
}

// Each run is a directory named with the ISO timestamp; sorted ascending
// because we write the timestamp with a sortable format (YYYY-MM-DDTHH-MM-SS).
const allRuns = readdirSync(runsRoot)
  .filter((name) => {
    try {
      return statSync(join(runsRoot, name)).isDirectory();
    } catch {
      return false;
    }
  })
  .sort();
const runs = allRuns.slice(-last);

if (runs.length === 0) {
  console.error("History dir is empty.");
  process.exit(1);
}

console.log(`Rebaseline history: ${runs.length} run(s) under ${runsRoot}`);
for (const r of runs) console.log(`  ${r}`);
console.log("");

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

function readPath(obj: unknown, path: string): unknown {
  return path.split(".").reduce<unknown>((acc, key) => {
    if (acc && typeof acc === "object" && key in (acc as Record<string, unknown>)) {
      return (acc as Record<string, unknown>)[key];
    }
    return undefined;
  }, obj);
}

// Concise number formatting: 12,345 for big, decimals for small fractions.
function fmt(v: unknown): string {
  if (typeof v !== "number") return String(v ?? "n/a");
  if (Number.isInteger(v) && Math.abs(v) >= 1000) return v.toLocaleString();
  if (Math.abs(v) >= 100) return v.toFixed(0);
  if (Math.abs(v) >= 1) return v.toFixed(2);
  return v.toFixed(3);
}

// Direction arrow + color: green if "better", red if "worse", yellow on
// significant change in either direction. For deliveryRate higher = better;
// for latency / lost / connectFailures lower = better.
function trendArrow(field: string, first: unknown, last: unknown): string {
  if (typeof first !== "number" || typeof last !== "number") return "";
  const delta = last - first;
  if (delta === 0) return ` ${c.dim}─${c.reset}`;
  const isHigherBetter =
    field === "deliveryRatePct" ||
    field.startsWith("connected") ||
    field.startsWith("welcomed") ||
    field.startsWith("subscribed") ||
    field === "csrResumes";
  const pctChange = first !== 0 ? (delta / Math.abs(first)) * 100 : 0;
  const better = isHigherBetter ? delta > 0 : delta < 0;
  const significant = Math.abs(pctChange) > 5;
  const arrow = delta > 0 ? "↑" : "↓";
  const color = better
    ? significant
      ? c.green
      : c.dim
    : significant
      ? c.red
      : c.yellow;
  return ` ${color}${arrow}${significant ? ` ${pctChange >= 0 ? "+" : ""}${pctChange.toFixed(0)}%` : ""}${c.reset}`;
}

const selected = tests.filter(matchesFilter);
if (selected.length === 0) {
  console.error(`No tests matched FILTER="${filter}"`);
  process.exit(1);
}

for (const spec of selected) {
  // For each baseline field, collect the value at each historical run.
  const fields = Object.keys(spec.baseline);
  if (fields.length === 0) continue;

  // Skip tests with no history present in any of the loaded runs.
  let anyRunHadData = false;
  for (const r of runs) {
    if (existsSync(join(runsRoot, r, `${spec.id}.json`))) {
      anyRunHadData = true;
      break;
    }
  }
  if (!anyRunHadData) continue;

  console.log(`${c.bold}${spec.id}${c.reset} ${c.dim}(${spec.category})${c.reset}`);
  for (const field of fields) {
    const values = runs.map((r) => {
      const path = join(runsRoot, r, `${spec.id}.json`);
      if (!existsSync(path)) return undefined;
      try {
        const json = JSON.parse(readFileSync(path, "utf-8")) as unknown;
        return readPath(json, field);
      } catch {
        return undefined;
      }
    });
    const seen = values.filter((v) => v !== undefined);
    if (seen.length === 0) continue;
    const first = seen[0];
    const last = seen[seen.length - 1];
    const trend = trendArrow(field, first, last);
    const series = values.map((v) => fmt(v).padStart(8)).join(" → ");
    console.log(`  ${field.padEnd(28)} ${series}${trend}`);
  }
  console.log("");
}
