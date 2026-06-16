// All bench scripts that write CSV/JSON result files route them through
// resultPath(). Default destination is `backend/results/`, which is
// gitignored except for a .gitkeep so the directory exists on a fresh
// clone. Override with `RESULTS_DIR=/some/path npm run bench:...`.

import { mkdirSync } from "node:fs";
import { join, resolve } from "node:path";

const DEFAULT_DIR = "results";

export function resultsDir(): string {
  return process.env.RESULTS_DIR
    ? resolve(process.env.RESULTS_DIR)
    : DEFAULT_DIR;
}

export function resultPath(filename: string): string {
  const dir = resultsDir();
  mkdirSync(dir, { recursive: true });
  return join(dir, filename);
}
