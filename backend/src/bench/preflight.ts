// Pre-run fairness and fleet audit. Run before ANY paid benchmark window.
//
//   TARGETS=rails-actioncable,rails-anycable,anycable-go-rails \
//   SHARDS=https://bench-runner-2-production.up.railway.app,... \
//     npm run bench:preflight
//
//   npm run bench:fleet          # inventory/diff only (no SHARDS/TARGETS needed)
//
// Encodes the checks whose absence cost full re-runs in past campaigns:
// stale runner images (locally-validated code that never shipped), secret
// drift across the fleet (silent publisher 401s), deploy churn during a
// window (contaminated numbers), dead targets, and misconfigured env.
// What it cannot verify mechanically it prints as a manual checklist
// (box sizes, worker counts from boot logs).
//
// Exit codes: 0 all checks passed, 1 at least one FAIL.

import { execSync } from "node:child_process";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { checkShardHealth } from "../lib/core/multi-shard.js";
import {
  getServiceVariables,
  isChurning,
  isLive,
  listServiceStates,
  readManifest,
  readRailwayToken,
  resolveProject,
  sha12,
  type ServiceState,
} from "../lib/core/railway-fleet.js";

const mode = process.argv[2] === "status" ? "status" : "preflight";

const here = dirname(fileURLToPath(import.meta.url));
const manifestPath = join(here, "..", "..", "..", "fleet-manifest.json");
if (!existsSync(manifestPath)) {
  console.error(`fleet-manifest.json not found at ${manifestPath}`);
  process.exit(1);
}
const manifest = readManifest(manifestPath);
const runnerRe = new RegExp(manifest.runnerPattern);

let failures = 0;
let warnings = 0;
const fail = (msg: string) => {
  failures++;
  console.log(`  [FAIL] ${msg}`);
};
const warn = (msg: string) => {
  warnings++;
  console.log(`  [warn] ${msg}`);
};
const pass = (msg: string) => console.log(`  [ ok ] ${msg}`);

// ---------------------------------------------------------------------------
// 1. Railway auth + project resolution

console.log("== Railway auth ==");
let token: string;
try {
  token = readRailwayToken();
} catch (e) {
  fail(`cannot read Railway token: ${(e as Error).message}. Run 'railway login'.`);
  process.exit(1);
}
let resolved;
try {
  resolved = await resolveProject(manifest, token);
  pass(`project "${manifest.project}" / env "${manifest.environment}" resolved`);
} catch (e) {
  fail((e as Error).message);
  process.exit(1);
}

// ---------------------------------------------------------------------------
// 2. Fleet inventory + churn

console.log("\n== Fleet inventory ==");
let states: ServiceState[];
try {
  states = await listServiceStates(resolved, token);
} catch (e) {
  fail((e as Error).message);
  process.exit(1);
}
const live = states.filter(isLive);
const churning = states.filter(isChurning);
const liveRunners = live.filter((s) => runnerRe.test(s.serviceName));
const liveOther = live.filter((s) => !runnerRe.test(s.serviceName));
console.log(
  `  ${states.length} services; ${live.length} live (${liveRunners.length} runners, ${liveOther.length} other); ${states.length - live.length} down`,
);
for (const s of liveOther) {
  console.log(`    live: ${s.serviceName} (${s.deployment?.status})`);
}
if (churning.length > 0) {
  fail(
    `deploy churn in progress: ${churning.map((s) => s.serviceName).join(", ")}. Never measure during churn; wait for it to settle.`,
  );
} else {
  pass("no deploy churn");
}

if (mode === "status") {
  console.log(
    `\nManifest expects everything torn down between campaigns; ${live.length} live service(s) are billing right now.`,
  );
  process.exit(failures > 0 ? 1 : 0);
}

// ---------------------------------------------------------------------------
// 3. Targets

const targetNames = (process.env.TARGETS || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

if (targetNames.length > 0) {
  console.log("\n== Targets ==");
  for (const name of targetNames) {
    const state = states.find((s) => s.serviceName === name);
    if (!state) {
      fail(`target "${name}" does not exist in the project`);
      continue;
    }
    if (!isLive(state)) {
      fail(
        `target "${name}" has no live deployment (status: ${state.deployment?.status ?? "none"})`,
      );
      continue;
    }
    pass(`${name} live (deployed ${state.deployment?.createdAt})`);
    const expect = manifest.targets[name];
    if (expect) {
      try {
        const vars = await getServiceVariables(resolved, state.serviceId, token);
        for (const key of expect.expectEnv) {
          if (!(key in vars)) fail(`${name}: expected env var ${key} is not set`);
          else pass(`${name}: ${key} set`);
        }
        for (const [key, forbidden] of Object.entries(expect.forbidEnv ?? {})) {
          if (vars[key] === forbidden) {
            fail(`${name}: ${key}=${forbidden} makes runs silently invalid (see manifest note)`);
          } else {
            pass(`${name}: ${key} is not ${forbidden}`);
          }
        }
        // Heap cap sanity: a NODE_OPTIONS max-old-space-size left over from a
        // smaller box turns reconnect storms into kernel OOM-kill loops.
        if (vars.NODE_OPTIONS && /max-old-space-size/.test(vars.NODE_OPTIONS)) {
          warn(
            `${name}: NODE_OPTIONS pins the heap (${vars.NODE_OPTIONS}); confirm it fits the current container size`,
          );
        }
      } catch (e) {
        warn(`${name}: could not read variables (${(e as Error).message})`);
      }
      if (expect.notes) console.log(`    note: ${expect.notes}`);
    } else {
      warn(`${name}: not in fleet-manifest.json targets; add it with expectEnv + notes`);
    }
  }
}

// ---------------------------------------------------------------------------
// 4. Shards: HTTP health + secrets parity + image freshness

const shardUrls = (process.env.SHARDS || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

if (shardUrls.length > 0) {
  console.log(`\n== Shards (${shardUrls.length}) ==`);

  // Map public URLs back to service names: bench-runner-7-production.up.railway.app
  // → bench-runner-7 (and bench-runner-production → bench-runner).
  const shardServiceNames = shardUrls.map((u) => {
    const host = u.replace(/^https?:\/\//, "").split("/")[0];
    return host.replace(/-production\.up\.railway\.app.*$/, "");
  });
  const excluded = shardServiceNames.filter((n) =>
    manifest.excludedRunners.includes(n),
  );
  if (excluded.length > 0) {
    fail(
      `SHARDS includes excluded runner(s): ${excluded.join(", ")} (see fleet-manifest.json; bench-runner-81 is the Centrifugo target)`,
    );
  }

  // HTTP health + auth + async-API probe.
  const health = await checkShardHealth(shardUrls);
  const badHealth = health.filter((h) => !h.ok);
  if (badHealth.length === 0) pass(`all ${shardUrls.length} shards healthy over HTTP`);
  for (const h of badHealth) fail(`${h.url}: ${h.detail}`);

  // Image freshness: every shard's deployment must postdate the last local
  // commit touching backend/. A stale runner runs old driver code and
  // produces confusing half-failures, never an error.
  let headTime: Date | null = null;
  try {
    headTime = new Date(
      execSync("git log -1 --format=%cI -- .", { cwd: join(here, "..", "..") })
        .toString()
        .trim(),
    );
    // Uncommitted driver/runner changes are invisible to the timestamp
    // check: the deployed image can look "fresh" while missing them.
    const dirty = execSync("git status --porcelain -- src", {
      cwd: join(here, "..", ".."),
    })
      .toString()
      .trim();
    if (dirty) {
      warn(
        "backend/src has uncommitted changes; the freshness check only sees commits. Commit and redeploy before trusting the fleet.",
      );
    }
  } catch {
    warn("could not read git HEAD time; skipping image freshness check");
  }
  if (headTime) {
    const stale: string[] = [];
    for (const name of shardServiceNames) {
      const state = states.find((s) => s.serviceName === name);
      if (!state || !isLive(state)) {
        fail(`shard service "${name}" has no live deployment`);
        continue;
      }
      if (state.deployment && new Date(state.deployment.createdAt) < headTime) {
        stale.push(name);
      }
    }
    if (stale.length > 0) {
      fail(
        `${stale.length} shard(s) run an image OLDER than the last backend commit: ${stale.join(", ")}.\n` +
          `         Redeploy them (concurrency <= 4, retry stragglers):\n` +
          `         for s in ${stale.join(" ")}; do railway up --service "$s" --ci --detach; done`,
      );
    } else {
      pass("every shard image postdates the last backend commit");
    }
  }

  // Secrets parity: sharedSecrets must hash identically on every shard
  // (and match the targets where applicable). Values never printed.
  console.log("  secrets parity (sha12, values never printed):");
  const hashesBySecret: Record<string, Map<string, string[]>> = {};
  for (const name of shardServiceNames) {
    const state = states.find((s) => s.serviceName === name);
    if (!state) continue;
    try {
      const vars = await getServiceVariables(resolved, state.serviceId, token);
      for (const secret of manifest.sharedSecrets) {
        const h = vars[secret] ? sha12(vars[secret]) : "(unset)";
        hashesBySecret[secret] ??= new Map();
        const list = hashesBySecret[secret].get(h) ?? [];
        list.push(name);
        hashesBySecret[secret].set(h, list);
      }
    } catch (e) {
      warn(`${name}: could not read variables (${(e as Error).message})`);
    }
  }
  for (const secret of manifest.sharedSecrets) {
    const groups = hashesBySecret[secret];
    if (!groups) continue;
    if (groups.size === 1 && !groups.has("(unset)")) {
      pass(`${secret}: identical on all ${shardServiceNames.length} shards`);
    } else {
      for (const [h, names] of groups) {
        fail(
          `${secret}: ${h === "(unset)" ? "NOT SET" : `hash ${h}`} on ${names.length} shard(s): ${names.slice(0, 8).join(", ")}${names.length > 8 ? ", ..." : ""}`,
        );
      }
      console.log(
        `         Secret drift causes silent publisher 401s (the uniform-65%-delivery bug class). Fix before running.`,
      );
    }
  }
}

// ---------------------------------------------------------------------------
// 5. What this script cannot verify

console.log(`\n== Manual checks (cannot be verified via API) ==
  - Container sizes EQUAL across all compared targets, set explicitly in the
    dashboard. Results that swap after a resize invalidate everything pre-resize.
  - Worker/process counts from BOOT LOGS, never env vars:
      railway logs --service rails-actioncable | grep -i "cluster mode\\|Worker"
    (stock puma.rb silently ignores WEB_CONCURRENCY without a workers directive)
  - One continuous window for every compared row; no deploys of anything
    (including more shards) once measurement starts.
  - Teardown plan for the end of the window (deploymentRemove per service,
    verify public domains return 404; limits changes alone keep billing).`);

console.log(
  `\n${failures === 0 ? "PREFLIGHT PASSED" : `PREFLIGHT FAILED (${failures} failure(s))`}${warnings > 0 ? `, ${warnings} warning(s)` : ""}`,
);
process.exit(failures > 0 ? 1 : 0);
