// Deploy-impact benchmark — Socket.io with Redis adapter, rolling deploy
//
// Drives the bench-runner's /bench-deploy-impact-socketio endpoint and
// triggers `railway redeploy` for each cluster node in sequence DURING
// the test window. The bench-runner detects deploys via first-disconnect
// so no precise coordination is needed; we just need to fire the deploys
// during the preDeploySec window.
//
// Usage:
//   BENCH_RUNNER_URL=https://bench-runner-production.up.railway.app \
//   RAILWAY_SERVICES=socketio-server-redis-a,socketio-server-redis-b \
//   NUM_CLIENTS=10000 PRE_DEPLOY_SEC=30 POST_DEPLOY_SEC=180 \
//     tsx src/bench/deploy-impact-socketio-redis.ts
//
// Requirements:
//   - The local `railway` CLI authenticated and linked to the project
//     (RAILWAY_SERVICES uses `railway redeploy -s <name>`).
//   - bench-runner deployed with /bench-deploy-impact-socketio
//   - At least 2 Socket.io+Redis services up (default: redis-a, redis-b)
//
// Output: prints + writes a JSON file with the full result.

import { spawnSync } from "child_process";
import { writeFileSync } from "fs";
import { Agent, setGlobalDispatcher } from "undici";

import { benchRunnerFetch } from "../lib/core/bench-runner-client.js";
import { resultPath } from "../lib/core/results-dir.js";

// Poll Railway deployment status for a specific service. Returns the
// latest deployment whose createdAt is at-or-after `triggeredAtMs` and
// whose status is SUCCESS. Returns null on timeout. Throws if the
// deployment ends in a terminal failure state.
async function waitForNewSuccessfulDeployment(
  serviceName: string,
  triggeredAtMs: number,
  timeoutMs = 6 * 60 * 1000,
  pollIntervalMs = 5000,
): Promise<{ id: string; status: string; createdAt: string } | null> {
  const startedAt = Date.now();
  // Allow 10s slack on createdAt vs trigger time (clock skew, propagation).
  const minCreatedAt = triggeredAtMs - 10_000;
  while (Date.now() - startedAt < timeoutMs) {
    const result = spawnSync("railway", ["status", "--json"], {
      stdio: ["ignore", "pipe", "pipe"],
      encoding: "utf-8",
    });
    if (result.status !== 0) {
      console.log(`  [poll] railway status failed: ${result.stderr?.slice(0, 200)}`);
      await new Promise((r) => setTimeout(r, pollIntervalMs));
      continue;
    }
    let data: any;
    try {
      data = JSON.parse(result.stdout);
    } catch {
      await new Promise((r) => setTimeout(r, pollIntervalMs));
      continue;
    }
    const env = data?.environments?.edges?.[0]?.node;
    const svc = env?.serviceInstances?.edges?.find(
      (e: any) => e?.node?.serviceName === serviceName,
    )?.node;
    const ld = svc?.latestDeployment;
    if (ld?.createdAt) {
      const createdMs = new Date(ld.createdAt).getTime();
      const status = ld.status || "UNKNOWN";
      if (createdMs >= minCreatedAt) {
        if (status === "SUCCESS") {
          return { id: ld.id, status, createdAt: ld.createdAt };
        }
        if (status === "FAILED" || status === "CRASHED" || status === "REMOVED") {
          throw new Error(
            `deployment for ${serviceName} ended in terminal state ${status}`,
          );
        }
        // INITIALIZING, BUILDING, DEPLOYING — keep polling
        console.log(`  [poll] ${serviceName} status=${status} (${Math.round((Date.now() - startedAt) / 1000)}s)`);
      }
    }
    await new Promise((r) => setTimeout(r, pollIntervalMs));
  }
  return null;
}

// bench-runner requests can take many minutes (steady-state + deploys
// + post-deploy hold). Match the server-side runner's worst-case window.
setGlobalDispatcher(
  new Agent({ headersTimeout: 30 * 60 * 1000, bodyTimeout: 30 * 60 * 1000 }),
);

const benchRunnerUrl = process.env.BENCH_RUNNER_URL;
if (!benchRunnerUrl) {
  console.error("BENCH_RUNNER_URL is required");
  process.exit(1);
}
const services = (process.env.RAILWAY_SERVICES || "socketio-server-redis-a,socketio-server-redis-b")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);
if (services.length === 0) {
  console.error("RAILWAY_SERVICES must list at least one service");
  process.exit(1);
}

const n = parseInt(process.env.NUM_CLIENTS || "10000", 10);
const rampPerSec = parseInt(process.env.RAMP_PER_SEC || "200", 10);
const pubRate = parseInt(process.env.PUB_RATE || "2", 10);
const preDeploy = parseInt(process.env.PRE_DEPLOY_SEC || "30", 10);
const postDeploy = parseInt(process.env.POST_DEPLOY_SEC || "180", 10);
const stream = process.env.STREAM || "deploy-impact";

// Settle window between each node's redeploy. The bench-runner's
// detection is event-driven (first disconnect) so we just need each
// node to fully restart before the next is taken out — otherwise we'd
// have zero serving capacity briefly.
const settleBetweenSec = parseInt(process.env.SETTLE_BETWEEN_SEC || "20", 10);

const tag = `deploy-impact-${new Date().toISOString().replace(/[:.]/g, "-")}`;
const outFile = resultPath(`deploy-impact-socketio-redis-${tag}.json`);

console.log(`Deploy-impact benchmark — Socket.io + Redis adapter`);
console.log(`  bench-runner: ${benchRunnerUrl}`);
console.log(`  cluster nodes (services to redeploy): ${services.join(", ")}`);
console.log(`  N=${n}  ramp=${rampPerSec}/s  pubRate=${pubRate}/s`);
console.log(
  `  preDeploy=${preDeploy}s  postDeploy=${postDeploy}s  settle-between=${settleBetweenSec}s`,
);
console.log(`  output: ${outFile}\n`);

// Kick off the bench-runner request in the background. It will:
// 1. Ramp N clients (rampPerSec) → roughly n/rampPerSec seconds
// 2. Run publisher + hold for preDeploy seconds
// 3. Hold postDeploy seconds for stragglers
// 4. Return the result JSON
// NODES_URLS: comma-separated WS node URLs forwarded to bench-runner so
// it round-robins clients across them. Pair with RAILWAY_SERVICES so the
// service names + URLs are aligned.
const nodesUrlsParam = process.env.NODES_URLS
  ? `&nodes=${encodeURIComponent(process.env.NODES_URLS)}`
  : "";

const url =
  `${benchRunnerUrl}/bench-deploy-impact-socketio` +
  `?n=${n}&ramp=${rampPerSec}&stream=${encodeURIComponent(stream)}` +
  `&pubRate=${pubRate}&preDeploy=${preDeploy}&postDeploy=${postDeploy}${nodesUrlsParam}`;

const benchPromise = (async () => {
  console.log(`POST ${url}`);
  const r = await benchRunnerFetch(url, { method: "POST" });
  if (!r.ok) {
    throw new Error(`bench-runner returned HTTP ${r.status}`);
  }
  return r.json();
})();

// Wait for the ramp + settle + half of preDeploy before triggering the
// first deploy. That centers the deploy in the steady-state window so
// we have publishing happening both before and during/after.
const rampWaitSec = Math.ceil(n / rampPerSec) + 5; // ramp time + settle pad
const deployTriggerDelaySec = rampWaitSec + Math.floor(preDeploy / 2);

const trigger = async () => {
  console.log(
    `\n[driver] waiting ${deployTriggerDelaySec}s before first redeploy ` +
      `(rampWait=${rampWaitSec} + preDeploy/2=${Math.floor(preDeploy / 2)})`,
  );
  await new Promise((r) => setTimeout(r, deployTriggerDelaySec * 1000));

  for (let i = 0; i < services.length; i++) {
    const svc = services[i];
    console.log(
      `\n[driver] redeploying ${svc} (node ${i + 1}/${services.length})`,
    );
    const triggeredAt = Date.now();
    const result = spawnSync("railway", ["redeploy", "-s", svc, "--yes"], {
      stdio: "inherit",
      encoding: "utf-8",
    });
    const triggerElapsedMs = Date.now() - triggeredAt;
    if (result.status !== 0) {
      console.error(
        `[driver] railway redeploy failed for ${svc} (exit ${result.status}, ${triggerElapsedMs}ms)`,
      );
      // Continue to the next node anyway; the bench-runner will record
      // whatever happened.
    } else {
      console.log(
        `[driver] ${svc} redeploy command returned in ${triggerElapsedMs}ms — polling for completion`,
      );
    }
    // Wait for THIS deploy to actually finish before triggering the
    // next node. This is what makes the deploy "rolling" vs "all at
    // once": real rolling deploys ensure each node is back to
    // RUNNING before disrupting the next.
    if (i < services.length - 1) {
      const dep = await waitForNewSuccessfulDeployment(svc, triggeredAt);
      if (dep) {
        const totalMs = Date.now() - triggeredAt;
        console.log(
          `[driver] ${svc} deploy SUCCESS (id=${dep.id.slice(0, 8)}, ${totalMs}ms total) — ${settleBetweenSec}s buffer before next node`,
        );
      } else {
        console.log(
          `[driver] ${svc} deploy poll TIMED OUT — proceeding to next node anyway`,
        );
      }
      // A small buffer after SUCCESS gives traffic a moment to migrate
      // back before we hit the next node.
      await new Promise((r) => setTimeout(r, settleBetweenSec * 1000));
    } else {
      // Last node — just log when it finishes (don't gate further work on it).
      waitForNewSuccessfulDeployment(svc, triggeredAt)
        .then((dep) => {
          if (dep)
            console.log(
              `[driver] final node ${svc} deploy SUCCESS (id=${dep.id.slice(0, 8)})`,
            );
        })
        .catch(() => {});
    }
  }
  console.log(`\n[driver] all ${services.length} nodes redeployed`);
};

(async () => {
  // Fire the trigger in parallel with the bench-runner request
  const [result] = await Promise.all([benchPromise, trigger()]);
  console.log(`\n=== Result ===`);
  console.log(JSON.stringify(result, null, 2));
  writeFileSync(outFile, JSON.stringify(result, null, 2));
  console.log(`\nSaved: ${outFile}`);
})().catch((err) => {
  console.error("FAILED:", err);
  process.exit(1);
});
