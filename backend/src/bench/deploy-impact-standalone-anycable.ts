// Standalone deploy-impact driver — AnyCable variant (A2c-anycable).
//
// Mirrors deploy-impact-standalone-socketio.ts but holds AnyCable
// clients against anycable-go and triggers `railway redeploy publisher`
// mid-test. The anycable-go service is untouched; only the publisher
// service restarts.
//
// Usage:
//   BENCH_RUNNER_URL=https://bench-runner-production.up.railway.app \
//   PUBLISHER_SERVICE=publisher-anycable \
//   NUM_CLIENTS=10000 DURATION_SEC=240 \
//     tsx src/bench/deploy-impact-standalone-anycable.ts
//
// Requirements:
//   - publisher Railway service configured with
//     WS_BROADCAST_URL=http://anycable-go.railway.internal:8090/_broadcast
//   - bench-runner has /bench-deploy-impact-standalone-anycable endpoint
//   - local railway CLI authenticated + linked to the project

import { spawnSync } from "child_process";
import { writeFileSync } from "fs";
import { Agent, setGlobalDispatcher } from "undici";

import { benchRunnerFetch } from "../lib/bench-runner-client.js";
import { resultPath } from "../lib/results-dir.js";

setGlobalDispatcher(
  new Agent({ headersTimeout: 30 * 60 * 1000, bodyTimeout: 30 * 60 * 1000 }),
);

const benchRunnerUrl = process.env.BENCH_RUNNER_URL;
if (!benchRunnerUrl) {
  console.error("BENCH_RUNNER_URL is required");
  process.exit(1);
}
const publisherService = process.env.PUBLISHER_SERVICE || "publisher";
const n = parseInt(process.env.NUM_CLIENTS || "10000", 10);
const durationSec = parseInt(process.env.DURATION_SEC || "240", 10);
const stream = process.env.STREAM || "standalone-publisher";
const rampPerSec = parseInt(process.env.RAMP_PER_SEC || "200", 10);
const cableUrl = process.env.CABLE_URL;
const redeployFraction = parseFloat(
  process.env.REDEPLOY_FRACTION || "0.33",
);

const tag = `standalone-deploy-impact-anycable-${new Date().toISOString().replace(/[:.]/g, "-")}`;
const outFile = resultPath(`${tag}.json`);

console.log(`Standalone deploy-impact (AnyCable)`);
console.log(`  bench-runner:      ${benchRunnerUrl}`);
console.log(`  publisher service: ${publisherService}`);
console.log(`  N=${n}  ramp=${rampPerSec}/s  duration=${durationSec}s`);
console.log(`  redeploy at ${Math.floor(durationSec * redeployFraction)}s into test`);
console.log(`  output: ${outFile}\n`);

const qs = new URLSearchParams({
  n: String(n),
  ramp: String(rampPerSec),
  stream,
  duration: String(durationSec),
});
if (cableUrl) qs.set("cableUrl", cableUrl);

const url = `${benchRunnerUrl}/bench-deploy-impact-standalone-anycable?${qs.toString()}`;

const benchPromise = (async () => {
  console.log(`POST ${url}`);
  const r = await benchRunnerFetch(url, { method: "POST" });
  if (!r.ok) {
    throw new Error(`bench-runner returned HTTP ${r.status}`);
  }
  return r.json();
})();

const rampWaitSec = Math.ceil(n / rampPerSec) + 5;
const redeployTriggerDelaySec =
  rampWaitSec + Math.floor(durationSec * redeployFraction);

const trigger = async () => {
  console.log(
    `\n[driver] waiting ${redeployTriggerDelaySec}s before triggering publisher redeploy ` +
      `(rampWait=${rampWaitSec} + ${Math.floor(durationSec * redeployFraction)}s into test)`,
  );
  await new Promise((r) => setTimeout(r, redeployTriggerDelaySec * 1000));

  console.log(`\n[driver] redeploying ${publisherService}`);
  const t0 = Date.now();
  const result = spawnSync(
    "railway",
    ["redeploy", "-s", publisherService, "--yes"],
    { stdio: "inherit", encoding: "utf-8" },
  );
  const elapsedMs = Date.now() - t0;
  if (result.status !== 0) {
    console.error(
      `[driver] railway redeploy failed (exit ${result.status}, ${elapsedMs}ms)`,
    );
  } else {
    console.log(
      `[driver] redeploy command returned in ${elapsedMs}ms (publisher will be down for the actual restart window)`,
    );
  }
};

(async () => {
  const [result] = await Promise.all([benchPromise, trigger()]);
  console.log(`\n=== Result ===`);
  console.log(JSON.stringify(result, null, 2));
  writeFileSync(outFile, JSON.stringify(result, null, 2));
  console.log(`\nSaved: ${outFile}`);
})().catch((err) => {
  console.error("FAILED:", err);
  process.exit(1);
});
