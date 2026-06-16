// Local jitter benchmark — AnyCable. Thin wrapper around the shared runner;
// see src/lib/jitter-runners.ts for the actual logic.
//
// Usage:
//   ANYCABLE_URL=ws://localhost:8080/cable \
//   BROADCAST_URL=http://localhost:8090/_broadcast \
//   NUM_CLIENTS=50 DURATION=150 \
//     tsx src/bench/jitter-anycable.ts

import { paramsFromEnv } from "../lib/core/params.js";
import { runJitterAnycable } from "../lib/jitter-runners.js";
import { formatHumanReport } from "../lib/core/stats.js";

const params = paramsFromEnv();

const result = await runJitterAnycable(params, {
  cableUrl: process.env.ANYCABLE_URL || "ws://localhost:8080/cable",
  broadcastUrl: process.env.BROADCAST_URL || "http://localhost:8090/_broadcast",
  broadcastSecret: process.env.BROADCAST_SECRET || undefined,
});

console.log(formatHumanReport("AnyCable Jitter", result));
process.exit(result.lostDeliveries > 0 ? 1 : 0);
