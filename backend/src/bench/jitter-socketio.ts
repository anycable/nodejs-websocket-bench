// Local jitter benchmark — Socket.io (default, no CSR).
// Thin wrapper around the shared runner; see src/lib/jitter-runners.ts.
//
// Usage:
//   SOCKETIO_URL=http://localhost:3000 NUM_CLIENTS=50 DURATION=150 \
//     tsx src/bench/jitter-socketio.ts
//
// The runner publishes via socketio-server's /publish-local endpoint,
// matching the realistic in-process io.to().emit() fan-out.

import { paramsFromEnv } from "../lib/params.js";
import { runJitterSocketio } from "../lib/jitter-runners.js";
import { formatHumanReport } from "../lib/stats.js";

const params = paramsFromEnv();

const result = await runJitterSocketio(params, {
  serverUrl: process.env.SOCKETIO_URL || "http://localhost:3000",
});

console.log(formatHumanReport("Socket.io Jitter (default, no CSR)", result));
process.exit(result.lostDeliveries > 0 ? 1 : 0);
