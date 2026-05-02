// Local jitter benchmark — Socket.io with Connection State Recovery enabled.
// Requires the server to be started with SOCKETIO_CSR=1.
// Thin wrapper around the shared runner; see src/lib/jitter-runners.ts.
//
// Usage:
//   # Terminal 1
//   SOCKETIO_CSR=1 npm run dev:socketio-csr
//   # Terminal 2
//   SOCKETIO_URL=http://localhost:3000 NUM_CLIENTS=50 DURATION=150 \
//     tsx src/bench/jitter-socketio-csr.ts

import { paramsFromEnv } from "../lib/params.js";
import { runJitterSocketioCsr } from "../lib/jitter-runners.js";
import { formatHumanReport } from "../lib/stats.js";

const params = paramsFromEnv();

const result = await runJitterSocketioCsr(params, {
  serverUrl: process.env.SOCKETIO_URL || "http://localhost:3000",
});

console.log(formatHumanReport("Socket.io Jitter (CSR enabled)", result));
process.exit(result.lostDeliveries > 0 ? 1 : 0);
