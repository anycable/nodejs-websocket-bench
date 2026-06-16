// Local jitter benchmark — Socket.io (default, no CSR).
// Thin wrapper around the shared runner; see src/lib/jitter-runners.ts.
//
// Usage:
//   SOCKETIO_URL=http://localhost:3000 NUM_CLIENTS=50 DURATION=150 \
//     tsx src/bench/jitter-socketio.ts
//
// The runner publishes per-message HTTP POSTs to socketio-server's
// /_broadcast endpoint by default — symmetric with how AnyCable is
// published, so the comparison isn't biased by one setup having a
// fewer-requests publisher path. Pass `publishViaServer: true` to
// trigger an in-process io.to().emit() loop on socketio-server instead
// (diagnostic only — isolates in-process fan-out cost).

import { paramsFromEnv } from "../lib/core/params.js";
import { runJitterSocketio } from "../lib/jitter-runners.js";
import { formatHumanReport } from "../lib/core/stats.js";

const params = paramsFromEnv();

const result = await runJitterSocketio(params, {
  serverUrl: process.env.SOCKETIO_URL || "http://localhost:3000",
});

console.log(formatHumanReport("Socket.io Jitter (default, no CSR)", result));
process.exit(result.lostDeliveries > 0 ? 1 : 0);
