// Reconnection avalanche benchmark — Socket.io on Railway
//
// Connect N clients to Railway-hosted Socket.io, then trigger a
// restart via `railway restart`. Measure disconnect detection,
// reconnection time, and messages lost.
//
// Usage:
//   SOCKETIO_URL=https://socketio-server-xxx.up.railway.app \
//   NUM_CLIENTS=1000 tsx src/bench/avalanche-railway-socketio.ts
//
// Then in a SEPARATE terminal, run:
//   railway restart -s socketio-server --yes
//
// The script will detect the restart automatically.

import { io, Socket } from "socket.io-client";

import { percentile } from "../lib/stats.js";

const url = process.env.SOCKETIO_URL;
if (!url) {
  console.error("SOCKETIO_URL is required (e.g. https://your-socketio.up.railway.app)");
  process.exit(1);
}
const numClients = parseInt(process.env.NUM_CLIENTS || "1000");
const rampRate = parseInt(process.env.RAMP_RATE || "50");
const stream = process.env.STREAM || "avalanche";

console.log(`Avalanche benchmark (Socket.io on Railway): ${numClients} clients`);
console.log(`URL: ${url}\n`);

const sockets: Socket[] = [];
let connected = 0;
let initialConnectDone = false;

// State written by listeners attached to every socket below. We track
// these from the moment connections are created so that we don't lose
// any disconnect / reconnect events firing between the user running
// `railway restart` and us attaching listeners afterward — Vladimir's
// note: subscribe to events first, then trigger the disruption.
let disconnected = 0;
let firstDisconnectAt = 0;
let allDisconnectedAt = 0;
let reconnectedCount = 0;
let firstReconnectAt = 0;
let allReconnectedAt = 0;
const reconnectTimes: number[] = [];
let restartDetectedAt = 0;

// Phase 1: Connect all clients with all listeners pre-attached.
for (let i = 0; i < numClients; i++) {
  const socket = io(url, {
    transports: ["websocket"],
    reconnection: true,
    reconnectionDelay: 500,
    reconnectionDelayMax: 5000,
    timeout: 10000,
  });

  socket.on("connect", () => {
    if (!initialConnectDone) {
      connected++;
      socket.emit("join", stream);
      return;
    }
    // After the initial ramp completes, any `connect` we see is a
    // reconnect after the restart.
    if (restartDetectedAt > 0) {
      reconnectedCount++;
      const now = Date.now();
      reconnectTimes.push(now - restartDetectedAt);
      if (reconnectedCount === 1) {
        firstReconnectAt = now;
        console.log(`  First reconnect at ${new Date().toISOString()}`);
      }
      if (reconnectedCount >= connected * 0.95 && !allReconnectedAt) {
        allReconnectedAt = now;
        console.log(`  95% reconnected (${reconnectedCount}/${connected}) in ${now - restartDetectedAt}ms`);
      }
    }
  });

  socket.on("disconnect", () => {
    if (!initialConnectDone) return; // ignore noise from the ramp
    disconnected++;
    const now = Date.now();
    if (disconnected === 1) {
      firstDisconnectAt = now;
      restartDetectedAt = now;
      console.log(`  First disconnect detected at ${new Date().toISOString()}`);
    }
    if (disconnected === connected) {
      allDisconnectedAt = now;
      console.log(`  All ${connected} clients disconnected (${now - firstDisconnectAt}ms spread)`);
    }
  });

  sockets.push(socket);

  if ((i + 1) % rampRate === 0) {
    await new Promise((r) => setTimeout(r, 1000));
    if ((i + 1) % 500 === 0) console.log(`Connected ${i + 1}/${numClients} (actual: ${connected})`);
  }
}

await new Promise((r) => setTimeout(r, 5000));
initialConnectDone = true;
console.log(`\nAll clients connected: ${connected}/${numClients}`);

// Phase 2: prompt the user to trigger the restart. By the time the
// console message is rendered, every socket already has its disconnect
// + reconnect listeners attached, so no events can be missed in the
// gap between the trigger landing and the listeners arming.
console.log(`\n>>> NOW RUN IN ANOTHER TERMINAL: railway restart -s socketio-server --yes <<<`);
console.log(`Waiting for disconnects...\n`);

// Wait up to 3 minutes for the full cycle
const deadline = Date.now() + 180000;
while (Date.now() < deadline) {
  await new Promise((r) => setTimeout(r, 1000));

  if (restartDetectedAt > 0 && (reconnectedCount >= connected * 0.95 || Date.now() - restartDetectedAt > 120000)) {
    break;
  }

  // Print status every 10s while waiting for restart
  if (restartDetectedAt === 0 && (Date.now() % 10000 < 1100)) {
    process.stdout.write(".");
  }
}

if (!allReconnectedAt) allReconnectedAt = Date.now();

// Results
reconnectTimes.sort((a, b) => a - b);
const p = (pct: number) => percentile(reconnectTimes, pct);
const recoveryTime = allReconnectedAt - restartDetectedAt;

console.log(`\n=== Socket.io Railway Avalanche Results ===`);
console.log(`Clients:              ${connected}`);
console.log(`Disconnected:         ${disconnected}`);
console.log(`Reconnected:          ${reconnectedCount} (${((reconnectedCount / connected) * 100).toFixed(1)}%)`);
console.log(`Disconnect spread:    ${allDisconnectedAt ? allDisconnectedAt - firstDisconnectAt : '?'}ms`);
console.log(`Recovery time (95%):  ${recoveryTime}ms`);
console.log(`Reconnect p50:        ${p(50)}ms`);
console.log(`Reconnect p95:        ${p(95)}ms`);
console.log(`Reconnect p99:        ${p(99)}ms`);
console.log(`Total downtime:       ${recoveryTime}ms`);

sockets.forEach((s) => s.disconnect());
process.exit(0);
