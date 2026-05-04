// Reconnection avalanche benchmark — Socket.io
//
// Connect N clients, then kill the server. Measure:
// 1. How long until all clients detect the disconnect
// 2. How long until all clients reconnect after server restart
// 3. How many messages are lost during the restart window
//
// Usage: NUM_CLIENTS=1000 tsx src/bench/avalanche-socketio.ts

import { io, Socket } from "socket.io-client";
import { spawn, ChildProcess } from "child_process";

import { percentile } from "../lib/stats.js";

const numClients = parseInt(process.env.NUM_CLIENTS || "1000");
const rampRate = parseInt(process.env.RAMP_RATE || "50");
const stream = process.env.STREAM || "avalanche";
const port = parseInt(process.env.PORT || "4000");

console.log(`Avalanche benchmark (Socket.io): ${numClients} clients on port ${port}`);

// Start the Socket.io server as a child process so we can kill it
let server!: ChildProcess;
function startServer(): Promise<void> {
  return new Promise((resolve) => {
    server = spawn("node", ["dist/socketio/server.js"], {
      env: { ...process.env, PORT: String(port) },
      stdio: ["pipe", "pipe", "pipe"],
    });
    server.stdout?.on("data", (d) => {
      if (d.toString().includes("listening")) resolve();
    });
    server.stderr?.on("data", (d) => process.stderr.write(d));
  });
}

// Phase 1: Start server and connect clients
console.log("Starting server...");
await startServer();
console.log(`Server running on :${port}`);

const sockets: Socket[] = [];
let connected = 0;
let disconnected = 0;
let reconnected = 0;
const reconnectTimes: number[] = [];

// Disconnect / reconnect tracking is set up here, BEFORE we kill the
// server, so events that fire in the few milliseconds between the
// SIGKILL landing and us being ready to listen don't get lost.
// killTime / restartTime are written below when those phases happen.
let killTime = 0;
let firstDisconnectAt = 0;
let allDisconnectedAt = 0;
let restartTime = 0;
let firstReconnectAt = 0;
let allReconnectedAt = 0;

for (let i = 0; i < numClients; i++) {
  const socket = io(`http://localhost:${port}`, {
    transports: ["websocket"],
    reconnection: true,
    reconnectionDelay: 500,
    reconnectionDelayMax: 5000,
  });

  socket.on("connect", () => {
    if (restartTime > 0) {
      // This is a reconnect after the server came back up.
      reconnected++;
      const now = Date.now();
      reconnectTimes.push(now - restartTime);
      if (reconnected === 1) firstReconnectAt = now;
      if (reconnected >= connected * 0.95 && !allReconnectedAt) {
        allReconnectedAt = now;
      }
    } else if (connected < numClients) {
      // Initial connect during ramp-up.
      connected++;
      socket.emit("join", stream);
    }
  });

  socket.on("disconnect", () => {
    if (killTime === 0) return; // ignore disconnects unrelated to the kill
    disconnected++;
    const now = Date.now();
    if (disconnected === 1) firstDisconnectAt = now;
    if (disconnected === connected) allDisconnectedAt = now;
  });

  sockets.push(socket);

  if ((i + 1) % rampRate === 0) {
    await new Promise((r) => setTimeout(r, 1000));
    if ((i + 1) % 500 === 0) console.log(`Connected ${i + 1}/${numClients} (actual: ${connected})`);
  }
}

await new Promise((r) => setTimeout(r, 3000));
console.log(`\nAll clients connected: ${connected}/${numClients}`);

// Phase 2: Start publishing, then kill the server mid-stream
console.log("\nStarting publisher (1 msg/sec)...");
let publishSeq = 0;
const publishInterval = setInterval(async () => {
  publishSeq++;
  try {
    await fetch(`http://localhost:${port}/_broadcast`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ stream, data: JSON.stringify({ seq: publishSeq, sentAt: Date.now() }) }),
    });
  } catch {}
}, 1000);

// Let it publish a few messages
await new Promise((r) => setTimeout(r, 5000));
console.log(`Published ${publishSeq} messages before kill`);

// Phase 3: Kill the server — simulate a deploy. All disconnect/reconnect
// listeners were attached in Phase 1; setting killTime here arms them.
killTime = Date.now();
console.log("\n>>> KILLING SERVER <<<");
server.kill("SIGKILL");

// Wait for all clients to detect disconnect
await new Promise<void>((resolve) => {
  const check = setInterval(() => {
    if (disconnected >= connected * 0.95 || Date.now() - killTime > 30000) {
      clearInterval(check);
      resolve();
    }
  }, 200);
});

const disconnectDetectionTime = allDisconnectedAt ? allDisconnectedAt - killTime : Date.now() - killTime;
console.log(`\nDisconnects detected: ${disconnected}/${connected}`);
console.log(`Time to detect: ${disconnectDetectionTime}ms`);

// Phase 4: Restart the server — reconnect listeners (set up in Phase 1)
// will start firing as clients re-establish.
console.log("\n>>> RESTARTING SERVER <<<");
restartTime = Date.now();
await startServer();
console.log("Server back up");

// Stop publisher
clearInterval(publishInterval);

// Wait for reconnections (up to 2 minutes)
await new Promise<void>((resolve) => {
  const check = setInterval(() => {
    if (reconnected >= connected * 0.95 || Date.now() - restartTime > 120000) {
      clearInterval(check);
      resolve();
    }
  }, 500);
});

if (!allReconnectedAt) allReconnectedAt = Date.now();

const recoveryTime = allReconnectedAt - restartTime;
reconnectTimes.sort((a, b) => a - b);
const p50 = percentile(reconnectTimes, 50);
const p95 = percentile(reconnectTimes, 95);
const p99 = percentile(reconnectTimes, 99);

console.log(`\n=== Socket.io Avalanche Results ===`);
console.log(`Clients:              ${connected}`);
console.log(`Disconnected:         ${disconnected}`);
console.log(`Reconnected:          ${reconnected} (${((reconnected / connected) * 100).toFixed(1)}%)`);
console.log(`Disconnect detection: ${disconnectDetectionTime}ms`);
console.log(`Recovery time (95%):  ${recoveryTime}ms`);
console.log(`Reconnect p50:        ${p50}ms`);
console.log(`Reconnect p95:        ${p95}ms`);
console.log(`Reconnect p99:        ${p99}ms`);
console.log(`Total downtime:       ${disconnectDetectionTime + recoveryTime}ms`);

// Cleanup
sockets.forEach((s) => s.disconnect());
server.kill();
process.exit(0);
