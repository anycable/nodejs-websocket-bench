// Delivery under jitter — Socket.io with Connection State Recovery enabled.
//
// Requires the server to be started with SOCKETIO_CSR=1.
//
// Difference from jitter-socketio.ts:
//   - reconnection: true — the socket.io-client's built-in reconnect loop
//     handles pid + offset automatically when CSR is enabled on the server
//   - we force-close the underlying TCP socket to simulate network jitter;
//     the client reconnects and the server replays buffered packets
//   - we count how often socket.recovered === true (a real CSR resume) vs.
//     falling back to a fresh connect
//
// Jitter cadence (1s offline every ~15s) is identical to the other variants.
//
// Usage:
//   SOCKETIO_URL=http://localhost:3000 NUM_CLIENTS=100 \
//     tsx src/bench/jitter-socketio-csr.ts

import { io, Socket } from "socket.io-client";

const url = process.env.SOCKETIO_URL || "http://localhost:3000";
const numClients = parseInt(process.env.NUM_CLIENTS || "50");
const stream = process.env.STREAM || "benchmark";
const testDurationSec = parseInt(process.env.DURATION || "150");
const jitterIntervalSec = parseInt(process.env.JITTER_INTERVAL || "15");
const jitterDurationMs = parseInt(process.env.JITTER_DURATION || "1000");
const rampRate = parseInt(process.env.RAMP_RATE || "50");

console.log(`Socket.io (CSR enabled) jitter test: ${numClients} clients, stream=${stream}, url=${url}`);
console.log(`Ramp-up: ${rampRate} connections/sec (~${Math.ceil(numClients / rampRate)}s)`);
console.log(`Jitter: force-close TCP ~${jitterDurationMs}ms every ~${jitterIntervalSec}s`);
console.log(`NOTE: server must be started with SOCKETIO_CSR=1\n`);

interface ClientResult {
  id: number;
  received: Set<number>;
  highestSeq: number;
  jitterCount: number;
  recoveredCount: number;
  failedConnects: number;
  latencies: number[]; // receivedAt - sentAt per message, in ms
}

async function runClient(id: number): Promise<ClientResult> {
  const result: ClientResult = {
    id,
    received: new Set(),
    highestSeq: 0,
    jitterCount: 0,
    recoveredCount: 0,
    failedConnects: 0,
    latencies: [],
  };

  // reconnection: true — this is the mode CSR is designed for.
  // socket.io-client tracks the last offset internally and presents
  // pid + offset to the server on reconnect via the handshake.
  const socket = io(url, {
    transports: ["websocket"],
    reconnection: true,
    // Spread reconnects so 1000+ clients don't pile on the server simultaneously.
    // Random jitter is added by socket.io-client itself (`randomizationFactor`,
    // default 0.5) — these bounds set the floor / ceiling.
    reconnectionDelay: 2000,
    reconnectionDelayMax: 5000,
    reconnectionAttempts: Infinity,
    timeout: 10000,
  });

  socket.on("connect", () => {
    if ((socket as any).recovered) {
      result.recoveredCount++;
      // Rooms + socket.data restored automatically; no need to re-join.
    } else {
      // Either the very first connect, or the session expired beyond
      // maxDisconnectionDuration. Re-join from scratch.
      socket.emit("join", stream);
    }
  });

  socket.on("connect_error", () => {
    result.failedConnects++;
  });

  socket.on("message", (msg: any) => {
    if (msg?.seq === undefined) return;
    result.received.add(msg.seq);
    if (msg.seq > result.highestSeq) result.highestSeq = msg.seq;
    if (typeof msg.sentAt === "number") result.latencies.push(Date.now() - msg.sentAt);
  });

  // Wait for initial connection
  await new Promise<void>((resolve) => {
    if (socket.connected) resolve();
    else socket.once("connect", () => resolve());
    setTimeout(resolve, 10000);
  });

  const endAt = Date.now() + testDurationSec * 1000;
  let nextJitter = Date.now() + (5 + Math.random() * jitterIntervalSec) * 1000;

  while (Date.now() < endAt) {
    if (Date.now() >= nextJitter) {
      result.jitterCount++;

      // Force-close the underlying TCP socket — no clean close.
      // socket.io-client will notice and reconnect via its built-in loop,
      // which passes pid + offset so CSR can resume.
      const rawSocket = (socket as any).io?.engine?.transport?.ws;
      if (rawSocket?.terminate) rawSocket.terminate();
      else if (rawSocket?.close) rawSocket.close();

      // Hold "offline" for the jitter duration. The reconnect attempt will
      // fire during / after this window.
      await new Promise((r) => setTimeout(r, jitterDurationMs));

      nextJitter = Date.now() + (jitterIntervalSec + Math.random() * 5) * 1000;
    }

    await new Promise((r) => setTimeout(r, 500));
  }

  socket.disconnect();
  return result;
}

const startTime = Date.now();
const clientPromises: Promise<ClientResult>[] = [];

let peakRssMb = 0;
const memTicker = setInterval(() => {
  const rss = process.memoryUsage().rss / 1024 / 1024;
  if (rss > peakRssMb) peakRssMb = rss;
}, 5000);

for (let i = 0; i < numClients; i++) {
  clientPromises.push(runClient(i));
  if ((i + 1) % rampRate === 0) {
    await new Promise((r) => setTimeout(r, 1000));
    if ((i + 1) % 1000 === 0) console.log(`Connected ${i + 1}/${numClients} clients...`);
  }
}

const results = await Promise.all(clientPromises);
clearInterval(memTicker);
const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

let totalReceived = 0;
let totalLost = 0;
let totalJitters = 0;
let totalRecovered = 0;
let totalFailed = 0;
const maxSeq = Math.max(...results.map((r) => r.highestSeq));

for (const r of results) {
  const lost = r.highestSeq - r.received.size;
  totalReceived += r.received.size;
  totalLost += Math.max(0, lost);
  totalJitters += r.jitterCount;
  totalRecovered += r.recoveredCount;
  totalFailed += r.failedConnects;
}

const avgDeliveryRate = maxSeq > 0
  ? ((totalReceived / (maxSeq * numClients)) * 100).toFixed(2)
  : "N/A";

const recoveryRate = totalJitters > 0
  ? ((totalRecovered / totalJitters) * 100).toFixed(1)
  : "N/A";

const latencies: number[] = [];
for (const r of results) latencies.push(...r.latencies);
latencies.sort((a, b) => a - b);
const lp = (pct: number) =>
  latencies.length ? latencies[Math.floor((latencies.length - 1) * (pct / 100))] : 0;
const lavg = latencies.length
  ? Math.round(latencies.reduce((s, n) => s + n, 0) / latencies.length)
  : 0;
const lmin = latencies.length ? latencies[0] : 0;
const norm = latencies.map((v) => v - lmin);
const np = (pct: number) =>
  norm.length ? norm[Math.floor((norm.length - 1) * (pct / 100))] : 0;
const navg = norm.length ? Math.round(norm.reduce((s, n) => s + n, 0) / norm.length) : 0;

console.log(`\n=== Socket.io (CSR) Jitter Results (${elapsed}s) ===`);
console.log(`Clients:            ${numClients}`);
console.log(`Messages sent:      ${maxSeq}`);
console.log(`Total jitters:      ${totalJitters} (avg ${(totalJitters / numClients).toFixed(1)} per client)`);
console.log(`CSR resumes:        ${totalRecovered} / ${totalJitters} (${recoveryRate}%)`);
console.log(`Connect failures:   ${totalFailed}`);
console.log(`Messages received:  ${totalReceived}`);
console.log(`Messages lost:      ${totalLost}`);
console.log(`Delivery rate:      ${avgDeliveryRate}%`);
console.log(`Latency raw (ms):   avg=${lavg}  p50=${lp(50)}  p95=${lp(95)}  p99=${lp(99)}  max=${lp(100)}  (n=${latencies.length})`);
console.log(`Latency over min:   avg=${navg}  p50=${np(50)}  p95=${np(95)}  p99=${np(99)}  max=${np(100)}  (skew floor=${lmin}ms)`);
console.log(`Client peak RSS:    ${peakRssMb.toFixed(0)} MB`);

if (totalLost > 0) {
  const lossy = results.filter((r) => r.highestSeq - r.received.size > 0).slice(0, 5);
  for (const r of lossy) {
    const missing: number[] = [];
    for (let i = 1; i <= r.highestSeq; i++) {
      if (!r.received.has(i)) missing.push(i);
    }
    console.log(`  Client ${r.id}: missing sequences ${missing.slice(0, 10).join(", ")}${missing.length > 10 ? "..." : ""}`);
  }
}

process.exit(totalLost > 0 ? 1 : 0);
