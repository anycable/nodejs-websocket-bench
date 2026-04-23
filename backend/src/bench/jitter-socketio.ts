// Delivery under jitter — Socket.io version
//
// Uses socket.io-client — the official Socket.io client.
// Socket.io auto-reconnects but has NO message recovery.
// Messages sent during disconnection are permanently lost.
//
// Each client randomly drops its connection for ~1s every ~15s.
// After the test, count which sequence numbers each client received.
//
// Usage:
//   SOCKETIO_URL=http://localhost:3000 NUM_CLIENTS=100 tsx src/bench/jitter-socketio.ts

import { io } from "socket.io-client";

const url = process.env.SOCKETIO_URL || "http://localhost:3000";
const numClients = parseInt(process.env.NUM_CLIENTS || "50");
const stream = process.env.STREAM || "benchmark";
const testDurationSec = parseInt(process.env.DURATION || "150");
const jitterIntervalSec = parseInt(process.env.JITTER_INTERVAL || "15");
const jitterDurationMs = parseInt(process.env.JITTER_DURATION || "1000");

const rampRate = parseInt(process.env.RAMP_RATE || "50"); // new connections per second

console.log(`Socket.io jitter test: ${numClients} clients, stream=${stream}, url=${url}`);
console.log(`Ramp-up: ${rampRate} connections/sec (~${Math.ceil(numClients / rampRate)}s)`);
console.log(`Jitter: disconnect ~${jitterDurationMs}ms every ~${jitterIntervalSec}s`);

interface ClientResult {
  id: number;
  received: Set<number>;
  highestSeq: number;
  jitterCount: number;
}

async function runClient(id: number): Promise<ClientResult> {
  const result: ClientResult = { id, received: new Set(), highestSeq: 0, jitterCount: 0 };

  let socket = io(url, {
    transports: ["websocket"],
    reconnection: false,
    timeout: 10000,
  });

  await new Promise<void>((resolve) => {
    socket.on("connect", resolve);
    setTimeout(resolve, 10000);
  });
  socket.emit("join", stream);

  socket.on("message", (msg: any) => {
    if (msg?.seq !== undefined) {
      result.received.add(msg.seq);
      if (msg.seq > result.highestSeq) result.highestSeq = msg.seq;
    }
  });

  const endAt = Date.now() + testDurationSec * 1000;
  let nextJitter = Date.now() + (5 + Math.random() * jitterIntervalSec) * 1000;

  while (Date.now() < endAt) {
    if (Date.now() >= nextJitter) {
      result.jitterCount++;

      // Forcefully kill the TCP socket — mimics real network failure.
      // No clean close, no goodbye. Just like WiFi dropping.
      const rawSocket = (socket as any).io?.engine?.transport?.ws;
      if (rawSocket?.terminate) {
        rawSocket.terminate();
      } else {
        socket.disconnect();
      }

      await new Promise((r) => setTimeout(r, jitterDurationMs));

      // Reconnect — Socket.io reconnects but has NO catch-up mechanism.
      // Messages sent during the outage are permanently lost.
      socket = io(url, {
        transports: ["websocket"],
        reconnection: false,
        timeout: 5000,
      });
      try {
        await new Promise<void>((resolve, reject) => {
          socket.on("connect", resolve);
          socket.on("connect_error", reject);
          setTimeout(resolve, 5000); // don't block forever
        });
      } catch {}
      if (socket.connected) {
        socket.emit("join", stream);
        socket.on("message", (msg: any) => {
          if (msg?.seq !== undefined) {
            result.received.add(msg.seq);
            if (msg.seq > result.highestSeq) result.highestSeq = msg.seq;
          }
        });
      }

      nextJitter = Date.now() + (jitterIntervalSec + Math.random() * 5) * 1000;
    }

    await new Promise((r) => setTimeout(r, 500));
  }

  socket.disconnect();
  return result;
}

const startTime = Date.now();
const clientPromises: Promise<ClientResult>[] = [];

for (let i = 0; i < numClients; i++) {
  clientPromises.push(runClient(i));
  if ((i + 1) % rampRate === 0) {
    await new Promise((r) => setTimeout(r, 1000));
    if ((i + 1) % 1000 === 0) console.log(`Connected ${i + 1}/${numClients} clients...`);
  }
}

const results = await Promise.all(clientPromises);
const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

let totalReceived = 0;
let totalLost = 0;
let totalJitters = 0;
const maxSeq = Math.max(...results.map((r) => r.highestSeq));

for (const r of results) {
  const expected = r.highestSeq;
  const lost = expected - r.received.size;
  totalReceived += r.received.size;
  totalLost += Math.max(0, lost);
  totalJitters += r.jitterCount;
}

const avgDeliveryRate = maxSeq > 0
  ? ((totalReceived / (maxSeq * numClients)) * 100).toFixed(2)
  : "N/A";

console.log(`\n=== Socket.io Jitter Results (${elapsed}s) ===`);
console.log(`Clients:          ${numClients}`);
console.log(`Messages sent:    ${maxSeq}`);
console.log(`Total jitters:    ${totalJitters} (avg ${(totalJitters / numClients).toFixed(1)} per client)`);
console.log(`Messages received: ${totalReceived}`);
console.log(`Messages lost:    ${totalLost}`);
console.log(`Delivery rate:    ${avgDeliveryRate}%`);

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
