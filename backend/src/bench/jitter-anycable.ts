// Delivery under jitter — AnyCable version
//
// Uses @anycable/core with ws — the real AnyCable client with built-in
// session recovery, stream position tracking, and automatic catch-up.
//
// Each client randomly drops its WebSocket connection for ~1s every ~15s.
// The client library handles reconnection and requests missed messages.
// After the test, count which sequence numbers each client received.
//
// Usage:
//   ANYCABLE_URL=ws://localhost:8080/cable NUM_CLIENTS=100 tsx src/bench/jitter-anycable.ts

import WebSocket from "ws";
import { createCable, Channel } from "@anycable/core";

const url = process.env.ANYCABLE_URL || "ws://localhost:8080/cable";
const numClients = parseInt(process.env.NUM_CLIENTS || "50");
const stream = process.env.STREAM || "benchmark";
const testDurationSec = parseInt(process.env.DURATION || "150"); // 2.5 min
const jitterIntervalSec = parseInt(process.env.JITTER_INTERVAL || "15");
const jitterDurationMs = parseInt(process.env.JITTER_DURATION || "1000");

// Suppress unhandled rejections from @anycable/core during jitter
process.on("unhandledRejection", () => {});

// Ramp-up: connect clients gradually to avoid triggering rate limits.
// At 50 connections/sec, 10K clients take ~200s to fully connect.
const rampRate = parseInt(process.env.RAMP_RATE || "50"); // new connections per second

console.log(`AnyCable jitter test: ${numClients} clients, stream=${stream}, url=${url}`);
console.log(`Ramp-up: ${rampRate} connections/sec (~${Math.ceil(numClients / rampRate)}s)`);
console.log(`Jitter: disconnect ~${jitterDurationMs}ms every ~${jitterIntervalSec}s`);

interface ClientResult {
  id: number;
  received: Set<number>;
  highestSeq: number;
  jitterCount: number;
  latencies: number[];
}

async function runClient(id: number): Promise<ClientResult> {
  const result: ClientResult = { id, received: new Set(), highestSeq: 0, jitterCount: 0, latencies: [] };

  const cable = createCable(url, {
    websocketImplementation: WebSocket as any,
    protocol: "actioncable-v1-ext-json",
    logLevel: "error" as any,
  });

  // Handle connection errors gracefully — expected during jitter
  cable.on("close", () => {});
  cable.on("disconnect", () => {});

  // Subscribe to the benchmark stream using pub/sub (signed streams)
  const channel = cable.streamFrom(stream);

  channel.on("message", (msg: any) => {
    if (msg?.seq === undefined) return;
    result.received.add(msg.seq);
    if (msg.seq > result.highestSeq) result.highestSeq = msg.seq;
    if (typeof msg.sentAt === "number") result.latencies.push(Date.now() - msg.sentAt);
  });

  const endAt = Date.now() + testDurationSec * 1000;
  let nextJitter = Date.now() + (5 + Math.random() * jitterIntervalSec) * 1000;

  while (Date.now() < endAt) {
    // Simulate connectivity jitter: disconnect then reconnect.
    // The extended protocol tracks stream positions (epoch + offset)
    // and requests missed messages on reconnection.
    if (Date.now() >= nextJitter) {
      result.jitterCount++;

      cable.disconnect();
      await new Promise((r) => setTimeout(r, jitterDurationMs));
      cable.connect();

      // Wait for reconnection and history recovery.
      // Add random spread so reconnections don't all hit at once.
      await new Promise((r) => setTimeout(r, 2000 + Math.random() * 1000));

      // Spread jitter events across clients to avoid burst reconnections
      nextJitter = Date.now() + (jitterIntervalSec + Math.random() * 10) * 1000;
    }

    await new Promise((r) => setTimeout(r, 500));
  }

  cable.disconnect();
  return result;
}

// Staggered ramp-up: connect clients gradually
const startTime = Date.now();
const clientPromises: Promise<ClientResult>[] = [];

let peakRssMb = 0;
const memTicker = setInterval(() => {
  const rss = process.memoryUsage().rss / 1024 / 1024;
  if (rss > peakRssMb) peakRssMb = rss;
}, 5000);

for (let i = 0; i < numClients; i++) {
  clientPromises.push(runClient(i));
  // Throttle: connect `rampRate` clients per second
  if ((i + 1) % rampRate === 0) {
    await new Promise((r) => setTimeout(r, 1000));
    if ((i + 1) % 1000 === 0) console.log(`Connected ${i + 1}/${numClients} clients...`);
  }
}

const results = await Promise.all(clientPromises);
clearInterval(memTicker);
const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

// Report
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

console.log(`\n=== AnyCable Jitter Results (${elapsed}s) ===`);
console.log(`Clients:          ${numClients}`);
console.log(`Messages sent:    ${maxSeq}`);
console.log(`Total jitters:    ${totalJitters} (avg ${(totalJitters / numClients).toFixed(1)} per client)`);
console.log(`Messages received: ${totalReceived}`);
console.log(`Messages lost:    ${totalLost}`);
console.log(`Delivery rate:    ${avgDeliveryRate}%`);
console.log(`Latency raw (ms): avg=${lavg}  p50=${lp(50)}  p95=${lp(95)}  p99=${lp(99)}  max=${lp(100)}  (n=${latencies.length})`);
console.log(`Latency over min: avg=${navg}  p50=${np(50)}  p95=${np(95)}  p99=${np(99)}  max=${np(100)}  (skew floor=${lmin}ms)`);
console.log(`Client peak RSS:  ${peakRssMb.toFixed(0)} MB`);

if (totalLost > 0) {
  // Show first few clients with losses
  const lossy = results.filter((r) => r.highestSeq - r.received.size > 0).slice(0, 5);
  for (const r of lossy) {
    const missing = [];
    for (let i = 1; i <= r.highestSeq; i++) {
      if (!r.received.has(i)) missing.push(i);
    }
    console.log(`  Client ${r.id}: missing sequences ${missing.slice(0, 10).join(", ")}${missing.length > 10 ? "..." : ""}`);
  }
}

process.exit(totalLost > 0 ? 1 : 0);
