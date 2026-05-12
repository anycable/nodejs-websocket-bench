// Throughput (msg/sec) runners — same shape as jitter-runners minus the
// jitter loop. Connect N subscribers to a single topic, broadcast
// totalMessages at intervalMs, drain, summarize.
//
// We sweep across rates from a CLI driver and look for each setup's
// breaking point: the rate at which delivery drops or the latency tail
// blows out. Per-rate inputs:
//   - intervalMs=1000  → target 1 msg/sec   (mild load — typical chat)
//   - intervalMs=100   → target 10 msg/sec  (moderate broadcast)
//   - intervalMs=10    → target 100 msg/sec (aggressive load)
//   - intervalMs=1     → target 1000 msg/sec (stress)
//
// For N=10,000 subscribers and totalMessages=100, the outbound delivery
// volume is 1M per run. Multiplying by rate gives the per-second outbound
// pressure on the server. Different setups break at different points;
// this is what the test is here to find.
//
// Publisher placement (production-realistic):
//   - Socket.io / +CSR / uWS — publishing runs in-process on the server
//     itself, via the existing /publish-local endpoint. This is how each
//     setup is actually used.
//   - AnyCable — publishing runs from the bench-runner over HTTP to
//     anycable-go's /_broadcast. Same as production: your app posts to
//     anycable-go, which then fans out.

import WebSocket from "ws";
import { createCable } from "@anycable/core";
import { io as ioClient, Socket } from "socket.io-client";

import { ClientStat, JitterResult, newStat, recordMsg, summarize } from "./stats.js";

export interface ThroughputParams {
  n: number;             // subscribers
  totalMessages: number; // broadcasts to fan out to every subscriber
  intervalMs: number;    // ms between broadcasts (target rate = 1000/intervalMs)
  rampPerSec: number;
  stream: string;
  drainSec: number;      // extra wait after publish completes to catch the tail
}

export interface ThroughputResult extends JitterResult {
  intervalMs: number;
  targetRateMsgPerSec: number;
  // Server's outbound deliveries per second across the publishing window
  // (total expected deliveries / publishing wall-clock). When delivery is
  // 100%, outbound == useful.
  outboundDeliveriesPerSec: number;
  usefulDeliveriesPerSec: number;
  publishingMs: number;
}

let suppressed = false;
function suppressClientRejections() {
  if (suppressed) return;
  suppressed = true;
  process.on("unhandledRejection", () => {});
}

async function settleAfterRamp() {
  await new Promise((r) => setTimeout(r, 5000));
}

function trackPeakRss(): { stop: () => number } {
  let peak = process.memoryUsage().rss;
  const handle = setInterval(() => {
    const rss = process.memoryUsage().rss;
    if (rss > peak) peak = rss;
  }, 5000);
  return {
    stop() {
      clearInterval(handle);
      return peak / 1024 / 1024;
    },
  };
}

async function maybePauseForRamp(p: ThroughputParams, i: number, label: string) {
  if ((i + 1) % p.rampPerSec === 0) {
    await new Promise((r) => setTimeout(r, 1000));
    if ((i + 1) % 1000 === 0) console.log(`[${label}] ramped ${i + 1}/${p.n}`);
  }
}

function augment(
  base: JitterResult,
  p: ThroughputParams,
  publishingMs: number
): ThroughputResult {
  const target = p.intervalMs > 0 ? 1000 / p.intervalMs : Infinity;
  const publishingSec = Math.max(0.001, publishingMs / 1000);
  // Expected = totalMessages × subscribers spread across the publishing window.
  const outbound = base.expectedDeliveries / publishingSec;
  const useful = base.receivedDeliveries / publishingSec;
  return {
    ...base,
    intervalMs: p.intervalMs,
    targetRateMsgPerSec: Number(target.toFixed(1)),
    outboundDeliveriesPerSec: Math.round(outbound),
    usefulDeliveriesPerSec: Math.round(useful),
    publishingMs,
  };
}

// Wait for the server-side publish-local burst to finish. Sleep at least
// total × intervalMs (the nominal publishing window), or 1s if interval
// is sub-millisecond, then add the drain so any slow-tail receives land.
async function waitForServerPublish(p: ThroughputParams) {
  const nominalMs = Math.max(p.totalMessages * p.intervalMs, 1000);
  await new Promise((r) => setTimeout(r, nominalMs));
  await new Promise((r) => setTimeout(r, p.drainSec * 1000));
}

// ---------------------------------------------------------------------------
// AnyCable — publisher is the bench-runner (HTTP POST to /_broadcast),
// matching the production deployment shape.

export interface AnycableUrls {
  cableUrl: string;
  broadcastUrl: string;
  broadcastSecret?: string;
}

export async function runThroughputAnycable(
  p: ThroughputParams,
  urls: AnycableUrls
): Promise<ThroughputResult> {
  suppressClientRejections();
  console.log(`[tp-ac] params=${JSON.stringify(p)}`);
  const startedAt = Date.now();
  const rss = trackPeakRss();

  const stats: ClientStat[] = [];
  const cables: ReturnType<typeof createCable>[] = [];

  for (let i = 0; i < p.n; i++) {
    const stat = newStat();
    stats.push(stat);
    const cable = createCable(urls.cableUrl, {
      websocketImplementation: WebSocket as unknown as typeof globalThis.WebSocket,
      protocol: "actioncable-v1-ext-json",
      logLevel: "error" as never,
    });
    cable.on("close", () => {});
    cable.on("disconnect", () => {});
    const channel = cable.streamFrom(p.stream);
    channel.on("message", (msg: unknown) => recordMsg(stat, msg));
    cables.push(cable);
    await maybePauseForRamp(p, i, "tp-ac");
  }

  await settleAfterRamp();
  console.log(`[tp-ac] all ramped; starting publisher`);

  const publishStart = Date.now();
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (urls.broadcastSecret) headers["Authorization"] = `Bearer ${urls.broadcastSecret}`;
  for (let seq = 1; seq <= p.totalMessages; seq++) {
    const data = JSON.stringify({ seq, sentAt: Date.now(), text: `m${seq}` });
    try {
      await fetch(urls.broadcastUrl, {
        method: "POST",
        headers,
        body: JSON.stringify({ stream: p.stream, data }),
      });
    } catch {
      /* lost individual broadcasts show up in deliveryRatePct */
    }
    if (p.intervalMs > 0) {
      await new Promise((r) => setTimeout(r, p.intervalMs));
    }
  }
  const publishingMs = Date.now() - publishStart;

  // Drain — let any in-flight messages land before tearing down.
  await new Promise((r) => setTimeout(r, p.drainSec * 1000));

  for (const c of cables) {
    try { c.disconnect(); } catch { /* tear-down errors are not interesting */ }
  }

  const peakRssMb = rss.stop();
  const base = summarize({
    label: "anycable",
    totalMessages: p.totalMessages,
    stats,
    elapsedMs: Date.now() - startedAt,
    peakRssMb,
  });
  const result = augment(base, p, publishingMs);
  console.log(`[tp-ac] result: ${JSON.stringify(result)}`);
  return result;
}

// ---------------------------------------------------------------------------
// Socket.io variants — both share the same connection pattern and the same
// in-process /publish-local trigger; only socket.io-client options differ.

export interface SocketioUrls {
  serverUrl: string;
}

export async function runThroughputSocketio(
  p: ThroughputParams,
  urls: SocketioUrls
): Promise<ThroughputResult> {
  return runSocketioCommon(p, urls, "tp-sio", "socketio-default", {
    reconnection: false,
  });
}

export async function runThroughputSocketioCsr(
  p: ThroughputParams,
  urls: SocketioUrls
): Promise<ThroughputResult> {
  return runSocketioCommon(p, urls, "tp-csr", "socketio-csr", {
    reconnection: true,
    reconnectionDelay: 2000,
    reconnectionDelayMax: 5000,
    reconnectionAttempts: Infinity,
  });
}

async function runSocketioCommon(
  p: ThroughputParams,
  urls: SocketioUrls,
  label: string,
  resultLabel: string,
  options: Record<string, unknown>
): Promise<ThroughputResult> {
  suppressClientRejections();
  console.log(`[${label}] params=${JSON.stringify(p)}`);
  const startedAt = Date.now();
  const rss = trackPeakRss();

  const stats: ClientStat[] = [];
  const sockets: Socket[] = [];

  for (let i = 0; i < p.n; i++) {
    const stat = newStat();
    stats.push(stat);
    const socket = ioClient(urls.serverUrl, {
      transports: ["websocket"],
      timeout: 10000,
      ...options,
    });
    socket.on("connect", () => socket.emit("join", p.stream));
    socket.on("connect_error", () => stat.failedConnects++);
    socket.on("message", (msg: unknown) => recordMsg(stat, msg));
    sockets.push(socket);
    await maybePauseForRamp(p, i, label);
  }

  await settleAfterRamp();
  console.log(`[${label}] all ramped; starting publisher`);

  const publishStart = Date.now();
  const qs = new URLSearchParams({
    total: String(p.totalMessages),
    interval: String(p.intervalMs),
    stream: p.stream,
  });
  try {
    await fetch(`${urls.serverUrl}/publish-local?${qs.toString()}`, { method: "POST" });
  } catch {
    /* publish kickoff failure surfaces as zero deliveries */
  }
  await waitForServerPublish(p);
  const publishingMs = Date.now() - publishStart - p.drainSec * 1000;

  for (const s of sockets) {
    try { s.disconnect(); } catch { /* */ }
  }

  const peakRssMb = rss.stop();
  const base = summarize({
    label: resultLabel,
    totalMessages: p.totalMessages,
    stats,
    elapsedMs: Date.now() - startedAt,
    peakRssMb,
  });
  const result = augment(base, p, publishingMs);
  console.log(`[${label}] result: ${JSON.stringify(result)}`);
  return result;
}

// ---------------------------------------------------------------------------
// uWebSockets.js — same shape as the jitter-uws ReconnectingWs minus the
// reconnect machinery (throughput test isn't testing recovery).

export interface UwsUrls {
  serverWsUrl: string;
  serverHttpUrl: string;
}

function makeUwsClient(url: string, topic: string, stat: ClientStat): WebSocket {
  const ws = new WebSocket(url);
  ws.on("open", () => {
    ws.send(JSON.stringify({ type: "subscribe", topic }));
  });
  ws.on("message", (data) => {
    let text: string;
    if (typeof data === "string") text = data;
    else if (Buffer.isBuffer(data)) text = data.toString("utf-8");
    else return;
    try {
      recordMsg(stat, JSON.parse(text));
    } catch { /* drop malformed frames */ }
  });
  ws.on("error", () => { stat.failedConnects++; });
  return ws;
}

export async function runThroughputUws(
  p: ThroughputParams,
  urls: UwsUrls
): Promise<ThroughputResult> {
  suppressClientRejections();
  console.log(`[tp-uws] params=${JSON.stringify(p)}`);
  const startedAt = Date.now();
  const rss = trackPeakRss();

  const stats: ClientStat[] = [];
  const sockets: WebSocket[] = [];

  for (let i = 0; i < p.n; i++) {
    const stat = newStat();
    stats.push(stat);
    sockets.push(makeUwsClient(urls.serverWsUrl, p.stream, stat));
    await maybePauseForRamp(p, i, "tp-uws");
  }

  await settleAfterRamp();
  console.log(`[tp-uws] all ramped; starting publisher`);

  const publishStart = Date.now();
  const qs = new URLSearchParams({
    total: String(p.totalMessages),
    interval: String(p.intervalMs),
    stream: p.stream,
  });
  try {
    await fetch(`${urls.serverHttpUrl}/publish-local?${qs.toString()}`, { method: "POST" });
  } catch {
    /* */
  }
  await waitForServerPublish(p);
  const publishingMs = Date.now() - publishStart - p.drainSec * 1000;

  for (const s of sockets) {
    try { s.close(); } catch { /* */ }
  }

  const peakRssMb = rss.stop();
  const base = summarize({
    label: "uws",
    totalMessages: p.totalMessages,
    stats,
    elapsedMs: Date.now() - startedAt,
    peakRssMb,
  });
  const result = augment(base, p, publishingMs);
  console.log(`[tp-uws] result: ${JSON.stringify(result)}`);
  return result;
}
