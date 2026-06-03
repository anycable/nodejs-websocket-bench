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
import { connect as natsConnect, StringCodec, NatsConnection } from "nats";
import { createCable } from "@anycable/core";
import { io as ioClient, Socket } from "socket.io-client";

import { ClientStat, JitterResult, newStat, recordMsg, summarize } from "./stats.js";

export type PublisherMode = "serial" | "pool" | "fireforget" | "nats";

export interface ThroughputParams {
  n: number;             // subscribers
  totalMessages: number; // broadcasts to fan out to every subscriber
  intervalMs: number;    // ms between broadcasts (target rate = 1000/intervalMs)
  rampPerSec: number;
  stream: string;
  drainSec: number;      // extra wait after publish completes to catch the tail
  // AnyCable-only: how to drive the broadcast loop.
  //   serial     — await each HTTP /_broadcast call (call-rate-bound baseline)
  //   pool       — keep publisherConcurrency HTTP calls in flight
  //   fireforget — dispatch HTTP calls without awaiting
  //   nats       — publish via NATS (anycable-go subscribes to the channel)
  publisher?: PublisherMode;
  publisherConcurrency?: number; // pool mode only; default 16
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

// External-publisher path returns once the publisher loop has finished
// dispatching; just wait drainSec for stragglers.
async function waitForServerPublishExternal(p: ThroughputParams) {
  await new Promise((r) => setTimeout(r, p.drainSec * 1000));
}

// ---------------------------------------------------------------------------
// AnyCable — publisher is the bench-runner (HTTP POST to /_broadcast),
// matching the production deployment shape.

export interface AnycableUrls {
  cableUrl: string;
  broadcastUrl: string;
  broadcastSecret?: string;
  // Optional NATS broadcaster — used when publisher mode is "nats".
  natsUrl?: string;     // e.g. nats://anycable-go-pro.railway.internal:4242
  natsSubject?: string; // default __anycable__ (matches anycable-go default)
}

async function runAnycablePublisher(
  p: ThroughputParams,
  urls: AnycableUrls
): Promise<void> {
  const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
  const mode = p.publisher ?? "serial";

  if (mode === "nats") {
    if (!urls.natsUrl) throw new Error("nats publisher requires natsUrl");
    const subject = urls.natsSubject || "__anycable__";
    const sc = StringCodec();
    const nc = await natsConnect({ servers: urls.natsUrl });
    try {
      for (let seq = 1; seq <= p.totalMessages; seq++) {
        const payload = JSON.stringify({
          stream: p.stream,
          data: JSON.stringify({ seq, sentAt: Date.now(), text: `m${seq}` }),
        });
        nc.publish(subject, sc.encode(payload));
        if (p.intervalMs > 0) await sleep(p.intervalMs);
      }
      // flush before draining so all publishes are on the wire
      await nc.flush();
    } finally {
      await nc.drain();
    }
    return;
  }

  // HTTP-based modes (serial / pool / fireforget)
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (urls.broadcastSecret) headers["Authorization"] = `Bearer ${urls.broadcastSecret}`;
  const dispatch = (seq: number) =>
    fetch(urls.broadcastUrl, {
      method: "POST",
      headers,
      body: JSON.stringify({
        stream: p.stream,
        data: JSON.stringify({ seq, sentAt: Date.now(), text: `m${seq}` }),
      }),
    }).catch(() => { /* lost broadcasts surface in deliveryRatePct */ });

  if (mode === "serial") {
    for (let seq = 1; seq <= p.totalMessages; seq++) {
      await dispatch(seq);
      if (p.intervalMs > 0) await sleep(p.intervalMs);
    }
    return;
  }

  if (mode === "fireforget") {
    const inflight: Promise<unknown>[] = [];
    for (let seq = 1; seq <= p.totalMessages; seq++) {
      inflight.push(dispatch(seq));
      if (p.intervalMs > 0) await sleep(p.intervalMs);
    }
    await Promise.allSettled(inflight);
    return;
  }

  // pool: bounded concurrency, intervalMs paces dispatches
  const concurrency = Math.max(1, p.publisherConcurrency ?? 16);
  let active = 0;
  const waiters: Array<() => void> = [];
  const acquire = () =>
    new Promise<void>((resolve) => {
      if (active < concurrency) { active++; resolve(); }
      else waiters.push(() => { active++; resolve(); });
    });
  const release = () => {
    active--;
    const next = waiters.shift();
    if (next) next();
  };
  const inflight: Promise<unknown>[] = [];
  for (let seq = 1; seq <= p.totalMessages; seq++) {
    await acquire();
    inflight.push(dispatch(seq).finally(release));
    if (p.intervalMs > 0) await sleep(p.intervalMs);
  }
  await Promise.allSettled(inflight);
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
  await runAnycablePublisher(p, urls);
  const publishingMs = Date.now() - publishStart;
  console.log(`[tp-ac] publisher done in ${publishingMs}ms (mode=${p.publisher ?? "serial"})`);

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
// AnyCable cluster — 2 anycable-go instances behind a shared NATS broadcaster.
// Clients are split 50/50 across the instances; publisher runs in the bench-
// runner and POSTs to the cluster's HTTP /_broadcast endpoint (on instance A,
// since both instances receive every broadcast via NATS regardless of which
// one the publisher targets). This is the production shape for AnyCable
// horizontally scaled — symmetric to the Socket.io + Redis adapter cluster
// test (pool=16 HTTP publisher, half the deliveries on each instance).
export interface AnycableClusterUrls {
  cableUrlA: string;       // ws + cable URL for instance A
  cableUrlB: string;       // ws + cable URL for instance B
  broadcastUrl: string;    // single HTTP /_broadcast target (any instance — NATS fans out)
  broadcastSecret?: string;
  // Optional NATS for `publisher=nats` mode (more idiomatic for a NATS-backed
  // cluster — app publishes to NATS directly, both instances consume).
  natsUrl?: string;
  natsSubject?: string;
}

export async function runThroughputAnycableCluster(
  p: ThroughputParams,
  urls: AnycableClusterUrls
): Promise<ThroughputResult> {
  suppressClientRejections();
  console.log(`[tp-ac-cluster] params=${JSON.stringify(p)}`);
  const startedAt = Date.now();
  const rss = trackPeakRss();

  const stats: ClientStat[] = [];
  const cables: ReturnType<typeof createCable>[] = [];
  const half = Math.floor(p.n / 2);

  for (let i = 0; i < p.n; i++) {
    const stat = newStat();
    stats.push(stat);
    const target = i < half ? urls.cableUrlA : urls.cableUrlB;
    const cable = createCable(target, {
      websocketImplementation: WebSocket as unknown as typeof globalThis.WebSocket,
      protocol: "actioncable-v1-ext-json",
      logLevel: "error" as never,
    });
    cable.on("close", () => {});
    cable.on("disconnect", () => {});
    const channel = cable.streamFrom(p.stream);
    channel.on("message", (msg: unknown) => recordMsg(stat, msg));
    cables.push(cable);
    await maybePauseForRamp(p, i, "tp-ac-cluster");
  }

  await settleAfterRamp();
  console.log(`[tp-ac-cluster] all ramped (A=${half}, B=${p.n - half}); starting publisher`);

  const publishStart = Date.now();
  // Reuse runAnycablePublisher — supports HTTP serial/pool/fireforget and NATS modes.
  await runAnycablePublisher(p, {
    cableUrl: urls.cableUrlA,
    broadcastUrl: urls.broadcastUrl,
    broadcastSecret: urls.broadcastSecret,
    natsUrl: urls.natsUrl,
    natsSubject: urls.natsSubject,
  });
  const publishingMs = Date.now() - publishStart;
  console.log(`[tp-ac-cluster] publisher done in ${publishingMs}ms (mode=${p.publisher ?? "serial"})`);

  // Drain
  await new Promise((r) => setTimeout(r, p.drainSec * 1000));

  for (const c of cables) {
    try { c.disconnect(); } catch { /* */ }
  }

  const peakRssMb = rss.stop();
  const base = summarize({
    label: "anycable-cluster",
    totalMessages: p.totalMessages,
    stats,
    elapsedMs: Date.now() - startedAt,
    peakRssMb,
  });
  const result = augment(base, p, publishingMs);
  console.log(`[tp-ac-cluster] result: ${JSON.stringify(result)}`);
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

// HTTP publisher loop targeting Socket.io's /_broadcast endpoint on
// instance A — same shape as runAnycablePublisher's HTTP modes, so the
// publisher CPU work is symmetric across the cluster-comparison rows.
async function runSocketioRedisHttpPublisher(
  p: ThroughputParams,
  baseUrl: string,
  mode: PublisherMode | string
): Promise<void> {
  const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
  const dispatch = (seq: number) =>
    fetch(`${baseUrl}/_broadcast`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        stream: p.stream,
        data: JSON.stringify({ seq, sentAt: Date.now(), text: `m${seq}` }),
      }),
    }).catch(() => { /* lost broadcasts surface in deliveryRatePct */ });

  if (mode === "serial") {
    for (let seq = 1; seq <= p.totalMessages; seq++) {
      await dispatch(seq);
      if (p.intervalMs > 0) await sleep(p.intervalMs);
    }
    return;
  }
  if (mode === "fireforget") {
    const inflight: Promise<unknown>[] = [];
    for (let seq = 1; seq <= p.totalMessages; seq++) {
      inflight.push(dispatch(seq));
      if (p.intervalMs > 0) await sleep(p.intervalMs);
    }
    await Promise.allSettled(inflight);
    return;
  }
  // pool (default): bounded concurrency
  const concurrency = Math.max(1, p.publisherConcurrency ?? 16);
  let active = 0;
  const waiters: Array<() => void> = [];
  const acquire = () =>
    new Promise<void>((resolve) => {
      if (active < concurrency) { active++; resolve(); }
      else waiters.push(() => { active++; resolve(); });
    });
  const release = () => {
    active--;
    const next = waiters.shift();
    if (next) next();
  };
  const inflight: Promise<unknown>[] = [];
  for (let seq = 1; seq <= p.totalMessages; seq++) {
    await acquire();
    inflight.push(dispatch(seq).finally(release));
    if (p.intervalMs > 0) await sleep(p.intervalMs);
  }
  await Promise.allSettled(inflight);
}

// Socket.io + Redis adapter — two instances behind a shared Redis. Clients
// are split 50/50 across the instances; publisher runs in-process on
// instance A via /publish-local. Half the deliveries fan out locally on A,
// half cross Redis pub/sub to instance B and fan out there. This shape
// matches production multi-node Socket.io deployment, and answers the
// question "what does going horizontal cost on Socket.io?".
export interface SocketioRedisUrls {
  subscriberUrlA: string; // ws + http base for instance A
  subscriberUrlB: string; // ws + http base for instance B
  // publisher is always instance A
}

export async function runThroughputSocketioRedis(
  p: ThroughputParams,
  urls: SocketioRedisUrls
): Promise<ThroughputResult> {
  suppressClientRejections();
  console.log(`[tp-sio-redis] params=${JSON.stringify(p)} urls=${JSON.stringify(urls)}`);
  const startedAt = Date.now();
  const rss = trackPeakRss();

  const stats: ClientStat[] = [];
  const sockets: Socket[] = [];
  const half = Math.floor(p.n / 2);

  for (let i = 0; i < p.n; i++) {
    const stat = newStat();
    stats.push(stat);
    const target = i < half ? urls.subscriberUrlA : urls.subscriberUrlB;
    const socket = ioClient(target, {
      transports: ["websocket"],
      timeout: 10000,
      reconnection: false,
    });
    socket.on("connect", () => socket.emit("join", p.stream));
    socket.on("connect_error", () => stat.failedConnects++);
    socket.on("message", (msg: unknown) => recordMsg(stat, msg));
    sockets.push(socket);
    await maybePauseForRamp(p, i, "tp-sio-redis");
  }

  await settleAfterRamp();
  const mode = p.publisher ?? "serial";
  console.log(`[tp-sio-redis] all ramped (A=${half}, B=${p.n - half}); starting publisher (mode=${mode}) targeting A`);

  // Two publisher shapes — selected by `?publisher=`:
  //   default ("serial" / "inproc"): bench-runner POSTs /publish-local once
  //     on instance A and A loops io.to().emit() in-process. Publisher CPU
  //     shares A's event loop.
  //   pool / fireforget: bench-runner drives an HTTP loop against A's
  //     /_broadcast. Each emit publishes to Redis and is also delivered
  //     locally on A. This mirrors the production shape where the app and
  //     the WS layer are separate processes — the same shape AnyCable's
  //     HTTP pool=16 row uses, so the comparison becomes apples to apples.
  const publishStart = Date.now();
  let publishingMs: number;
  const httpModes = new Set(["pool", "fireforget"]);
  if (httpModes.has(mode)) {
    await runSocketioRedisHttpPublisher(p, urls.subscriberUrlA, mode);
    publishingMs = Date.now() - publishStart;
    await new Promise((r) => setTimeout(r, p.drainSec * 1000));
  } else {
    const qs = new URLSearchParams({
      total: String(p.totalMessages),
      interval: String(p.intervalMs),
      stream: p.stream,
    });
    try {
      await fetch(`${urls.subscriberUrlA}/publish-local?${qs.toString()}`, { method: "POST" });
    } catch {
      /* publish kickoff failure surfaces as zero deliveries */
    }
    await waitForServerPublish(p);
    publishingMs = Date.now() - publishStart - p.drainSec * 1000;
  }

  for (const s of sockets) {
    try { s.disconnect(); } catch { /* */ }
  }

  const peakRssMb = rss.stop();
  const base = summarize({
    label: "socketio-redis",
    totalMessages: p.totalMessages,
    stats,
    elapsedMs: Date.now() - startedAt,
    peakRssMb,
  });
  const result = augment(base, p, publishingMs);
  console.log(`[tp-sio-redis] result: ${JSON.stringify(result)}`);
  return result;
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
  const useHttpPublisher =
    p.publisher === "pool" ||
    p.publisher === "fireforget" ||
    p.publisher === "serial";

  if (useHttpPublisher) {
    // External HTTP publisher: bench-runner POSTs to /_broadcast.
    // This is the "standalone" shape — publisher is a separate process
    // from the WS server.
    console.log(`[${label}] publisher=${p.publisher} concurrency=${p.publisherConcurrency ?? 16} (external HTTP /_broadcast)`);
    await runSocketioRedisHttpPublisher(p, urls.serverUrl, p.publisher!);
    await waitForServerPublishExternal(p);
  } else {
    // In-process publisher: kickoff /publish-local on the WS server,
    // which runs its own emit loop. Same Node event loop as the WS
    // fan-out.
    console.log(`[${label}] publisher=in-process (/publish-local)`);
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
  }
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
  const useHttpPublisher =
    p.publisher === "pool" ||
    p.publisher === "fireforget" ||
    p.publisher === "serial";

  if (useHttpPublisher) {
    console.log(`[tp-uws] publisher=${p.publisher} concurrency=${p.publisherConcurrency ?? 16} (external HTTP /_broadcast)`);
    await runSocketioRedisHttpPublisher(p, urls.serverHttpUrl, p.publisher!);
    await waitForServerPublishExternal(p);
  } else {
    console.log(`[tp-uws] publisher=in-process (/publish-local)`);
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
  }
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
