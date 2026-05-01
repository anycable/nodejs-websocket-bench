// Railway-hosted bench runner.
//
// Same source as socketio-server, but a different entry point — set
// SERVICE_ENTRY=bench-runner/server on the Railway service to start it.
//
// All targets default to *.railway.internal so client-side bottlenecks
// (NAT, Node event-loop on a developer machine) don't get in the way of
// measuring server capacity at 10K+ scale.
//
// Endpoints:
//   POST /bench-jitter-anycable
//   POST /bench-jitter-socketio
//   POST /bench-jitter-socketio-csr
//
// Each runs once synchronously and returns the full result JSON. Use a long
// curl --max-time when triggering. Console output is also captured by
// Railway logs.

import express from "express";
import WebSocket from "ws";
import { createCable } from "@anycable/core";
import { io as ioClient, Socket } from "socket.io-client";

const SOCKETIO_URL = process.env.SOCKETIO_URL || "http://socketio-server.railway.internal:3000";
const ANYCABLE_URL = process.env.ANYCABLE_URL || "ws://anycable-go.railway.internal:8080/cable";
const ANYCABLE_BROADCAST_URL =
  process.env.ANYCABLE_BROADCAST_URL || "http://anycable-go.railway.internal:8080/_broadcast";
const ANYCABLE_BROADCAST_SECRET = process.env.ANYCABLE_BROADCAST_SECRET || "";

process.on("unhandledRejection", () => {});

const app = express();
app.use(express.json());

app.get("/health", (_req, res) =>
  res.json({
    status: "ok",
    mode: "bench-runner",
    socketioUrl: SOCKETIO_URL,
    anycableUrl: ANYCABLE_URL,
  })
);

// ---------------------------------------------------------------------------
// Helpers — shared across variants

interface JitterParams {
  n: number;
  durationSec: number;
  jitterIntervalSec: number;
  jitterDurationMs: number;
  totalMessages: number;
  intervalMs: number;
  rampPerSec: number;
  stream: string;
}

interface ClientStat {
  received: Set<number>;
  highestSeq: number;
  jitterCount: number;
  recoveredCount: number;
  failedConnects: number;
  latencies: number[];
}

function newStat(): ClientStat {
  return {
    received: new Set(),
    highestSeq: 0,
    jitterCount: 0,
    recoveredCount: 0,
    failedConnects: 0,
    latencies: [],
  };
}

function recordMsg(stat: ClientStat, msg: any) {
  if (msg?.seq === undefined) return;
  stat.received.add(msg.seq);
  if (msg.seq > stat.highestSeq) stat.highestSeq = msg.seq;
  if (typeof msg.sentAt === "number") stat.latencies.push(Date.now() - msg.sentAt);
}

function readParams(req: express.Request): JitterParams {
  return {
    n: parseInt((req.query.n as string) || "1000"),
    durationSec: parseInt((req.query.duration as string) || "160"),
    jitterIntervalSec: parseInt((req.query.jitter as string) || "15"),
    jitterDurationMs: parseInt((req.query.jitterMs as string) || "1000"),
    totalMessages: parseInt((req.query.msgs as string) || "120"),
    intervalMs: parseInt((req.query.interval as string) || "500"),
    rampPerSec: parseInt((req.query.ramp as string) || "200"),
    stream: (req.query.stream as string) || `bench-${Date.now()}`,
  };
}

function summarize(label: string, p: JitterParams, stats: ClientStat[], elapsedMs: number, extras: Record<string, unknown> = {}) {
  let received = 0;
  let lost = 0;
  let jitters = 0;
  let recovered = 0;
  let failedConnects = 0;
  const allLatencies: number[] = [];
  let maxSeq = 0;
  for (const s of stats) {
    received += s.received.size;
    lost += Math.max(0, s.highestSeq - s.received.size);
    jitters += s.jitterCount;
    recovered += s.recoveredCount;
    failedConnects += s.failedConnects;
    if (s.highestSeq > maxSeq) maxSeq = s.highestSeq;
    for (const v of s.latencies) allLatencies.push(v);
  }
  const expected = p.totalMessages * stats.length;
  const deliveryRate = expected > 0 ? (received / expected) * 100 : 0;

  allLatencies.sort((a, b) => a - b);
  const lp = (pct: number) =>
    allLatencies.length ? allLatencies[Math.floor((allLatencies.length - 1) * (pct / 100))] : 0;
  const lavg = allLatencies.length
    ? Math.round(allLatencies.reduce((s, n) => s + n, 0) / allLatencies.length)
    : 0;
  const lmin = allLatencies.length ? allLatencies[0] : 0;
  const norm = allLatencies.map((v) => v - lmin);
  const np = (pct: number) =>
    norm.length ? norm[Math.floor((norm.length - 1) * (pct / 100))] : 0;
  const navg = norm.length ? Math.round(norm.reduce((s, n) => s + n, 0) / norm.length) : 0;

  const result = {
    label,
    elapsedMs,
    clients: stats.length,
    publishedMessages: maxSeq,
    expectedDeliveries: expected,
    receivedDeliveries: received,
    lostDeliveries: lost,
    deliveryRatePct: Number(deliveryRate.toFixed(2)),
    jitterEvents: jitters,
    avgJittersPerClient: Number((jitters / Math.max(1, stats.length)).toFixed(1)),
    csrResumes: recovered,
    csrResumeRatePct:
      jitters > 0 ? Number(((recovered / jitters) * 100).toFixed(1)) : null,
    connectFailures: failedConnects,
    latencyRawMs: { avg: lavg, p50: lp(50), p95: lp(95), p99: lp(99), max: lp(100) },
    latencyOverMinMs: { avg: navg, p50: np(50), p95: np(95), p99: np(99), max: np(100), skewFloor: lmin },
    latencySamples: allLatencies.length,
    runnerPeakRssMb: Math.round(process.memoryUsage().rss / 1024 / 1024),
    ...extras,
  };
  return result;
}

// In-process publisher. Pushes numbered messages over HTTP to the broadcast
// endpoint. For Socket.io this is the same socketio-server's /_broadcast;
// for AnyCable it's anycable-go's /_broadcast with bearer auth.
async function publish(opts: { url: string; secret?: string; total: number; intervalMs: number; stream: string }) {
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (opts.secret) headers["Authorization"] = `Bearer ${opts.secret}`;
  for (let seq = 1; seq <= opts.total; seq++) {
    const data = JSON.stringify({ seq, sentAt: Date.now(), text: `m${seq}` });
    try {
      await fetch(opts.url, { method: "POST", headers, body: JSON.stringify({ stream: opts.stream, data }) });
    } catch {}
    await new Promise((r) => setTimeout(r, opts.intervalMs));
  }
}

// Sleep until publishing should kick off — clients have ramped up.
async function waitForRamp(p: JitterParams) {
  const rampSec = Math.ceil(p.n / p.rampPerSec) + 5;
  await new Promise((r) => setTimeout(r, rampSec * 1000));
}

// ---------------------------------------------------------------------------
// AnyCable jitter

app.post("/bench-jitter-anycable", async (req, res) => {
  const p = readParams(req);
  console.log(`[jitter-ac] params=${JSON.stringify(p)}`);
  const startedAt = Date.now();

  const stats: ClientStat[] = [];
  const cables: any[] = [];

  for (let i = 0; i < p.n; i++) {
    const stat = newStat();
    stats.push(stat);

    const cable = createCable(ANYCABLE_URL, {
      websocketImplementation: WebSocket as any,
      protocol: "actioncable-v1-ext-json",
      logLevel: "error" as any,
    });
    cable.on("close", () => {});
    cable.on("disconnect", () => {});
    const channel = cable.streamFrom(p.stream);
    channel.on("message", (msg: any) => recordMsg(stat, msg));
    cables.push(cable);

    if ((i + 1) % p.rampPerSec === 0) {
      await new Promise((r) => setTimeout(r, 1000));
      if ((i + 1) % 1000 === 0) console.log(`[jitter-ac] ramped ${i + 1}/${p.n}`);
    }
  }

  await new Promise((r) => setTimeout(r, 5000));
  console.log(`[jitter-ac] all ramped; starting publisher and jitter loop`);

  // Run publisher and jitter loop concurrently. The endpoint returns once
  // both complete or the duration elapses.
  const publishTask = publish({
    url: ANYCABLE_BROADCAST_URL,
    secret: ANYCABLE_BROADCAST_SECRET,
    total: p.totalMessages,
    intervalMs: p.intervalMs,
    stream: p.stream,
  });

  const endAt = Date.now() + p.durationSec * 1000;
  const jitterTasks = cables.map((cable, i) =>
    (async () => {
      const stat = stats[i];
      let next = Date.now() + (5 + Math.random() * p.jitterIntervalSec) * 1000;
      while (Date.now() < endAt) {
        if (Date.now() >= next) {
          stat.jitterCount++;
          cable.disconnect();
          await new Promise((r) => setTimeout(r, p.jitterDurationMs));
          cable.connect();
          await new Promise((r) => setTimeout(r, 2000 + Math.random() * 1000));
          next = Date.now() + (p.jitterIntervalSec + Math.random() * 10) * 1000;
        }
        await new Promise((r) => setTimeout(r, 500));
      }
    })()
  );

  await Promise.all([publishTask, ...jitterTasks]);

  // Tear down
  for (const c of cables) {
    try { c.disconnect(); } catch {}
  }

  const result = summarize("anycable", p, stats, Date.now() - startedAt);
  console.log(`[jitter-ac] result: ${JSON.stringify(result)}`);
  res.json(result);
});

// ---------------------------------------------------------------------------
// Socket.io default (reconnection: false, manual fresh socket on jitter)

app.post("/bench-jitter-socketio", async (req, res) => {
  const p = readParams(req);
  console.log(`[jitter-sio] params=${JSON.stringify(p)}`);
  const startedAt = Date.now();

  const stats: ClientStat[] = [];
  const sockets: { current: Socket; stat: ClientStat }[] = [];

  function bindHandlers(socket: Socket, stat: ClientStat) {
    socket.on("message", (msg: any) => recordMsg(stat, msg));
  }

  for (let i = 0; i < p.n; i++) {
    const stat = newStat();
    stats.push(stat);

    const socket = ioClient(SOCKETIO_URL, {
      transports: ["websocket"],
      reconnection: false,
      timeout: 10000,
    });
    socket.on("connect", () => socket.emit("join", p.stream));
    socket.on("connect_error", () => stat.failedConnects++);
    bindHandlers(socket, stat);
    sockets.push({ current: socket, stat });

    if ((i + 1) % p.rampPerSec === 0) {
      await new Promise((r) => setTimeout(r, 1000));
      if ((i + 1) % 1000 === 0) console.log(`[jitter-sio] ramped ${i + 1}/${p.n}`);
    }
  }

  await new Promise((r) => setTimeout(r, 5000));
  console.log(`[jitter-sio] all ramped; starting publisher and jitter loop`);

  // For Socket.io, the realistic fan-out is io.to().emit() — we trigger via
  // socketio-server's /publish-local endpoint to keep publish in-process on
  // the Socket.io server. Note: socketio-server is the SAME Railway service
  // we connect against; the publish triggers there, not on the bench-runner.
  const publishUrl = SOCKETIO_URL + "/publish-local";
  const publishTask = (async () => {
    const qs = new URLSearchParams({
      total: String(p.totalMessages),
      interval: String(p.intervalMs),
      stream: p.stream,
    });
    try {
      await fetch(`${publishUrl}?${qs}`, { method: "POST" });
    } catch {}
    // The /publish-local endpoint runs publishing async on the server side;
    // we sleep for the publishing window so this task tracks the same time.
    await new Promise((r) => setTimeout(r, p.totalMessages * p.intervalMs));
  })();

  const endAt = Date.now() + p.durationSec * 1000;
  const jitterTasks = sockets.map((entry, i) =>
    (async () => {
      let next = Date.now() + (5 + Math.random() * p.jitterIntervalSec) * 1000;
      while (Date.now() < endAt) {
        if (Date.now() >= next) {
          entry.stat.jitterCount++;
          const raw = (entry.current as any).io?.engine?.transport?.ws;
          if (raw?.terminate) raw.terminate();
          else entry.current.disconnect();
          await new Promise((r) => setTimeout(r, p.jitterDurationMs));

          const fresh = ioClient(SOCKETIO_URL, {
            transports: ["websocket"],
            reconnection: false,
            timeout: 5000,
          });
          try {
            await new Promise<void>((resolve, reject) => {
              fresh.once("connect", () => resolve());
              fresh.once("connect_error", reject);
              setTimeout(resolve, 5000);
            });
          } catch {
            entry.stat.failedConnects++;
          }
          if (fresh.connected) {
            fresh.emit("join", p.stream);
            bindHandlers(fresh, entry.stat);
            entry.current = fresh;
          }
          next = Date.now() + (p.jitterIntervalSec + Math.random() * 5) * 1000;
        }
        await new Promise((r) => setTimeout(r, 500));
      }
    })()
  );

  await Promise.all([publishTask, ...jitterTasks]);

  for (const e of sockets) {
    try { e.current.disconnect(); } catch {}
  }

  const result = summarize("socketio-default", p, stats, Date.now() - startedAt);
  console.log(`[jitter-sio] result: ${JSON.stringify(result)}`);
  res.json(result);
});

// ---------------------------------------------------------------------------
// Socket.io + Connection State Recovery (reconnection: true, native CSR flow)

app.post("/bench-jitter-socketio-csr", async (req, res) => {
  const p = readParams(req);
  console.log(`[jitter-csr] params=${JSON.stringify(p)}`);
  const startedAt = Date.now();

  const stats: ClientStat[] = [];
  const sockets: Socket[] = [];

  for (let i = 0; i < p.n; i++) {
    const stat = newStat();
    stats.push(stat);

    const socket = ioClient(SOCKETIO_URL, {
      transports: ["websocket"],
      reconnection: true,
      reconnectionDelay: 2000,
      reconnectionDelayMax: 5000,
      reconnectionAttempts: Infinity,
      timeout: 10000,
    });
    socket.on("connect", () => {
      if ((socket as any).recovered) stat.recoveredCount++;
      else socket.emit("join", p.stream);
    });
    socket.on("connect_error", () => stat.failedConnects++);
    socket.on("message", (msg: any) => recordMsg(stat, msg));
    sockets.push(socket);

    if ((i + 1) % p.rampPerSec === 0) {
      await new Promise((r) => setTimeout(r, 1000));
      if ((i + 1) % 1000 === 0) console.log(`[jitter-csr] ramped ${i + 1}/${p.n}`);
    }
  }

  await new Promise((r) => setTimeout(r, 5000));
  console.log(`[jitter-csr] all ramped; starting publisher and jitter loop`);

  const publishUrl = SOCKETIO_URL + "/publish-local";
  const publishTask = (async () => {
    const qs = new URLSearchParams({
      total: String(p.totalMessages),
      interval: String(p.intervalMs),
      stream: p.stream,
    });
    try {
      await fetch(`${publishUrl}?${qs}`, { method: "POST" });
    } catch {}
    await new Promise((r) => setTimeout(r, p.totalMessages * p.intervalMs));
  })();

  const endAt = Date.now() + p.durationSec * 1000;
  const jitterTasks = sockets.map((socket, i) =>
    (async () => {
      const stat = stats[i];
      let next = Date.now() + (5 + Math.random() * p.jitterIntervalSec) * 1000;
      while (Date.now() < endAt) {
        if (Date.now() >= next) {
          stat.jitterCount++;
          // Force-close the underlying TCP socket; socket.io-client's
          // built-in reconnect machinery handles the resume with pid+offset.
          const raw = (socket as any).io?.engine?.transport?.ws;
          if (raw?.terminate) raw.terminate();
          else if (raw?.close) raw.close();
          await new Promise((r) => setTimeout(r, p.jitterDurationMs));
          next = Date.now() + (p.jitterIntervalSec + Math.random() * 5) * 1000;
        }
        await new Promise((r) => setTimeout(r, 500));
      }
    })()
  );

  await Promise.all([publishTask, ...jitterTasks]);

  for (const s of sockets) {
    try { s.disconnect(); } catch {}
  }

  const result = summarize("socketio-csr", p, stats, Date.now() - startedAt);
  console.log(`[jitter-csr] result: ${JSON.stringify(result)}`);
  res.json(result);
});

// ---------------------------------------------------------------------------

const port = parseInt(process.env.PORT || "3001");
app.listen(port, () => {
  console.log(`bench-runner listening on :${port}`);
  console.log(`  socketio target:  ${SOCKETIO_URL}`);
  console.log(`  anycable target:  ${ANYCABLE_URL}`);
  console.log(`  anycable broadcast: ${ANYCABLE_BROADCAST_URL}`);
});
