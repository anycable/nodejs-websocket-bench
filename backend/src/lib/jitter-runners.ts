// Pure async runners for the jitter benchmarks. Same code path is used by
// the local CLI scripts (with env-derived params) and the Railway-hosted
// bench-runner HTTP endpoints (with query-derived params).
//
// Each runner:
//   1. Ramps up `params.n` clients at `rampPerSec` connections / second.
//   2. Starts publishing `totalMessages` numbered messages at `intervalMs`.
//   3. Concurrently each client drops its TCP socket for `jitterDurationMs`
//      every ~`jitterIntervalSec`, simulating WiFi handoffs / cellular blips.
//   4. After publishing finishes (or `durationSec` elapses), summarizes.

import WebSocket from "ws";
import { createCable } from "@anycable/core";
import { io as ioClient, Socket } from "socket.io-client";

import { ClientStat, JitterResult, newStat, recordMsg, summarize } from "./stats.js";
import type { JitterParams } from "./params.js";

// Suppress noisy unhandledRejection logs from socket libraries during jitter.
// Done once, regardless of how many runners get invoked.
let suppressed = false;
function suppressClientRejections() {
  if (suppressed) return;
  suppressed = true;
  process.on("unhandledRejection", () => {});
}

// Sleep N seconds for ramp + a small settling buffer before publishing starts.
async function settleAfterRamp(p: JitterParams) {
  await new Promise((r) => setTimeout(r, 5000));
}

// Tracks process RSS during the run; returns the peak observed.
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

// Pace ramp-up at `rampPerSec` new connections per second.
async function maybePauseForRamp(p: JitterParams, i: number, label: string) {
  if ((i + 1) % p.rampPerSec === 0) {
    await new Promise((r) => setTimeout(r, 1000));
    if ((i + 1) % 1000 === 0) {
      console.log(`[${label}] ramped ${i + 1}/${p.n}`);
    }
  }
}

// HTTP publisher loop. Used for AnyCable runs (Socket.io publishes via its
// own /publish-local endpoint instead — see runJitterSocketioVia).
async function publish(opts: {
  url: string;
  secret?: string;
  total: number;
  intervalMs: number;
  stream: string;
}) {
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (opts.secret) headers["Authorization"] = `Bearer ${opts.secret}`;
  for (let seq = 1; seq <= opts.total; seq++) {
    const data = JSON.stringify({ seq, sentAt: Date.now(), text: `m${seq}` });
    try {
      await fetch(opts.url, {
        method: "POST",
        headers,
        body: JSON.stringify({ stream: opts.stream, data }),
      });
    } catch {
      // Skip individual failures — we'll see them via lostDeliveries.
    }
    await new Promise((r) => setTimeout(r, opts.intervalMs));
  }
}

// Force-close the underlying TCP socket of a socket.io-client.
// No clean close — the kind of failure WiFi drops actually produce.
function terminateUnderlyingTcp(socket: Socket) {
  // The internal path is engine.transport.ws (a `ws` WebSocket).
  // Cast is unavoidable: socket.io-client doesn't expose this in its types.
  const raw = (socket as unknown as {
    io?: { engine?: { transport?: { ws?: WebSocket } } };
  }).io?.engine?.transport?.ws;
  if (!raw) return false;
  if (typeof (raw as WebSocket & { terminate?: () => void }).terminate === "function") {
    (raw as WebSocket & { terminate: () => void }).terminate();
    return true;
  }
  if (typeof raw.close === "function") {
    raw.close();
    return true;
  }
  return false;
}

// ---------------------------------------------------------------------------
// AnyCable jitter

export interface AnycableUrls {
  cableUrl: string;
  broadcastUrl: string;
  broadcastSecret?: string;
}

export async function runJitterAnycable(
  p: JitterParams,
  urls: AnycableUrls
): Promise<JitterResult> {
  suppressClientRejections();
  console.log(`[jitter-ac] params=${JSON.stringify(p)}`);
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
      // The @anycable/core types don't include "error" yet; the runtime
      // accepts any of error|warn|info|debug.
      logLevel: "error" as never,
    });
    cable.on("close", () => {});
    cable.on("disconnect", () => {});
    const channel = cable.streamFrom(p.stream);
    channel.on("message", (msg: unknown) => recordMsg(stat, msg));
    cables.push(cable);

    await maybePauseForRamp(p, i, "jitter-ac");
  }

  await settleAfterRamp(p);
  console.log(`[jitter-ac] all ramped; starting publisher and jitter loop`);

  const publishTask = publish({
    url: urls.broadcastUrl,
    secret: urls.broadcastSecret,
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
          // Spread reconnect storms so 1000s of clients don't slam the server.
          await new Promise((r) => setTimeout(r, 2000 + Math.random() * 1000));
          next = Date.now() + (p.jitterIntervalSec + Math.random() * 10) * 1000;
        }
        await new Promise((r) => setTimeout(r, 500));
      }
    })()
  );

  await Promise.all([publishTask, ...jitterTasks]);

  for (const c of cables) {
    try {
      c.disconnect();
    } catch {
      /* tear-down errors are not interesting */
    }
  }

  const peakRssMb = rss.stop();
  const result = summarize({
    label: "anycable",
    totalMessages: p.totalMessages,
    stats,
    elapsedMs: Date.now() - startedAt,
    peakRssMb,
  });
  console.log(`[jitter-ac] result: ${JSON.stringify(result)}`);
  return result;
}

// ---------------------------------------------------------------------------
// Socket.io URL bundle — used by both default and CSR variants.

export interface SocketioUrls {
  serverUrl: string;
  // If set, publishing is delegated to socketio-server's /publish-local
  // endpoint (in-process Socket.io fan-out). Otherwise the runner
  // publishes via /_broadcast over HTTP.
  publishViaServer?: boolean;
}

// ---------------------------------------------------------------------------
// Socket.io default — reconnection: false, manual fresh socket on jitter.

export async function runJitterSocketio(
  p: JitterParams,
  urls: SocketioUrls
): Promise<JitterResult> {
  suppressClientRejections();
  console.log(`[jitter-sio] params=${JSON.stringify(p)}`);
  const startedAt = Date.now();
  const rss = trackPeakRss();

  const stats: ClientStat[] = [];
  const sockets: { current: Socket; stat: ClientStat }[] = [];

  function bindHandlers(socket: Socket, stat: ClientStat) {
    socket.on("message", (msg: unknown) => recordMsg(stat, msg));
  }

  for (let i = 0; i < p.n; i++) {
    const stat = newStat();
    stats.push(stat);

    const socket = ioClient(urls.serverUrl, {
      transports: ["websocket"],
      reconnection: false,
      timeout: 10000,
    });
    socket.on("connect", () => socket.emit("join", p.stream));
    socket.on("connect_error", () => stat.failedConnects++);
    bindHandlers(socket, stat);
    sockets.push({ current: socket, stat });

    await maybePauseForRamp(p, i, "jitter-sio");
  }

  await settleAfterRamp(p);
  console.log(`[jitter-sio] all ramped; starting publisher and jitter loop`);

  const publishTask = startSocketioPublishing(p, urls);

  const endAt = Date.now() + p.durationSec * 1000;
  const jitterTasks = sockets.map((entry) =>
    (async () => {
      let next = Date.now() + (5 + Math.random() * p.jitterIntervalSec) * 1000;
      while (Date.now() < endAt) {
        if (Date.now() >= next) {
          entry.stat.jitterCount++;
          if (!terminateUnderlyingTcp(entry.current)) {
            entry.current.disconnect();
          }
          await new Promise((r) => setTimeout(r, p.jitterDurationMs));

          // Default Socket.io has no resume protocol — open a fresh socket.
          const fresh = ioClient(urls.serverUrl, {
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
    try {
      e.current.disconnect();
    } catch {}
  }

  const peakRssMb = rss.stop();
  const result = summarize({
    label: "socketio-default",
    totalMessages: p.totalMessages,
    stats,
    elapsedMs: Date.now() - startedAt,
    peakRssMb,
  });
  console.log(`[jitter-sio] result: ${JSON.stringify(result)}`);
  return result;
}

// ---------------------------------------------------------------------------
// Socket.io + Connection State Recovery — reconnection: true, native CSR flow.

export async function runJitterSocketioCsr(
  p: JitterParams,
  urls: SocketioUrls
): Promise<JitterResult> {
  suppressClientRejections();
  console.log(`[jitter-csr] params=${JSON.stringify(p)}`);
  const startedAt = Date.now();
  const rss = trackPeakRss();

  const stats: ClientStat[] = [];
  const sockets: Socket[] = [];

  for (let i = 0; i < p.n; i++) {
    const stat = newStat();
    stats.push(stat);

    const socket = ioClient(urls.serverUrl, {
      transports: ["websocket"],
      reconnection: true,
      // Spread reconnects so 1000+ clients don't pile on the server.
      reconnectionDelay: 2000,
      reconnectionDelayMax: 5000,
      reconnectionAttempts: Infinity,
      timeout: 10000,
    });
    socket.on("connect", () => {
      // CSR-resumed sockets have `recovered === true`; rooms are auto-rejoined.
      const recovered = (socket as unknown as { recovered?: boolean }).recovered;
      if (recovered) stat.recoveredCount++;
      else socket.emit("join", p.stream);
    });
    socket.on("connect_error", () => stat.failedConnects++);
    socket.on("message", (msg: unknown) => recordMsg(stat, msg));
    sockets.push(socket);

    await maybePauseForRamp(p, i, "jitter-csr");
  }

  await settleAfterRamp(p);
  console.log(`[jitter-csr] all ramped; starting publisher and jitter loop`);

  const publishTask = startSocketioPublishing(p, urls);

  const endAt = Date.now() + p.durationSec * 1000;
  const jitterTasks = sockets.map((socket, i) =>
    (async () => {
      const stat = stats[i];
      let next = Date.now() + (5 + Math.random() * p.jitterIntervalSec) * 1000;
      while (Date.now() < endAt) {
        if (Date.now() >= next) {
          stat.jitterCount++;
          // socket.io-client's built-in reconnect machinery handles the
          // resume with pid + offset — we just close the TCP layer.
          terminateUnderlyingTcp(socket);
          await new Promise((r) => setTimeout(r, p.jitterDurationMs));
          next = Date.now() + (p.jitterIntervalSec + Math.random() * 5) * 1000;
        }
        await new Promise((r) => setTimeout(r, 500));
      }
    })()
  );

  await Promise.all([publishTask, ...jitterTasks]);

  for (const s of sockets) {
    try {
      s.disconnect();
    } catch {}
  }

  const peakRssMb = rss.stop();
  const result = summarize({
    label: "socketio-csr",
    totalMessages: p.totalMessages,
    stats,
    elapsedMs: Date.now() - startedAt,
    peakRssMb,
  });
  console.log(`[jitter-csr] result: ${JSON.stringify(result)}`);
  return result;
}

// ---------------------------------------------------------------------------
// Socket.io publisher: by default, delegates to socketio-server's
// /publish-local endpoint so the publish path is the realistic in-process
// io.to().emit(). Falls back to /_broadcast over HTTP if publishViaServer
// is false.

async function startSocketioPublishing(p: JitterParams, urls: SocketioUrls): Promise<void> {
  if (urls.publishViaServer === false) {
    return publish({
      url: `${urls.serverUrl}/_broadcast`,
      total: p.totalMessages,
      intervalMs: p.intervalMs,
      stream: p.stream,
    });
  }
  // Default path on Railway: trigger publishing on the Socket.io server.
  const qs = new URLSearchParams({
    total: String(p.totalMessages),
    interval: String(p.intervalMs),
    stream: p.stream,
  });
  try {
    await fetch(`${urls.serverUrl}/publish-local?${qs.toString()}`, { method: "POST" });
  } catch {
    /* publish kickoff failure shows up as zero deliveries */
  }
  // /publish-local runs publishing async on the server; we sleep for the
  // publishing window so this task tracks the same time.
  await new Promise((r) => setTimeout(r, p.totalMessages * p.intervalMs));
}
