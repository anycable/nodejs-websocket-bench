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
import { createCable, backoffWithJitter } from "@anycable/core";
// The official Rails client (for the Action Cable / Solid Cable / Async::Cable
// targets), set up to run under Node. AnyCable keeps @anycable/core.
import { ActionCable } from "./core/actioncable-node.js";
import { io as ioClient, Socket } from "socket.io-client";

import { ClientStat, JitterResult, newStat, recordMsg, summarize } from "./core/stats.js";
import { MIN_OFFLINE_MS, settleAfterRamp } from "./core/timing.js";
import { trackPeakRss } from "./core/peak-rss.js";
import { log } from "./core/log.js";
import type { JitterParams } from "./core/params.js";

// Suppress noisy unhandledRejection logs from socket libraries during jitter.
// Done once, regardless of how many runners get invoked.
let suppressed = false;
function suppressClientRejections() {
  if (suppressed) return;
  suppressed = true;
  process.on("unhandledRejection", () => {});
}


// Pace ramp-up at `rampPerSec` new connections per second.
async function maybePauseForRamp(p: JitterParams, i: number, label: string) {
  if ((i + 1) % p.rampPerSec === 0) {
    await new Promise((r) => setTimeout(r, 1000));
    if ((i + 1) % 1000 === 0) {
      log.debug(`[${label}] ramped ${i + 1}/${p.n}`);
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

// Force-close the underlying TCP socket of an @anycable/core cable.
// `cable.transport` is a Transport interface (close()/open() etc), but
// the WebSocketTransport implementation exposes `.ws` — the actual
// `ws` WebSocket — and we use `.terminate()` on it for the same kind
// of unclean-close semantics we use against socket.io-client. After
// terminate, the cable's Monitor sees the socket close and reconnects
// with backoff, mirroring socket.io-client's retry path. This makes
// the AnyCable jitter test apples-to-apples with the Socket.io one.
function terminateCableWs(cable: ReturnType<typeof createCable>): boolean {
  const transport = (cable as unknown as { transport?: unknown }).transport as
    | { ws?: WebSocket & { terminate?: () => void; close?: () => void } }
    | undefined;
  const raw = transport?.ws;
  if (!raw) return false;
  if (typeof raw.terminate === "function") {
    raw.terminate();
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
  // Channel to subscribe to. Defaults to "$pubsub" (anycable-go's public
  // pub/sub channel, used by the standalone OSS/Pro targets via streamFrom).
  // For a real Rails app, pass "BenchmarkChannel" and the driver subscribes
  // to that named channel with { stream_name } params instead.
  channel?: string;
  // WebSocket subprotocol. AnyCable uses the extended Action Cable protocol
  // ("actioncable-v1-ext-json") which carries the delivery-guarantee /
  // resume machinery; vanilla Action Cable and Solid Cable speak the base
  // protocol ("actioncable-v1-json").
  acProtocol?: string;
  // Base delay (ms) for the client's reconnect backoff. When set, the first
  // reconnect fires in ~reconnectBaseMs (then exponential x2 up to 5s) instead
  // of @anycable/core's multi-second default. Smaller values shrink the
  // resume-tail p99 after a transient drop (the tail = drop + reconnect delay).
  // Only applies to the @anycable/core client.
  reconnectBaseMs?: number;
  // Which JS client to drive with. "anycable" (default) = @anycable/core
  // (extended protocol, resume) for the AnyCable target. "actioncable" =
  // @rails/actioncable (the official Rails client, base protocol, no resume)
  // for the Action Cable / Solid Cable / Async::Cable targets.
  clientLib?: "anycable" | "actioncable";
}

export async function runJitterAnycable(
  p: JitterParams,
  urls: AnycableUrls
): Promise<JitterResult> {
  suppressClientRejections();
  log.info(`[jitter-ac] params=${JSON.stringify(p)}`);
  const startedAt = Date.now();
  const rss = trackPeakRss();

  const stats: ClientStat[] = [];
  // Unified control surface over the two client libraries so the jitter loop
  // and teardown stay client-agnostic. disconnect()/connect() take the client
  // cleanly offline and back — a standard fixed-length outage regardless of
  // the client's own backoff.
  interface JitterConn {
    disconnect(): void;
    connect(): void;
  }
  const conns: JitterConn[] = [];
  const useActionCable = urls.clientLib === "actioncable";

  for (let i = 0; i < p.n; i++) {
    const stat = newStat();
    stats.push(stat);

    if (useActionCable) {
      // Official Rails client. createConsumer connects lazily; the
      // subscription re-establishes on reconnect (no resume, base protocol).
      const consumer = ActionCable.createConsumer(urls.cableUrl);
      consumer.subscriptions.create(
        { channel: urls.channel ?? "BenchmarkChannel", stream_name: p.stream },
        {
          connected() {
            stat.everConnected = true;
          },
          received(data: unknown) {
            recordMsg(stat, data);
          },
        }
      );
      conns.push({
        // Drop the underlying socket uncleanly (like a network blip) but leave
        // the ConnectionMonitor running, so the official Rails client recovers
        // on its OWN native, poll-based schedule (seconds) rather than an
        // immediate reconnect. This is what a real Action Cable app experiences.
        disconnect: () => {
          const conn = (
            consumer as unknown as {
              connection?: { webSocket?: { close?: () => void } };
            }
          ).connection;
          conn?.webSocket?.close?.();
        },
        // No-op: the native monitor drives reconnection.
        connect: () => {},
      });
    } else {
      const cable = createCable(urls.cableUrl, {
        websocketImplementation: WebSocket as unknown as typeof globalThis.WebSocket,
        protocol: (urls.acProtocol ?? "actioncable-v1-ext-json") as never,
        // The @anycable/core types don't include "error" yet; the runtime
        // accepts any of error|warn|info|debug.
        logLevel: "error" as never,
        ...(urls.reconnectBaseMs && urls.reconnectBaseMs > 0
          ? {
              reconnectStrategy: backoffWithJitter(urls.reconnectBaseMs, {
                backoffRate: 2,
                jitterRatio: 0.2,
                maxInterval: 5000,
              }),
            }
          : {}),
      });
      cable.on("close", () => {});
      cable.on("disconnect", () => {});
      cable.on("connect", () => {
        stat.everConnected = true;
      });
      // "$pubsub" -> anycable-go's signed pub/sub channel (streamFrom). Any
      // other value -> a real Rails channel subscribed with { stream_name }.
      const channel =
        urls.channel && urls.channel !== "$pubsub"
          ? cable.subscribeTo(urls.channel, { stream_name: p.stream })
          : cable.streamFrom(p.stream);
      channel.on("message", (msg: unknown) => recordMsg(stat, msg));
      conns.push({
        disconnect: () => cable.disconnect(),
        connect: () => {
          cable.connect().catch(() => {});
        },
      });
    }

    await maybePauseForRamp(p, i, "jitter-ac");
  }

  await settleAfterRamp();
  log.info(`[jitter-ac] all ramped; starting publisher and jitter loop`);

  const publishTask = publish({
    url: urls.broadcastUrl,
    secret: urls.broadcastSecret,
    total: p.totalMessages,
    intervalMs: p.intervalMs,
    stream: p.stream,
  });

  const endAt = Date.now() + p.durationSec * 1000;
  const jitterTasks = conns.map((conn, i) =>
    (async () => {
      const stat = stats[i];
      let next = Date.now() + (5 + Math.random() * p.jitterIntervalSec) * 1000;
      while (Date.now() < endAt) {
        if (Date.now() >= next) {
          // Simulate a standard ~jitterDurationMs network outage. disconnect()
          // takes the client cleanly offline (its reconnect monitor is stopped,
          // so the outage length is fixed regardless of backoff); after the
          // window connect() brings it back. AnyCable resumes the messages
          // broadcast during the outage (session id retained), the
          // @rails/actioncable at-most-once clients simply lose them.
          stat.jitterCount++;
          conn.disconnect();
          await new Promise((r) => setTimeout(r, p.jitterDurationMs));
          conn.connect();
          next = Date.now() + (p.jitterIntervalSec + Math.random() * 5) * 1000;
        }
        await new Promise((r) => setTimeout(r, 500));
      }
    })()
  );

  await Promise.all([publishTask, ...jitterTasks]);

  for (const conn of conns) {
    try {
      conn.disconnect();
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
    samplesCap: p.samplesCap,
  });
  log.info(`[jitter-ac] result: ${JSON.stringify(result)}`);
  return result;
}

// ---------------------------------------------------------------------------
// Socket.io URL bundle — used by both default and CSR variants.

export interface SocketioUrls {
  serverUrl: string;
  // Default: per-message HTTP POST to /_broadcast. This matches how
  // production publishers actually work (events emit one at a time from
  // outside the WS process) and keeps Socket.io / uWS / AnyCable on the
  // same publish path. Set this to `true` to delegate publishing to
  // socketio-server's /publish-local endpoint instead (one HTTP trigger,
  // then a 100-message emit() loop inside the server process — useful
  // for measuring in-process fan-out in isolation).
  publishViaServer?: boolean;
}

// ---------------------------------------------------------------------------
// Socket.io default — reconnection: false, manual fresh socket on jitter.

export async function runJitterSocketio(
  p: JitterParams,
  urls: SocketioUrls
): Promise<JitterResult> {
  suppressClientRejections();
  log.info(`[jitter-sio] params=${JSON.stringify(p)}`);
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
    socket.on("connect", () => {
      stat.everConnected = true;
      socket.emit("join", p.stream);
    });
    socket.on("connect_error", () => stat.failedConnects++);
    bindHandlers(socket, stat);
    sockets.push({ current: socket, stat });

    await maybePauseForRamp(p, i, "jitter-sio");
  }

  await settleAfterRamp();
  log.info(`[jitter-sio] all ramped; starting publisher and jitter loop`);

  const publishTask = startSocketioPublishing(p, urls);

  const endAt = Date.now() + p.durationSec * 1000;
  const jitterTasks = sockets.map((entry) =>
    (async () => {
      let next = Date.now() + (5 + Math.random() * p.jitterIntervalSec) * 1000;
      while (Date.now() < endAt) {
        if (Date.now() >= next) {
          // Force-close the TCP layer if we can reach it; fall back to a
          // library-level disconnect. Only count the event when something
          // actually closed — a socket that's already mid-reconnect (no
          // engine.transport.ws AND not currently connected) is a no-op
          // both ways and shouldn't inflate jitterEvents.
          const wasConnected = entry.current.connected;
          const torn = terminateUnderlyingTcp(entry.current);
          if (!torn && wasConnected) entry.current.disconnect();
          if (torn || wasConnected) entry.stat.jitterCount++;
          // Default Socket.io has `reconnection: false`, so the offline
          // window is set entirely by us, not the library. Floor it to
          // MIN_OFFLINE_MS so the four jitter configurations face the
          // same disruption shape (CSR / AnyCable / uWS sit at ~2 s
          // because their libraries' built-in backoff dominates over
          // jitterDurationMs anyway). Without the floor, default
          // Socket.io would be measured against a ~1 s window while
          // the others see ~2–5 s — its loss rate would understate
          // what a typical socket.io-client user with default
          // `reconnection: true` settings would actually experience.
          await new Promise((r) =>
            setTimeout(r, Math.max(p.jitterDurationMs, MIN_OFFLINE_MS)),
          );

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
    samplesCap: p.samplesCap,
  });
  log.info(`[jitter-sio] result: ${JSON.stringify(result)}`);
  return result;
}

// ---------------------------------------------------------------------------
// Socket.io + Connection State Recovery — reconnection: true, native CSR flow.

export async function runJitterSocketioCsr(
  p: JitterParams,
  urls: SocketioUrls
): Promise<JitterResult> {
  suppressClientRejections();
  log.info(`[jitter-csr] params=${JSON.stringify(p)}`);
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
      stat.everConnected = true;
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

  await settleAfterRamp();
  log.info(`[jitter-csr] all ramped; starting publisher and jitter loop`);

  const publishTask = startSocketioPublishing(p, urls);

  const endAt = Date.now() + p.durationSec * 1000;
  const jitterTasks = sockets.map((socket, i) =>
    (async () => {
      const stat = stats[i];
      let next = Date.now() + (5 + Math.random() * p.jitterIntervalSec) * 1000;
      while (Date.now() < endAt) {
        if (Date.now() >= next) {
          // socket.io-client's built-in reconnect machinery handles the
          // resume with pid + offset — we just close the TCP layer.
          // Only count the jitter when terminate actually severed
          // something (sockets mid-reconnect have no engine.transport.ws)
          // so csrResumeRatePct's denominator stays honest.
          if (terminateUnderlyingTcp(socket)) {
            stat.jitterCount++;
          }
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
    samplesCap: p.samplesCap,
  });
  log.info(`[jitter-csr] result: ${JSON.stringify(result)}`);
  return result;
}

// ---------------------------------------------------------------------------
// Socket.io publisher: default is per-message HTTP POST to /_broadcast,
// matching how AnyCable is published and how production publishers actually
// emit (one event at a time from outside the WS process). Set
// publishViaServer=true to trigger an in-process emit() loop on the
// Socket.io server instead — useful for isolating in-process fan-out cost.

async function startSocketioPublishing(p: JitterParams, urls: SocketioUrls): Promise<void> {
  if (urls.publishViaServer !== true) {
    return publish({
      url: `${urls.serverUrl}/_broadcast`,
      total: p.totalMessages,
      intervalMs: p.intervalMs,
      stream: p.stream,
    });
  }
  // Opt-in path: trigger publishing inside the Socket.io server process.
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
