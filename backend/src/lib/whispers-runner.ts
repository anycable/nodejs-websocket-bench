// Whispers runner — client-to-client updates that bypass the backend.
//
// Workload: N clients distributed across R rooms (N/R peers per room).
// Each client periodically "whispers" a small payload to its room; every
// other peer in that room receives it. We measure:
//   - Delivery rate (received / expected)
//   - Latency p50/p95/p99 (sender's sentAt to receiver's recvAt)
//   - Per-client receive count
//
// Two protocols:
//   - AnyCable native: channel.whisper(payload). anycable-go fans out
//     without invoking application code. This is the "free for backend"
//     case (the one that puts AnyCable in the Liveblocks/PartyKit
//     category).
//   - Socket.io emulated: socket.emit("whisper", room, payload). The
//     socketio-server's "whisper" handler re-emits to room minus sender.
//     Server CPU is in the path; no app code runs.

import { WebSocket } from "ws";
import { io as ioClient, Socket } from "socket.io-client";
import { createCable } from "@anycable/core";

import { downsampleSorted, percentile } from "./core/stats.js";
import { settleAfterRamp } from "./core/timing.js";

export interface WhispersParams {
  n: number; // total clients
  rooms: number; // number of rooms; clients distributed round-robin
  rampPerSec: number;
  whisperIntervalMs: number; // each client whispers every N ms
  testDurationSec: number; // hold + measure window
  payloadBytes: number; // size of the whisper payload in bytes
  // When set, room names become `${roomPrefix}-${i % rooms}` instead of
  // the default `whisper-room-${i % rooms}`. Multi-shard drivers pass a
  // shared per-run prefix so every shard's clients land in the same rooms
  // (fan-out crosses shards via the broker, bench-runner load per shard
  // drops to ~total / numShards).
  roomPrefix?: string;
  // When set, the result includes up to `samplesCap` sorted latency
  // samples for downstream merging. Linear-interpolated downsample.
  samplesCap?: number;
}

export interface WhispersResult {
  protocol: "anycable" | "socketio" | "uws";
  n: number;
  rooms: number;
  initiallyConnected: number;
  whisperIntervalMs: number;
  whispersSent: number;
  whispersReceived: number;
  expectedReceived: number; // sent × (peers per room - 1)
  deliveryRatePct: number;
  latencyMs: { p50: number; p95: number; p99: number; max: number };
  totalElapsedMs: number;
  // Set when WhispersParams.samplesCap is positive. Used by the multi-shard
  // coordinator to recompute global percentiles across shards.
  latencySamplesSorted?: number[];
}

interface ClientStat {
  sent: number;
  received: number;
  latencies: number[];
  connected: boolean;
}

const padPayload = (size: number) =>
  size <= 0 ? "" : "x".repeat(Math.max(0, size - 20)); // 20 bytes reserved for seq+sentAt JSON

// ---------------------------------------------------------------------------
// AnyCable variant
// ---------------------------------------------------------------------------

export async function runWhispersAnycable(
  p: WhispersParams,
  cableUrl: string,
): Promise<WhispersResult> {
  console.log(
    `[whispers-ac] n=${p.n} rooms=${p.rooms} interval=${p.whisperIntervalMs}ms duration=${p.testDurationSec}s cable=${cableUrl}`,
  );
  const startedAt = Date.now();
  const stats: ClientStat[] = [];
  const cables: ReturnType<typeof createCable>[] = [];
  const channels: ReturnType<ReturnType<typeof createCable>["streamFrom"]>[] = [];
  let initiallyConnected = 0;
  const filler = padPayload(p.payloadBytes);

  for (let i = 0; i < p.n; i++) {
    const stat: ClientStat = {
      sent: 0,
      received: 0,
      latencies: [],
      connected: false,
    };
    stats.push(stat);

    const cable = createCable(cableUrl, {
      websocketImplementation: WebSocket as unknown as typeof globalThis.WebSocket,
      protocol: "actioncable-v1-ext-json",
      logLevel: "error" as never,
    });
    cable.on("connect", () => {
      if (!stat.connected) {
        stat.connected = true;
        initiallyConnected++;
      }
    });
    cable.on("disconnect", () => {});
    cable.on("close", () => {});

    const room = `${p.roomPrefix ?? "whisper-room"}-${i % p.rooms}`;
    const channel = cable.streamFrom(room);
    channel.on("message", (msg: unknown) => {
      const data =
        typeof msg === "string"
          ? safeParse(msg)
          : (msg as { sentAt?: number } | undefined);
      if (data && typeof data.sentAt === "number") {
        stat.received++;
        stat.latencies.push(Date.now() - data.sentAt);
      }
    });

    cables.push(cable);
    channels.push(channel);

    if ((i + 1) % p.rampPerSec === 0) {
      await new Promise((r) => setTimeout(r, 1000));
      if ((i + 1) % 1000 === 0) {
        console.log(`[whispers-ac] ramped ${i + 1}/${p.n}`);
      }
    }
  }
  await settleAfterRamp();
  console.log(
    `[whispers-ac] all ramped (${Date.now() - startedAt}ms): ${initiallyConnected}/${p.n} connected`,
  );

  // Whisper loop: each client whispers every whisperIntervalMs.
  const stopAt = Date.now() + p.testDurationSec * 1000;
  const tasks = channels.map((channel, i) =>
    (async () => {
      const stat = stats[i];
      // Stagger initial fire so all clients don't whisper at the exact
      // same tick.
      const stagger = Math.floor(Math.random() * p.whisperIntervalMs);
      await new Promise((r) => setTimeout(r, stagger));
      while (Date.now() < stopAt) {
        try {
          await channel.whisper({
            sentAt: Date.now(),
            from: i,
            pad: filler,
          });
          stat.sent++;
        } catch {
          /* swallow whisper errors (offline, etc.) */
        }
        await new Promise((r) => setTimeout(r, p.whisperIntervalMs));
      }
    })(),
  );

  await Promise.all(tasks);
  // Settle for in-flight delivery
  await new Promise((r) => setTimeout(r, 3000));

  for (const c of cables) {
    try {
      c.disconnect();
    } catch {
      /* ignore */
    }
  }

  return summarize("anycable", p, stats, initiallyConnected, startedAt);
}

// ---------------------------------------------------------------------------
// Socket.io variant
// ---------------------------------------------------------------------------

export interface SocketioWhisperUrl {
  serverUrl: string;
}

export async function runWhispersSocketio(
  p: WhispersParams,
  urls: SocketioWhisperUrl,
): Promise<WhispersResult> {
  console.log(
    `[whispers-sio] n=${p.n} rooms=${p.rooms} interval=${p.whisperIntervalMs}ms duration=${p.testDurationSec}s url=${urls.serverUrl}`,
  );
  const startedAt = Date.now();
  const stats: ClientStat[] = [];
  const sockets: Socket[] = [];
  const roomNames: string[] = [];
  let initiallyConnected = 0;
  const filler = padPayload(p.payloadBytes);

  for (let i = 0; i < p.n; i++) {
    const stat: ClientStat = {
      sent: 0,
      received: 0,
      latencies: [],
      connected: false,
    };
    stats.push(stat);

    const room = `${p.roomPrefix ?? "whisper-room"}-${i % p.rooms}`;
    roomNames.push(room);

    const socket = ioClient(urls.serverUrl, {
      transports: ["websocket"],
      reconnection: true,
      reconnectionDelay: 500,
    });
    socket.on("connect", () => {
      if (!stat.connected) {
        stat.connected = true;
        initiallyConnected++;
        socket.emit("join", room);
      }
    });
    socket.on("whisper", (payload: { sentAt?: number } | undefined) => {
      if (payload && typeof payload.sentAt === "number") {
        stat.received++;
        stat.latencies.push(Date.now() - payload.sentAt);
      }
    });

    sockets.push(socket);

    if ((i + 1) % p.rampPerSec === 0) {
      await new Promise((r) => setTimeout(r, 1000));
      if ((i + 1) % 1000 === 0) {
        console.log(`[whispers-sio] ramped ${i + 1}/${p.n}`);
      }
    }
  }
  await settleAfterRamp();
  console.log(
    `[whispers-sio] all ramped (${Date.now() - startedAt}ms): ${initiallyConnected}/${p.n} connected`,
  );

  const stopAt = Date.now() + p.testDurationSec * 1000;
  const tasks = sockets.map((socket, i) =>
    (async () => {
      const stat = stats[i];
      const room = roomNames[i];
      const stagger = Math.floor(Math.random() * p.whisperIntervalMs);
      await new Promise((r) => setTimeout(r, stagger));
      while (Date.now() < stopAt) {
        try {
          socket.emit("whisper", room, {
            sentAt: Date.now(),
            from: i,
            pad: filler,
          });
          stat.sent++;
        } catch {
          /* */
        }
        await new Promise((r) => setTimeout(r, p.whisperIntervalMs));
      }
    })(),
  );

  await Promise.all(tasks);
  await new Promise((r) => setTimeout(r, 3000));

  for (const s of sockets) {
    try {
      s.disconnect();
    } catch {
      /* ignore */
    }
  }

  return summarize("socketio", p, stats, initiallyConnected, startedAt);
}

// ---------------------------------------------------------------------------
// uWS variant
// ---------------------------------------------------------------------------

export interface UwsWhisperUrl {
  serverWsUrl: string;
}

export async function runWhispersUws(
  p: WhispersParams,
  urls: UwsWhisperUrl,
): Promise<WhispersResult> {
  console.log(
    `[whispers-uws] n=${p.n} rooms=${p.rooms} interval=${p.whisperIntervalMs}ms duration=${p.testDurationSec}s url=${urls.serverWsUrl}`,
  );
  const startedAt = Date.now();
  const stats: ClientStat[] = [];
  const sockets: WebSocket[] = [];
  const topicNames: string[] = [];
  let initiallyConnected = 0;
  const filler = padPayload(p.payloadBytes);

  for (let i = 0; i < p.n; i++) {
    const stat: ClientStat = {
      sent: 0,
      received: 0,
      latencies: [],
      connected: false,
    };
    stats.push(stat);

    const topic = `${p.roomPrefix ?? "whisper-room"}-${i % p.rooms}`;
    topicNames.push(topic);

    const ws = new WebSocket(urls.serverWsUrl);
    ws.on("open", () => {
      if (!stat.connected) {
        stat.connected = true;
        initiallyConnected++;
        ws.send(JSON.stringify({ type: "subscribe", topic }));
      }
    });
    ws.on("message", (data) => {
      let text: string;
      if (typeof data === "string") text = data;
      else if (Buffer.isBuffer(data)) text = data.toString("utf-8");
      else return;
      try {
        const parsed = JSON.parse(text) as {
          type?: string;
          payload?: { sentAt?: number };
        };
        if (
          parsed.type === "whisper" &&
          parsed.payload &&
          typeof parsed.payload.sentAt === "number"
        ) {
          stat.received++;
          stat.latencies.push(Date.now() - parsed.payload.sentAt);
        }
      } catch {
        /* */
      }
    });
    ws.on("error", () => {});

    sockets.push(ws);

    if ((i + 1) % p.rampPerSec === 0) {
      await new Promise((r) => setTimeout(r, 1000));
      if ((i + 1) % 1000 === 0) {
        console.log(`[whispers-uws] ramped ${i + 1}/${p.n}`);
      }
    }
  }
  await settleAfterRamp();
  console.log(
    `[whispers-uws] all ramped (${Date.now() - startedAt}ms): ${initiallyConnected}/${p.n} connected`,
  );

  const stopAt = Date.now() + p.testDurationSec * 1000;
  const tasks = sockets.map((ws, i) =>
    (async () => {
      const stat = stats[i];
      const topic = topicNames[i];
      const stagger = Math.floor(Math.random() * p.whisperIntervalMs);
      await new Promise((r) => setTimeout(r, stagger));
      while (Date.now() < stopAt) {
        try {
          if (ws.readyState === ws.OPEN) {
            ws.send(
              JSON.stringify({
                type: "whisper",
                topic,
                payload: { sentAt: Date.now(), from: i, pad: filler },
              }),
            );
            stat.sent++;
          }
        } catch {
          /* */
        }
        await new Promise((r) => setTimeout(r, p.whisperIntervalMs));
      }
    })(),
  );

  await Promise.all(tasks);
  await new Promise((r) => setTimeout(r, 3000));

  for (const s of sockets) {
    try {
      s.close();
    } catch {
      /* ignore */
    }
  }

  return summarize("uws", p, stats, initiallyConnected, startedAt);
}

// ---------------------------------------------------------------------------
// Shared summary
// ---------------------------------------------------------------------------

function summarize(
  protocol: "anycable" | "socketio" | "uws",
  p: WhispersParams,
  stats: ClientStat[],
  initiallyConnected: number,
  startedAt: number,
): WhispersResult {
  let sent = 0;
  let received = 0;
  const allLatencies: number[] = [];
  for (const s of stats) {
    sent += s.sent;
    received += s.received;
    for (const l of s.latencies) allLatencies.push(l);
  }
  allLatencies.sort((a, b) => a - b);

  // Per-room sent counts and peers
  const sentPerRoom = new Map<number, number>();
  const peersPerRoom = new Map<number, number>();
  for (let i = 0; i < stats.length; i++) {
    const r = i % p.rooms;
    sentPerRoom.set(r, (sentPerRoom.get(r) || 0) + stats[i].sent);
    peersPerRoom.set(r, (peersPerRoom.get(r) || 0) + 1);
  }
  let expected = 0;
  for (const [room, roomSent] of sentPerRoom) {
    const peers = peersPerRoom.get(room) || 0;
    expected += roomSent * Math.max(0, peers - 1);
  }

  return {
    protocol,
    n: p.n,
    rooms: p.rooms,
    initiallyConnected,
    whisperIntervalMs: p.whisperIntervalMs,
    whispersSent: sent,
    whispersReceived: received,
    expectedReceived: expected,
    deliveryRatePct:
      expected > 0 ? Math.round((received / expected) * 10000) / 100 : 0,
    latencyMs: {
      p50: percentile(allLatencies, 50),
      p95: percentile(allLatencies, 95),
      p99: percentile(allLatencies, 99),
      max: percentile(allLatencies, 100),
    },
    totalElapsedMs: Date.now() - startedAt,
    latencySamplesSorted: p.samplesCap
      ? downsampleSorted(allLatencies, p.samplesCap)
      : undefined,
  };
}

function safeParse(s: string): { sentAt?: number } | undefined {
  try {
    return JSON.parse(s);
  } catch {
    return undefined;
  }
}
