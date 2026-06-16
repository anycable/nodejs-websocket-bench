// Synchronous idle-connection probe against anycable-go.
//
// Opens N raw WebSocket connections (using `actioncable-v1-ext-json` so
// it's the same connection type a real client would establish), waits for
// the server's "welcome", subscribes to a stream, holds for `holdSec`,
// then tears down and resolves with the final counts.
//
// The probe is sharded by deploying multiple bench-runner instances —
// each runs as a separate Railway container with its own source IP and
// its own outbound port pool. See `bench/idle-multi.ts` for the
// coordinator that fans out and aggregates.

import WebSocket from "ws";
import { io as ioClient, Socket } from "socket.io-client";

import { settleAfterRamp } from "./core/timing.js";

// uWS idle: opens raw `ws` WebSocket against the uws-server's /ws path
// (no subprotocol — uWS's App.ws() doesn't negotiate protocols), sends
// the {type:"subscribe"} frame on open, holds, tears down. Same shape
// as the AnyCable / socket.io variants so the multi-shard coordinator
// works without changes.
export async function runIdleUws(
  p: IdleParams,
  serverWsUrl: string,
  shardLabel?: string
): Promise<IdleResult> {
  const tag = shardLabel ? `[idle-uws:${shardLabel}]` : "[idle-uws]";
  console.log(
    `${tag} target=${serverWsUrl} n=${p.n} ramp=${p.rampPerSec}/s hold=${p.holdSec}s`
  );

  const result = { connected: 0, welcomed: 0, subscribed: 0, failed: 0 };
  const sockets: WebSocket[] = [];
  const startedAt = Date.now();

  for (let i = 0; i < p.n; i++) {
    const ws = new WebSocket(serverWsUrl);
    sockets.push(ws);

    let opened = false;
    ws.once("open", () => {
      opened = true;
      result.connected++;
      // No welcome frame from uWS — we count welcomed alongside connected
      // so the field has the same semantic as the socket.io variant.
      result.welcomed++;
      try {
        ws.send(JSON.stringify({ type: "subscribe", topic: p.stream }));
        // uWS doesn't ack subscriptions — count "subscribed" as soon as
        // the frame is on the wire, mirroring socket.io's join semantic.
        result.subscribed++;
      } catch {
        /* socket may have closed mid-handshake; not interesting here */
      }
    });
    ws.once("error", () => {
      if (!opened) result.failed++;
    });

    if ((i + 1) % p.rampPerSec === 0) {
      await new Promise((r) => setTimeout(r, 1000));
      if ((i + 1) % 1000 === 0) {
        console.log(
          `${tag} ramped ${i + 1}/${p.n}  connected=${result.connected} welcomed=${result.welcomed} subscribed=${result.subscribed} failed=${result.failed}`
        );
      }
    }
  }

  await settleAfterRamp();
  const rampElapsedMs = Date.now() - startedAt;
  console.log(
    `${tag} all ramped (${rampElapsedMs}ms): connected=${result.connected}/${p.n} welcomed=${result.welcomed} subscribed=${result.subscribed} failed=${result.failed}`
  );

  console.log(`${tag} holding ${p.holdSec}s...`);
  const holdStartedAt = Date.now();
  await new Promise((r) => setTimeout(r, p.holdSec * 1000));
  const holdElapsedMs = Date.now() - holdStartedAt;

  console.log(
    `${tag} hold complete: connected=${result.connected} welcomed=${result.welcomed} subscribed=${result.subscribed} failed=${result.failed}`
  );

  for (let i = 0; i < sockets.length; i += 500) {
    for (const s of sockets.slice(i, i + 500)) {
      try {
        s.close();
      } catch {
        /* ignore tear-down errors */
      }
    }
    await new Promise((r) => setTimeout(r, 100));
  }

  return {
    ...result,
    rampElapsedMs,
    holdElapsedMs,
    totalElapsedMs: Date.now() - startedAt,
    shardLabel,
  };
}

export interface IdleParams {
  n: number;
  holdSec: number;
  rampPerSec: number;
  stream: string;
}

export interface IdleResult {
  connected: number;
  welcomed: number;
  subscribed: number;
  failed: number;
  rampElapsedMs: number;
  holdElapsedMs: number;
  totalElapsedMs: number;
  shardLabel?: string;
}

export async function runIdleAnycable(
  p: IdleParams,
  cableUrl: string,
  shardLabel?: string
): Promise<IdleResult> {
  const tag = shardLabel ? `[idle:${shardLabel}]` : "[idle]";
  console.log(
    `${tag} target=${cableUrl} n=${p.n} ramp=${p.rampPerSec}/s hold=${p.holdSec}s`
  );

  const result = { connected: 0, welcomed: 0, subscribed: 0, failed: 0 };
  const sockets: WebSocket[] = [];
  const startedAt = Date.now();

  for (let i = 0; i < p.n; i++) {
    const ws = new WebSocket(cableUrl, ["actioncable-v1-ext-json"]);
    sockets.push(ws);

    // `failed` should count connection ATTEMPTS that never opened —
    // not transient errors on already-established sockets (which can
    // fire on idle ping timeouts or during tear-down, both of which
    // we don't want to double-count against `connected`).
    let opened = false;
    ws.once("open", () => {
      opened = true;
      result.connected++;
    });
    ws.once("error", () => {
      if (!opened) result.failed++;
    });
    ws.on("message", (raw) => {
      try {
        const msg = JSON.parse(raw.toString());
        if (msg.type === "welcome") {
          result.welcomed++;
          // Subscribe to a $pubsub stream — works without RPC since
          // anycable-go is started with ANYCABLE_PUBLIC=true.
          ws.send(
            JSON.stringify({
              command: "subscribe",
              identifier: JSON.stringify({
                channel: "$pubsub",
                stream_name: p.stream,
              }),
            })
          );
        } else if (msg.type === "confirm_subscription") {
          result.subscribed++;
        }
      } catch {
        /* malformed messages aren't interesting for an idle probe */
      }
    });

    if ((i + 1) % p.rampPerSec === 0) {
      await new Promise((r) => setTimeout(r, 1000));
      if ((i + 1) % 1000 === 0) {
        console.log(
          `${tag} ramped ${i + 1}/${p.n}  connected=${result.connected} welcomed=${result.welcomed} subscribed=${result.subscribed} failed=${result.failed}`
        );
      }
    }
  }

  // Settle for a few seconds so welcome/subscribe acks land before reporting.
  await settleAfterRamp();
  const rampElapsedMs = Date.now() - startedAt;
  console.log(
    `${tag} all ramped (${rampElapsedMs}ms): connected=${result.connected}/${p.n} welcomed=${result.welcomed} subscribed=${result.subscribed} failed=${result.failed}`
  );

  console.log(`${tag} holding ${p.holdSec}s...`);
  const holdStartedAt = Date.now();
  await new Promise((r) => setTimeout(r, p.holdSec * 1000));
  const holdElapsedMs = Date.now() - holdStartedAt;

  console.log(
    `${tag} hold complete: connected=${result.connected} welcomed=${result.welcomed} subscribed=${result.subscribed} failed=${result.failed}`
  );

  // Tear down — close in batches so we don't burst the OS with 25K
  // simultaneous closes.
  for (let i = 0; i < sockets.length; i += 500) {
    for (const s of sockets.slice(i, i + 500)) {
      try {
        s.close();
      } catch {
        /* ignore tear-down errors */
      }
    }
    await new Promise((r) => setTimeout(r, 100));
  }

  return {
    ...result,
    rampElapsedMs,
    holdElapsedMs,
    totalElapsedMs: Date.now() - startedAt,
    shardLabel,
  };
}


// Same shape, different transport: opens socket.io-client connections
// against a Socket.io server, emits `join` once connected, holds, tears
// down. Maps cleanly into the same IdleResult so coordinators can fan
// out to either anycable or socketio shards via the same plumbing.
//
// Note: `welcomed` and `subscribed` are not separate concepts in
// Socket.io — once `connect` fires we count both. We keep the field
// names identical to AnyCable's report so aggregation code is shared.
export async function runIdleSocketio(
  p: IdleParams,
  serverUrl: string,
  shardLabel?: string
): Promise<IdleResult> {
  const tag = shardLabel ? `[idle-sio:${shardLabel}]` : "[idle-sio]";
  console.log(
    `${tag} target=${serverUrl} n=${p.n} ramp=${p.rampPerSec}/s hold=${p.holdSec}s`
  );

  const result = { connected: 0, welcomed: 0, subscribed: 0, failed: 0 };
  const sockets: Socket[] = [];
  const startedAt = Date.now();

  for (let i = 0; i < p.n; i++) {
    const sock = ioClient(serverUrl, {
      transports: ["websocket"],
      reconnection: false,
      timeout: 10000,
    });
    sockets.push(sock);

    let opened = false;
    sock.once("connect", () => {
      opened = true;
      result.connected++;
      result.welcomed++;
      sock.emit("join", p.stream);
      // Treat the post-connect emit as our "subscribe" milestone since
      // socket.io's `join` is fire-and-forget (no ack from server).
      result.subscribed++;
    });
    sock.once("connect_error", () => {
      if (!opened) result.failed++;
    });

    if ((i + 1) % p.rampPerSec === 0) {
      await new Promise((r) => setTimeout(r, 1000));
      if ((i + 1) % 1000 === 0) {
        console.log(
          `${tag} ramped ${i + 1}/${p.n}  connected=${result.connected} welcomed=${result.welcomed} subscribed=${result.subscribed} failed=${result.failed}`
        );
      }
    }
  }

  await settleAfterRamp();
  const rampElapsedMs = Date.now() - startedAt;
  console.log(
    `${tag} all ramped (${rampElapsedMs}ms): connected=${result.connected}/${p.n} welcomed=${result.welcomed} subscribed=${result.subscribed} failed=${result.failed}`
  );

  console.log(`${tag} holding ${p.holdSec}s...`);
  const holdStartedAt = Date.now();
  await new Promise((r) => setTimeout(r, p.holdSec * 1000));
  const holdElapsedMs = Date.now() - holdStartedAt;

  console.log(
    `${tag} hold complete: connected=${result.connected} welcomed=${result.welcomed} subscribed=${result.subscribed} failed=${result.failed}`
  );

  for (let i = 0; i < sockets.length; i += 500) {
    for (const s of sockets.slice(i, i + 500)) {
      try {
        s.disconnect();
      } catch {
        /* ignore tear-down errors */
      }
    }
    await new Promise((r) => setTimeout(r, 100));
  }

  return {
    ...result,
    rampElapsedMs,
    holdElapsedMs,
    totalElapsedMs: Date.now() - startedAt,
    shardLabel,
  };
}
