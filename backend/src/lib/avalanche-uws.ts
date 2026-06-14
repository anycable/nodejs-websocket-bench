// Avalanche runner: connect N raw-WS clients to the uWS server, wait
// for an externally-triggered restart (Railway redeploy), measure how
// the fleet recovers.
//
// The reconnecting client wrapper attaches its open/close listeners at
// construction so events between the SIGKILL landing and the test
// process being "ready to listen" can't slip through (matches the rule
// applied to the socket.io and AnyCable variants).

import WebSocket from "ws";

import { percentile } from "./stats.js";
import { settleAfterRamp } from "./timing.js";

export interface AvalancheUwsParams {
  n: number;
  rampPerSec: number;
  prearmSec: number;
  recoveryWaitSec: number;
  stream: string;
}

export interface AvalancheUwsResult {
  clients: number;
  initiallyConnected: number;
  disconnected: number;
  reconnected: number;
  reconnectRatePct: number;
  neverReconnected: number;
  neverReconnectedPct: number;
  disconnectSpreadMs: number;
  recoveryTimeMs: number;
  reconnectMs: { p50: number; p95: number; p99: number; max: number };
  totalDowntimeMs: number;
  rampElapsedMs: number;
  totalElapsedMs: number;
}

interface UwsClient {
  ws: WebSocket | null;
  closed: boolean;
  attempts: number;
  reconnectTimer: NodeJS.Timeout | null;
  initiallyConnectedOnce: boolean;
}

export async function runAvalancheUws(
  p: AvalancheUwsParams,
  serverWsUrl: string
): Promise<AvalancheUwsResult> {
  console.log(
    `[avalanche-uws] target=${serverWsUrl} n=${p.n} ramp=${p.rampPerSec}/s prearm=${p.prearmSec}s recoveryWait=${p.recoveryWaitSec}s`
  );

  const startedAt = Date.now();
  const clients: UwsClient[] = [];

  let initiallyConnected = 0;
  let initialConnectDone = false;
  let tearingDown = false;

  let disconnected = 0;
  let firstDisconnectAt = 0;
  let allDisconnectedAt = 0;
  let restartDetectedAt = 0;

  let reconnected = 0;
  let firstReconnectAt = 0;
  let allReconnectedAt = 0;
  const reconnectTimes: number[] = [];

  function connectClient(c: UwsClient): void {
    if (c.closed) return;
    let ws: WebSocket;
    try {
      ws = new WebSocket(serverWsUrl);
    } catch {
      scheduleReconnect(c);
      return;
    }
    c.ws = ws;
    ws.on("open", () => {
      c.attempts = 0;
      try {
        ws.send(JSON.stringify({ type: "subscribe", topic: p.stream }));
      } catch {
        /* close will follow */
      }
      if (tearingDown) return;
      if (!c.initiallyConnectedOnce && !initialConnectDone) {
        c.initiallyConnectedOnce = true;
        initiallyConnected++;
        return;
      }
      if (restartDetectedAt > 0) {
        reconnected++;
        const now = Date.now();
        reconnectTimes.push(now - restartDetectedAt);
        if (reconnected === 1) {
          firstReconnectAt = now;
          console.log(`  First reconnect at ${new Date().toISOString()}`);
        }
        if (
          reconnected >= initiallyConnected * 0.95 &&
          !allReconnectedAt
        ) {
          allReconnectedAt = now;
          console.log(
            `  95% reconnected (${reconnected}/${initiallyConnected}) in ${now - restartDetectedAt}ms`
          );
        }
      }
    });
    ws.on("error", () => {
      /* close will follow */
    });
    ws.on("close", () => {
      if (tearingDown) return;
      if (initialConnectDone && c.initiallyConnectedOnce) {
        disconnected++;
        const now = Date.now();
        if (disconnected === 1) {
          firstDisconnectAt = now;
          restartDetectedAt = now;
          console.log(`  First disconnect detected at ${new Date().toISOString()}`);
        }
        if (disconnected === initiallyConnected) {
          allDisconnectedAt = now;
          console.log(
            `  All ${initiallyConnected} clients disconnected (${now - firstDisconnectAt}ms spread)`
          );
        }
      }
      scheduleReconnect(c);
    });
  }

  function scheduleReconnect(c: UwsClient): void {
    if (c.closed) return;
    if (c.reconnectTimer) return;
    c.attempts++;
    const base = Math.min(500 * Math.pow(2, c.attempts - 1), 5000);
    const delay = base * (0.75 + Math.random() * 0.5);
    c.reconnectTimer = setTimeout(() => {
      c.reconnectTimer = null;
      connectClient(c);
    }, delay);
  }

  // Phase 1: ramp up.
  for (let i = 0; i < p.n; i++) {
    const c: UwsClient = {
      ws: null,
      closed: false,
      attempts: 0,
      reconnectTimer: null,
      initiallyConnectedOnce: false,
    };
    clients.push(c);
    connectClient(c);

    if ((i + 1) % p.rampPerSec === 0) {
      await new Promise((r) => setTimeout(r, 1000));
      if ((i + 1) % 1000 === 0) {
        console.log(`[avalanche-uws] ramped ${i + 1}/${p.n}`);
      }
    }
  }

  await settleAfterRamp();
  initialConnectDone = true;
  const rampElapsedMs = Date.now() - startedAt;
  console.log(
    `[avalanche-uws] all ramped (${rampElapsedMs}ms): ${initiallyConnected}/${p.n} connected`
  );

  console.log(
    `[avalanche-uws] ready for restart — caller has ${p.prearmSec}s + recovery`
  );

  const armDeadline = Date.now() + p.prearmSec * 1000;
  while (Date.now() < armDeadline && restartDetectedAt === 0) {
    await new Promise((r) => setTimeout(r, 200));
  }

  if (restartDetectedAt === 0) {
    console.log(`[avalanche-uws] no restart detected within ${p.prearmSec}s — aborting`);
  } else {
    console.log(
      `[avalanche-uws] restart detected — waiting for reconnects (up to ${p.recoveryWaitSec}s)`
    );
    const recoveryDeadline = restartDetectedAt + p.recoveryWaitSec * 1000;
    while (Date.now() < recoveryDeadline) {
      if (
        reconnected >= initiallyConnected * 0.95 ||
        Date.now() - restartDetectedAt > p.recoveryWaitSec * 1000
      ) {
        break;
      }
      await new Promise((r) => setTimeout(r, 500));
    }
    if (!allReconnectedAt) allReconnectedAt = Date.now();
  }

  tearingDown = true;
  for (const c of clients) {
    c.closed = true;
    if (c.reconnectTimer) {
      clearTimeout(c.reconnectTimer);
      c.reconnectTimer = null;
    }
    try {
      c.ws?.close();
    } catch {
      /* ignore */
    }
  }

  reconnectTimes.sort((a, b) => a - b);
  const recoveryTimeMs =
    restartDetectedAt > 0 ? allReconnectedAt - restartDetectedAt : 0;
  const neverReconnected = Math.max(0, initiallyConnected - reconnected);

  return {
    clients: p.n,
    initiallyConnected,
    disconnected,
    reconnected,
    reconnectRatePct:
      initiallyConnected > 0
        ? Number(((reconnected / initiallyConnected) * 100).toFixed(2))
        : 0,
    neverReconnected,
    neverReconnectedPct:
      initiallyConnected > 0
        ? Number(((neverReconnected / initiallyConnected) * 100).toFixed(2))
        : 0,
    disconnectSpreadMs:
      allDisconnectedAt > 0 ? allDisconnectedAt - firstDisconnectAt : 0,
    recoveryTimeMs,
    reconnectMs: {
      p50: percentile(reconnectTimes, 50),
      p95: percentile(reconnectTimes, 95),
      p99: percentile(reconnectTimes, 99),
      max: percentile(reconnectTimes, 100),
    },
    totalDowntimeMs: recoveryTimeMs,
    rampElapsedMs,
    totalElapsedMs: Date.now() - startedAt,
  };
}
