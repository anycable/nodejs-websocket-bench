// Avalanche runner: connect N socket.io-client sockets, wait for an
// externally-triggered server restart, measure how the fleet recovers.
//
// Listeners (connect / disconnect) are attached at socket creation —
// before the restart can possibly fire — so events are never lost in
// the gap between the SIGKILL/`railway restart` landing and the test
// process being ready to listen. (See Vladimir's review note from
// 2026-05-04.)

import { io as ioClient, Socket } from "socket.io-client";

import { percentile } from "./core/stats.js";
import { settleAfterRamp } from "./core/timing.js";

export interface AvalancheParams {
  n: number;
  rampPerSec: number;
  // Seconds to wait between ramp completion and the moment we expect
  // the external restart trigger. Caller is expected to fire the
  // restart during this window (e.g. via `railway restart`).
  prearmSec: number;
  // Seconds to wait after the first disconnect for clients to reconnect.
  recoveryWaitSec: number;
  stream: string;
}

export interface AvalancheResult {
  clients: number;
  initiallyConnected: number;
  disconnected: number;
  reconnected: number;
  reconnectRatePct: number;
  neverReconnected: number;
  neverReconnectedPct: number;
  // ms — first disconnect detected vs the moment the bench detected
  // restart (= first disconnect, by definition).
  disconnectSpreadMs: number;
  // ms from restartDetected to time when 95% of `connected` are back.
  recoveryTimeMs: number;
  reconnectMs: { p50: number; p95: number; p99: number; max: number };
  totalDowntimeMs: number;
  rampElapsedMs: number;
  totalElapsedMs: number;
}

export async function runAvalancheSocketio(
  p: AvalancheParams,
  serverUrl: string
): Promise<AvalancheResult> {
  console.log(
    `[avalanche-sio] target=${serverUrl} n=${p.n} ramp=${p.rampPerSec}/s prearm=${p.prearmSec}s recoveryWait=${p.recoveryWaitSec}s`
  );

  const sockets: Socket[] = [];
  const startedAt = Date.now();

  let initiallyConnected = 0;
  let initialConnectDone = false;
  // Once we start tearing down, disconnect/connect events from the
  // cleanup phase (s.disconnect()) shouldn't be counted as part of
  // the avalanche cycle.
  let tearingDown = false;

  let disconnected = 0;
  let firstDisconnectAt = 0;
  let allDisconnectedAt = 0;
  let restartDetectedAt = 0;

  let reconnected = 0;
  let firstReconnectAt = 0;
  let allReconnectedAt = 0;
  const reconnectTimes: number[] = [];

  // Phase 1: ramp up. Listeners attached at creation, so a restart
  // even mid-ramp can't outpace them.
  for (let i = 0; i < p.n; i++) {
    const socket = ioClient(serverUrl, {
      transports: ["websocket"],
      reconnection: true,
      reconnectionDelay: 500,
      reconnectionDelayMax: 5000,
      reconnectionAttempts: Infinity,
      timeout: 10000,
    });

    socket.on("connect", () => {
      if (tearingDown) return;
      if (!initialConnectDone) {
        initiallyConnected++;
        socket.emit("join", p.stream);
        return;
      }
      // After ramp done, any new `connect` is a reconnect post-restart.
      if (restartDetectedAt > 0) {
        reconnected++;
        const now = Date.now();
        reconnectTimes.push(now - restartDetectedAt);
        if (reconnected === 1) firstReconnectAt = now;
        if (
          reconnected >= initiallyConnected * 0.95 &&
          !allReconnectedAt
        ) {
          allReconnectedAt = now;
        }
      }
    });

    socket.on("disconnect", () => {
      if (tearingDown) return;
      if (!initialConnectDone) return; // ignore noise from the ramp
      disconnected++;
      const now = Date.now();
      if (disconnected === 1) {
        firstDisconnectAt = now;
        restartDetectedAt = now;
      }
      if (disconnected === initiallyConnected) {
        allDisconnectedAt = now;
      }
    });

    sockets.push(socket);

    if ((i + 1) % p.rampPerSec === 0) {
      await new Promise((r) => setTimeout(r, 1000));
      if ((i + 1) % 1000 === 0) {
        console.log(`[avalanche-sio] ramped ${i + 1}/${p.n}`);
      }
    }
  }

  // Settle so straggler initial connects land.
  await settleAfterRamp();
  initialConnectDone = true;
  const rampElapsedMs = Date.now() - startedAt;
  console.log(
    `[avalanche-sio] all ramped (${rampElapsedMs}ms): ${initiallyConnected}/${p.n} connected`
  );

  // Phase 2: pre-arm window. Caller is expected to trigger the
  // server restart during this window.
  console.log(
    `[avalanche-sio] ready for restart — caller has ${p.prearmSec}s + recovery`
  );

  // Wait for restart or timeout. We watch `restartDetectedAt` (set
  // when the first disconnect arrives).
  const armDeadline = Date.now() + p.prearmSec * 1000;
  while (Date.now() < armDeadline && restartDetectedAt === 0) {
    await new Promise((r) => setTimeout(r, 200));
  }

  if (restartDetectedAt === 0) {
    console.log(
      `[avalanche-sio] no restart detected within ${p.prearmSec}s — aborting`
    );
  } else {
    console.log(
      `[avalanche-sio] restart detected — waiting for reconnects (up to ${p.recoveryWaitSec}s)`
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

  // Tear down — flip the flag first so the listeners stop counting
  // these as part of the avalanche cycle.
  tearingDown = true;
  for (const s of sockets) {
    try {
      s.disconnect();
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
