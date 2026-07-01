// Avalanche runner for the Action Cable protocol (AnyCable, Action Cable,
// Solid Cable). Connects N @anycable/core cables, waits for an externally
// triggered redeploy of the target service, and measures how the fleet
// recovers. Mirrors runAvalancheSocketio so results land in the same
// AvalancheResult shape and the rebaseline classifier works unchanged.
//
// The architectural contrast this captures:
//   - Action Cable / Solid Cable terminate WebSockets in the Puma process,
//     so redeploying the Rails app drops every connection (disconnected ~= N)
//     and they must reconnect.
//   - AnyCable terminates WebSockets in the anycable-go gateway; redeploying
//     the Rails RPC backend leaves the gateway (and the held connections)
//     untouched, so disconnected stays ~0 — the fleet survives the deploy.
//
// Listeners are attached at cable creation, before any redeploy can fire, so
// the disconnect that signals the restart is never missed.

import WebSocket from "ws";
import { createCable } from "@anycable/core";

import { percentile } from "./core/stats.js";
import { settleAfterRamp } from "./core/timing.js";
import { ActionCable } from "./core/actioncable-node.js";
import type { AvalancheParams, AvalancheResult } from "./avalanche-runner.js";

export interface AvalancheAnycableUrls {
  cableUrl: string;
  channel?: string;
  acProtocol?: string;
  // "actioncable" drives the official @rails/actioncable client (native
  // reconnect, base protocol) for Action Cable / Solid Cable / Async::Cable;
  // default @anycable/core for AnyCable.
  clientLib?: "anycable" | "actioncable";
}

export async function runAvalancheAnycable(
  p: AvalancheParams,
  urls: AvalancheAnycableUrls
): Promise<AvalancheResult> {
  const protocol = urls.acProtocol ?? "actioncable-v1-ext-json";
  const channelName = urls.channel ?? "$pubsub";
  console.log(
    `[avalanche-ac] target=${urls.cableUrl} channel=${channelName} proto=${protocol} n=${p.n} ramp=${p.rampPerSec}/s prearm=${p.prearmSec}s recoveryWait=${p.recoveryWaitSec}s`
  );

  const conns: { disconnect(): void }[] = [];
  const useActionCable = urls.clientLib === "actioncable";
  const startedAt = Date.now();

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

  // Shared connect/disconnect accounting for both client libraries. A connect
  // during the ramp counts an initial connection; a connect after a detected
  // restart counts a recovery (and its time-to-reconnect).
  const handleConnect = (isReconnect: boolean) => {
    if (tearingDown) return;
    if (!initialConnectDone) {
      if (!isReconnect) initiallyConnected++;
      return;
    }
    if (restartDetectedAt > 0) {
      reconnected++;
      const now = Date.now();
      reconnectTimes.push(now - restartDetectedAt);
      if (reconnected === 1) firstReconnectAt = now;
      if (reconnected >= initiallyConnected * 0.95 && !allReconnectedAt) {
        allReconnectedAt = now;
      }
    }
  };
  const handleDrop = () => {
    if (tearingDown || !initialConnectDone) return;
    disconnected++;
    const now = Date.now();
    if (disconnected === 1) {
      firstDisconnectAt = now;
      restartDetectedAt = now;
    }
    if (disconnected === initiallyConnected) allDisconnectedAt = now;
  };

  for (let i = 0; i < p.n; i++) {
    if (useActionCable) {
      // Official Rails client — recovers on its own native monitor after the
      // deploy drops it (no forced reconnect), so we measure its real recovery.
      const consumer = ActionCable.createConsumer(urls.cableUrl);
      consumer.subscriptions.create(
        { channel: channelName, stream_name: p.stream },
        {
          connected() {
            handleConnect(false);
          },
          disconnected() {
            handleDrop();
          },
        }
      );
      conns.push({ disconnect: () => consumer.disconnect() });
    } else {
      const cable = createCable(urls.cableUrl, {
        websocketImplementation: WebSocket as unknown as typeof globalThis.WebSocket,
        protocol: protocol as never,
        logLevel: "error" as never,
      });
      cable.on("connect", (event?: { reconnect?: boolean }) =>
        handleConnect(!!(event && event.reconnect))
      );
      cable.on("disconnect", handleDrop);
      cable.on("close", handleDrop);
      // Subscribing triggers the connection. "$pubsub" -> streamFrom (signed
      // pub/sub), any other channel -> a real Rails channel with { stream_name }.
      if (channelName !== "$pubsub") {
        cable.subscribeTo(channelName, { stream_name: p.stream });
      } else {
        cable.streamFrom(p.stream);
      }
      conns.push({ disconnect: () => cable.disconnect() });
    }

    if ((i + 1) % p.rampPerSec === 0) {
      await new Promise((r) => setTimeout(r, 1000));
      if ((i + 1) % 1000 === 0) console.log(`[avalanche-ac] ramped ${i + 1}/${p.n}`);
    }
  }

  await settleAfterRamp();
  initialConnectDone = true;
  const rampElapsedMs = Date.now() - startedAt;
  console.log(
    `[avalanche-ac] all ramped (${rampElapsedMs}ms): ${initiallyConnected}/${p.n} connected`
  );
  console.log(`[avalanche-ac] ready for redeploy — caller has ${p.prearmSec}s + recovery`);

  const armDeadline = Date.now() + p.prearmSec * 1000;
  while (Date.now() < armDeadline && restartDetectedAt === 0) {
    await new Promise((r) => setTimeout(r, 200));
  }

  if (restartDetectedAt === 0) {
    // No disconnect observed during the window. For AnyCable this is the
    // expected, healthy outcome: the gateway held every connection across the
    // Rails redeploy.
    console.log(
      `[avalanche-ac] no disconnect within ${p.prearmSec}s — connections survived the deploy`
    );
  } else {
    console.log(
      `[avalanche-ac] disconnect detected — waiting for reconnects (up to ${p.recoveryWaitSec}s)`
    );
    const recoveryDeadline = restartDetectedAt + p.recoveryWaitSec * 1000;
    while (Date.now() < recoveryDeadline) {
      if (reconnected >= initiallyConnected * 0.95) break;
      await new Promise((r) => setTimeout(r, 500));
    }
    if (!allReconnectedAt) allReconnectedAt = Date.now();
  }

  tearingDown = true;
  for (const c of conns) {
    try {
      c.disconnect();
    } catch {
      /* ignore */
    }
  }

  reconnectTimes.sort((a, b) => a - b);
  const recoveryTimeMs = restartDetectedAt > 0 ? allReconnectedAt - restartDetectedAt : 0;
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
    disconnectSpreadMs: allDisconnectedAt > 0 ? allDisconnectedAt - firstDisconnectAt : 0,
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
