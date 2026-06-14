// Standalone deploy-impact runner — AnyCable variant.
//
// Counterpart to standalone-deploy-impact-runner.ts (which holds
// Socket.io clients). This one holds N AnyCable clients against an
// anycable-go WS process. The publisher (a separate Railway service)
// posts broadcasts to anycable-go's /_broadcast endpoint, and the
// driver redeploys that publisher mid-test.
//
// Architectural property under test: AnyCable is standalone by design.
// Redeploying the publisher (or any external Rails-shaped app) must
// never disturb the anycable-go WS layer. We expect:
//   - affectedClients: 0
//   - longestGapMs ≈ publisher restart window
//   - all clients receive every broadcast outside the gap window

import { WebSocket } from "ws";
import { createCable } from "@anycable/core";

import { percentile } from "./stats.js";
import { settleAfterRamp } from "./timing.js";

export interface StandaloneDeployImpactAnycableParams {
  n: number;
  rampPerSec: number;
  stream: string;
  testDurationSec: number;
}

export interface StandaloneDeployImpactAnycableResult {
  clients: number;
  initiallyConnected: number;
  affectedClients: number;
  reconnectedCount: number;
  reconnectMs: { p50: number; p95: number; p99: number; max: number };
  longestGapMs: number;
  publisherDowntimeEstSec: number;
  totalReceivedMsgs: number;
  uniqueMessagesReceived: number;
  rampElapsedMs: number;
  totalElapsedMs: number;
}

interface ClientState {
  cable: ReturnType<typeof createCable>;
  received: Set<number>;
  disconnectedAt: number;
  reconnectedAt: number;
  initialConnectComplete: boolean;
}

/**
 * Run the standalone deploy-impact test against anycable-go. Driver is
 * responsible for redeploying the publisher Railway service mid-test;
 * this runner only counts what each client sees.
 */
export async function runStandaloneDeployImpactAnycable(
  p: StandaloneDeployImpactAnycableParams,
  cableUrl: string,
): Promise<StandaloneDeployImpactAnycableResult> {
  console.log(
    `[standalone-deploy-impact-ac] cable=${cableUrl} n=${p.n} ramp=${p.rampPerSec}/s duration=${p.testDurationSec}s`,
  );

  const clients: ClientState[] = [];
  const startedAt = Date.now();
  let tearingDown = false;
  let initiallyConnected = 0;
  let firstReceiveAt = 0;
  let lastReceiveAt = 0;
  const receiveCountPer100ms = new Map<number, number>();
  let reconnectedCount = 0;
  const reconnectMs: number[] = [];

  // Phase 1: ramp
  for (let i = 0; i < p.n; i++) {
    const cable = createCable(cableUrl, {
      websocketImplementation: WebSocket as unknown as typeof globalThis.WebSocket,
      protocol: "actioncable-v1-ext-json",
      logLevel: "error" as never,
    });

    const state: ClientState = {
      cable,
      received: new Set<number>(),
      disconnectedAt: 0,
      reconnectedAt: 0,
      initialConnectComplete: false,
    };

    cable.on("connect", () => {
      if (tearingDown) return;
      if (!state.initialConnectComplete) {
        state.initialConnectComplete = true;
        initiallyConnected++;
        return;
      }
      if (state.disconnectedAt > 0 && state.reconnectedAt === 0) {
        const now = Date.now();
        state.reconnectedAt = now;
        reconnectedCount++;
        reconnectMs.push(now - state.disconnectedAt);
      }
    });

    cable.on("disconnect", () => {
      if (tearingDown) return;
      if (!state.initialConnectComplete) return;
      if (state.disconnectedAt > 0) return;
      state.disconnectedAt = Date.now();
    });
    cable.on("close", () => {});

    const channel = cable.streamFrom(p.stream);
    channel.on("message", (msg: unknown) => {
      if (tearingDown) return;
      // Publisher payload arrives as JSON-stringified
      // `{"seq": N, "sentAt": ...}` inside `data`; @anycable/core
      // parses it for us so msg is already an object.
      const parsed =
        typeof msg === "string"
          ? safeParse(msg)
          : (msg as { seq?: number } | undefined);
      if (parsed && typeof parsed.seq === "number") {
        state.received.add(parsed.seq);
        const now = Date.now();
        if (!firstReceiveAt) firstReceiveAt = now;
        lastReceiveAt = now;
        const bucket = Math.floor(now / 100);
        receiveCountPer100ms.set(
          bucket,
          (receiveCountPer100ms.get(bucket) || 0) + 1,
        );
      }
    });

    clients.push(state);

    if ((i + 1) % p.rampPerSec === 0) {
      await new Promise((r) => setTimeout(r, 1000));
      if ((i + 1) % 1000 === 0) {
        console.log(`[standalone-deploy-impact-ac] ramped ${i + 1}/${p.n}`);
      }
    }
  }

  await settleAfterRamp();
  const rampElapsedMs = Date.now() - startedAt;
  console.log(
    `[standalone-deploy-impact-ac] all ramped (${rampElapsedMs}ms): ${initiallyConnected}/${p.n} initial`,
  );

  // Phase 2: hold and observe. Driver fires publisher redeploy somewhere
  // in this window; we detect the gap from the receive-bucket map.
  console.log(
    `[standalone-deploy-impact-ac] holding for ${p.testDurationSec}s`,
  );
  await new Promise((r) => setTimeout(r, p.testDurationSec * 1000));
  await new Promise((r) => setTimeout(r, 2000));

  // Analytics
  let longestGapMs = 0;
  if (firstReceiveAt > 0 && lastReceiveAt > firstReceiveAt) {
    const startBucket = Math.floor(firstReceiveAt / 100);
    const endBucket = Math.floor(lastReceiveAt / 100);
    let currentGap = 0;
    for (let b = startBucket; b <= endBucket; b++) {
      if ((receiveCountPer100ms.get(b) || 0) === 0) {
        currentGap += 100;
        if (currentGap > longestGapMs) longestGapMs = currentGap;
      } else {
        currentGap = 0;
      }
    }
  }

  let affected = 0;
  let totalReceived = 0;
  const uniqueSeqs = new Set<number>();
  for (const c of clients) {
    if (!c.initialConnectComplete) continue;
    totalReceived += c.received.size;
    for (const s of c.received) uniqueSeqs.add(s);
    if (c.disconnectedAt > 0) affected++;
  }

  reconnectMs.sort((a, b) => a - b);

  tearingDown = true;
  for (const c of clients) {
    try {
      c.cable.disconnect();
    } catch {
      /* ignore */
    }
  }

  return {
    clients: p.n,
    initiallyConnected,
    affectedClients: affected,
    reconnectedCount,
    reconnectMs: {
      p50: percentile(reconnectMs, 50),
      p95: percentile(reconnectMs, 95),
      p99: percentile(reconnectMs, 99),
      max: percentile(reconnectMs, 100),
    },
    longestGapMs,
    publisherDowntimeEstSec: Math.round(longestGapMs / 100) / 10,
    totalReceivedMsgs: totalReceived,
    uniqueMessagesReceived: uniqueSeqs.size,
    rampElapsedMs,
    totalElapsedMs: Date.now() - startedAt,
  };
}

function safeParse(s: string): { seq?: number } | undefined {
  try {
    return JSON.parse(s);
  } catch {
    return undefined;
  }
}
