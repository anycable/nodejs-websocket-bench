// Standalone deploy-impact runner.
//
// Mirrors deploy-impact-runner.ts but tests the architectural OPPOSITE: a
// separate publisher service publishes broadcasts to the WS service over
// HTTP. The publisher is what gets redeployed mid-test. The WS service is
// untouched, so WS connections should never drop.
//
// The metric of interest flips: instead of "how disrupted is each user?"
// (large, every user affected on every embedded deploy), this measures
// "how long does the publishing pause last, and do clients miss anything
// while the publisher is down?"
//
// Expected outcome for properly standalone setups:
//   - Affected clients: 0 (none disconnect)
//   - Per-client gap: ~publisher downtime (15-30s for a Railway redeploy)
//   - Messages lost: 0 (no messages were sent during the pause; clients
//     resume receiving when the publisher comes back)
//
// Library: same client model as deploy-impact-runner. The runner just
// holds connections + records receive events; the driver redeploys the
// publisher Railway service mid-test.

import { io as ioClient, Socket } from "socket.io-client";

import { percentile } from "./stats.js";
import { settleAfterRamp } from "./timing.js";

export interface StandaloneDeployImpactParams {
  n: number;
  rampPerSec: number;
  stream: string;
  // Total runtime; the driver fires the publisher redeploy somewhere in
  // the middle of this window.
  testDurationSec: number;
}

export interface StandaloneDeployImpactResult {
  clients: number;
  initiallyConnected: number;
  // Per-client tracking
  affectedClients: number; // clients that disconnected at least once
  reconnectedCount: number;
  reconnectMs: { p50: number; p95: number; p99: number; max: number };
  // Reception pattern around the publisher gap (gap = period of no
  // messages received cluster-wide while publisher is down)
  longestGapMs: number; // longest contiguous "no messages received" window
  publisherDowntimeEstSec: number; // longestGapMs / 1000 rounded
  totalReceivedMsgs: number;
  uniqueMessagesReceived: number;
  rampElapsedMs: number;
  totalElapsedMs: number;
}

interface ClientState {
  socket: Socket;
  // Set of message seqs received
  received: Set<number>;
  disconnectedAt: number;
  reconnectedAt: number;
  initialConnectComplete: boolean;
}

/**
 * Run the standalone deploy-impact test. The publisher service is
 * external; this runner only holds WS clients and counts what they see.
 * The driver is responsible for triggering the publisher redeploy.
 */
export async function runStandaloneDeployImpactSocketio(
  p: StandaloneDeployImpactParams,
  serverUrls: string[],
): Promise<StandaloneDeployImpactResult> {
  if (serverUrls.length === 0) {
    throw new Error("at least one serverUrl required");
  }
  console.log(
    `[standalone-deploy-impact] nodes=${serverUrls.length} n=${p.n} ramp=${p.rampPerSec}/s duration=${p.testDurationSec}s`,
  );

  const clients: ClientState[] = [];
  const startedAt = Date.now();
  let tearingDown = false;
  let initiallyConnected = 0;
  let firstReceiveAt = 0;
  let lastReceiveAt = 0;
  // Track all receive timestamps to find gaps (we'll bucket by 100ms)
  const receiveCountPer100ms = new Map<number, number>();
  let reconnectedCount = 0;
  const reconnectMs: number[] = [];

  // Phase 1: ramp
  for (let i = 0; i < p.n; i++) {
    const url = serverUrls[i % serverUrls.length];
    const socket = ioClient(url, {
      transports: ["websocket"],
      reconnection: true,
      reconnectionDelay: 500,
      reconnectionDelayMax: 5000,
      reconnectionAttempts: Infinity,
      timeout: 10000,
    });

    const state: ClientState = {
      socket,
      received: new Set<number>(),
      disconnectedAt: 0,
      reconnectedAt: 0,
      initialConnectComplete: false,
    };

    socket.on("connect", () => {
      if (tearingDown) return;
      if (!state.initialConnectComplete) {
        state.initialConnectComplete = true;
        initiallyConnected++;
        socket.emit("join", p.stream);
        return;
      }
      if (state.disconnectedAt > 0 && state.reconnectedAt === 0) {
        const now = Date.now();
        state.reconnectedAt = now;
        reconnectedCount++;
        reconnectMs.push(now - state.disconnectedAt);
        socket.emit("join", p.stream);
      }
    });

    socket.on("disconnect", () => {
      if (tearingDown) return;
      if (!state.initialConnectComplete) return;
      if (state.disconnectedAt > 0) return;
      state.disconnectedAt = Date.now();
    });

    socket.on("message", (payload: { seq?: number } | undefined) => {
      if (tearingDown) return;
      if (typeof payload?.seq === "number") {
        state.received.add(payload.seq);
        const now = Date.now();
        if (!firstReceiveAt) firstReceiveAt = now;
        lastReceiveAt = now;
        const bucket = Math.floor(now / 100);
        receiveCountPer100ms.set(bucket, (receiveCountPer100ms.get(bucket) || 0) + 1);
      }
    });

    clients.push(state);

    if ((i + 1) % p.rampPerSec === 0) {
      await new Promise((r) => setTimeout(r, 1000));
      if ((i + 1) % 1000 === 0) {
        console.log(`[standalone-deploy-impact] ramped ${i + 1}/${p.n}`);
      }
    }
  }

  await settleAfterRamp();
  const rampElapsedMs = Date.now() - startedAt;
  console.log(
    `[standalone-deploy-impact] all ramped (${rampElapsedMs}ms): ${initiallyConnected}/${p.n} initial`,
  );

  // Phase 2: hold and observe. The driver fires the publisher redeploy
  // somewhere in this window. The publisher service should be publishing
  // throughout; we'll detect gaps in receive cadence.
  console.log(
    `[standalone-deploy-impact] holding for ${p.testDurationSec}s; driver fires publisher redeploy during this window`,
  );
  await new Promise((r) => setTimeout(r, p.testDurationSec * 1000));

  // Settle
  await new Promise((r) => setTimeout(r, 2000));

  // Analytics
  // Find longest gap: contiguous run of empty 100ms buckets between
  // firstReceiveAt and lastReceiveAt.
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
      c.socket.disconnect();
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
