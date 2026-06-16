// Deploy-impact runner — Socket.io with Redis adapter (clustered).
//
// The structural test that decides whether in-process WS is sustainable
// past one node. Setup:
//
//   - N socket.io-clients connect to the cluster (a single LB domain
//     that round-robins to multiple Socket.io instances behind it, or
//     a list of per-node domains we round-robin client-side).
//   - A publisher emits monotonically-numbered messages to a stream at
//     a fixed rate throughout the entire test.
//   - Mid-test, the orchestrator triggers a rolling deploy: each node
//     is drained + redeployed in sequence.
//   - Every client logs every message it receives (with timestamp).
//   - Per client, we compute the gap: time between the last successfully
//     received message before disconnect and the first one after
//     reconnect. We also compute messages lost = published in the gap
//     minus received in the gap.
//
// Output: gap p50/p95/p99/max, msgsLost p50/p95/p99/max, deliveryRatePct,
// reconnect stats, full-reconnect time. These are the load-bearing
// numbers for section 3 of the compare page.
//
// Listeners are pre-attached at socket creation so we never lose
// disconnect / reconnect events in the gap between deploy-trigger
// landing and the runner being ready to count.

import { io as ioClient, Socket } from "socket.io-client";

import { percentile } from "./core/stats.js";
import { settleAfterRamp } from "./core/timing.js";

export interface DeployImpactParams {
  n: number;
  rampPerSec: number;
  stream: string;
  // Publishing
  publishRatePerSec: number; // e.g. 2 messages/sec
  preDeploySec: number; // hold steady-state before triggering deploy
  postDeploySec: number; // wait after deploy starts for full recovery
}

export interface DeployImpactResult {
  clients: number;
  initiallyConnected: number;
  // Publishing totals
  totalPublishedMsgs: number;
  expectedDeliveries: number; // = totalPublished * initiallyConnected
  totalReceivedMsgs: number;
  deliveryRatePct: number;
  // Per-client gap (ms between last-msg-before-disconnect and first-msg-after-reconnect)
  // Only computed for clients that actually disconnected.
  affectedClients: number;
  gapMs: { p50: number; p95: number; p99: number; max: number };
  // Per-client messages lost during the gap window.
  msgsLostPerClient: { p50: number; p95: number; p99: number; max: number };
  totalMsgsLost: number;
  // Reconnect behavior
  reconnectedCount: number;
  reconnectRatePct: number;
  reconnectMs: { p50: number; p95: number; p99: number; max: number };
  // Wall-clock recovery
  firstDisconnectAt: number; // unix ms
  fullReconnectTimeMs: number; // ms from first disconnect to 95% reconnected
  rampElapsedMs: number;
  totalElapsedMs: number;
}

interface ClientState {
  socket: Socket;
  // Per-message reception log: seq -> recvAt (unix ms)
  received: Map<number, number>;
  disconnectedAt: number; // unix ms; 0 if never
  reconnectedAt: number; // unix ms; 0 if never
  // Tracked for ramp-vs-cycle distinction
  initialConnectComplete: boolean;
}

/**
 * Run the deploy-impact test against a Socket.io cluster.
 *
 * The runner holds connections + publishes throughout. The CALLER
 * (local driver) is responsible for triggering the rolling deploy
 * at the right moment by running `railway redeploy` sequentially
 * against each cluster node. The runner uses first-disconnect-event
 * as the deploy-detected marker so we don't need explicit coordination.
 *
 * @param p test parameters
 * @param serverUrls list of one or more Socket.io node URLs (clients
 *   round-robin across these to spread load)
 * @param publish a callback that publishes message `seq` to the stream.
 *   Wired by the caller to whatever publish path applies (HTTP POST to
 *   one node's `/_broadcast`, direct `io.emit()`, etc.)
 */
export async function runDeployImpactSocketio(
  p: DeployImpactParams,
  serverUrls: string[],
  publish: (seq: number) => Promise<void>,
): Promise<DeployImpactResult> {
  if (serverUrls.length === 0) {
    throw new Error("at least one serverUrl required");
  }
  console.log(
    `[deploy-impact] nodes=${serverUrls.length} n=${p.n} ramp=${p.rampPerSec}/s ` +
      `pubRate=${p.publishRatePerSec}/s preDeploy=${p.preDeploySec}s postDeploy=${p.postDeploySec}s`,
  );

  const clients: ClientState[] = [];
  const startedAt = Date.now();
  let tearingDown = false;

  let initiallyConnected = 0;
  let firstDisconnectAt = 0;
  let reconnectedCount = 0;
  let allReconnectedAt = 0;
  const reconnectMs: number[] = [];

  // Phase 1: ramp. Round-robin across nodes so the cluster's nodes
  // share the load evenly. Listeners attached at creation.
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
      received: new Map(),
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
      // Reconnect after a deploy-induced disconnect
      if (state.disconnectedAt > 0 && state.reconnectedAt === 0) {
        const now = Date.now();
        state.reconnectedAt = now;
        reconnectedCount++;
        reconnectMs.push(now - state.disconnectedAt);
        // Re-join the stream (server-side rooms are lost on a fresh
        // socket connection)
        socket.emit("join", p.stream);
        if (
          reconnectedCount >= Math.floor(initiallyConnected * 0.95) &&
          !allReconnectedAt
        ) {
          allReconnectedAt = now;
        }
      }
    });

    socket.on("disconnect", () => {
      if (tearingDown) return;
      if (!state.initialConnectComplete) return;
      if (state.disconnectedAt > 0) return; // already counted
      const now = Date.now();
      state.disconnectedAt = now;
      if (!firstDisconnectAt) firstDisconnectAt = now;
    });

    // Per-message receive log. Payload is `{ seq: number }` from the
    // publisher; we record the seq + arrival timestamp. Event name
    // matches what `/_broadcast` emits server-side: 'message'.
    socket.on("message", (payload: { seq?: number } | undefined) => {
      if (tearingDown) return;
      if (typeof payload?.seq === "number") {
        state.received.set(payload.seq, Date.now());
      }
    });

    clients.push(state);

    if ((i + 1) % p.rampPerSec === 0) {
      await new Promise((r) => setTimeout(r, 1000));
      if ((i + 1) % 1000 === 0) {
        console.log(`[deploy-impact] ramped ${i + 1}/${p.n}`);
      }
    }
  }

  // Settle so straggler initial connects land
  await settleAfterRamp();
  const rampElapsedMs = Date.now() - startedAt;
  console.log(
    `[deploy-impact] all ramped (${rampElapsedMs}ms): ${initiallyConnected}/${p.n} initial-connected`,
  );

  // Phase 2: publisher loop. Runs through preDeploy + deploy + postDeploy.
  // Tracks per-message publish time so we can compute "what was published
  // during each client's gap window."
  let publishSeq = 0;
  const publishedAt = new Map<number, number>();
  const publishIntervalMs = Math.max(1, Math.round(1000 / p.publishRatePerSec));
  let publisherStopped = false;

  const publisher = (async () => {
    while (!publisherStopped) {
      const seq = ++publishSeq;
      publishedAt.set(seq, Date.now());
      try {
        await publish(seq);
      } catch (err) {
        // Publishing might transiently fail mid-deploy; log and continue
        if (publishSeq % 50 === 0) {
          console.log(`[deploy-impact] publish error at seq=${seq}: ${err}`);
        }
      }
      await new Promise((r) => setTimeout(r, publishIntervalMs));
    }
  })();

  console.log(
    `[deploy-impact] holding steady-state for ${p.preDeploySec}s — caller fires rolling deploy during this window`,
  );
  await new Promise((r) => setTimeout(r, p.preDeploySec * 1000));

  console.log(
    `[deploy-impact] post-deploy hold ${p.postDeploySec}s for stragglers to reconnect + catch up`,
  );
  await new Promise((r) => setTimeout(r, p.postDeploySec * 1000));

  // Stop the publisher
  publisherStopped = true;
  await publisher;

  // Phase 3: settle. Give one more second for in-flight messages to
  // land before we read state.
  await new Promise((r) => setTimeout(r, 1000));
  if (!allReconnectedAt) allReconnectedAt = Date.now();

  // Per-client analytics
  const gaps: number[] = [];
  const lostPerClient: number[] = [];
  let totalReceived = 0;
  let totalLost = 0;
  let affected = 0;

  for (const c of clients) {
    if (!c.initialConnectComplete) continue;
    totalReceived += c.received.size;

    if (c.disconnectedAt === 0) {
      // Never lost the connection — no gap, no loss
      lostPerClient.push(0);
      continue;
    }
    affected++;

    // Gap = first-received-after-reconnect minus last-received-before-disconnect.
    // Anchor the "before" side at disconnectedAt; "after" side at reconnectedAt.
    // If we never reconnected, gap is open-ended (use end of test).
    let lastBefore = 0;
    let firstAfter = 0;
    for (const [, recvAt] of c.received) {
      if (recvAt < c.disconnectedAt && recvAt > lastBefore) lastBefore = recvAt;
      if (
        recvAt > c.disconnectedAt &&
        (firstAfter === 0 || recvAt < firstAfter)
      )
        firstAfter = recvAt;
    }
    const gapEnd =
      firstAfter > 0
        ? firstAfter
        : c.reconnectedAt > 0
          ? c.reconnectedAt
          : Date.now();
    const gapStart =
      lastBefore > 0 ? lastBefore : c.disconnectedAt;
    gaps.push(gapEnd - gapStart);

    // Messages lost = those published during the gap that this client
    // never received (regardless of timing — they just don't appear
    // in `received`).
    let lost = 0;
    for (const [seq, pubAt] of publishedAt) {
      if (pubAt >= gapStart && pubAt <= gapEnd && !c.received.has(seq)) {
        lost++;
      }
    }
    lostPerClient.push(lost);
    totalLost += lost;
  }

  gaps.sort((a, b) => a - b);
  lostPerClient.sort((a, b) => a - b);
  reconnectMs.sort((a, b) => a - b);

  const expectedDeliveries = publishSeq * initiallyConnected;

  // Tear down
  tearingDown = true;
  for (const c of clients) {
    try {
      c.socket.disconnect();
    } catch {
      /* ignore */
    }
  }

  const fullReconnectTimeMs =
    firstDisconnectAt > 0 && allReconnectedAt > firstDisconnectAt
      ? allReconnectedAt - firstDisconnectAt
      : 0;

  return {
    clients: p.n,
    initiallyConnected,
    totalPublishedMsgs: publishSeq,
    expectedDeliveries,
    totalReceivedMsgs: totalReceived,
    deliveryRatePct:
      expectedDeliveries > 0
        ? Number(((totalReceived / expectedDeliveries) * 100).toFixed(2))
        : 0,
    affectedClients: affected,
    gapMs: {
      p50: percentile(gaps, 50),
      p95: percentile(gaps, 95),
      p99: percentile(gaps, 99),
      max: percentile(gaps, 100),
    },
    msgsLostPerClient: {
      p50: percentile(lostPerClient, 50),
      p95: percentile(lostPerClient, 95),
      p99: percentile(lostPerClient, 99),
      max: percentile(lostPerClient, 100),
    },
    totalMsgsLost: totalLost,
    reconnectedCount,
    reconnectRatePct:
      initiallyConnected > 0
        ? Number(((reconnectedCount / initiallyConnected) * 100).toFixed(2))
        : 0,
    reconnectMs: {
      p50: percentile(reconnectMs, 50),
      p95: percentile(reconnectMs, 95),
      p99: percentile(reconnectMs, 99),
      max: percentile(reconnectMs, 100),
    },
    firstDisconnectAt,
    fullReconnectTimeMs,
    rampElapsedMs,
    totalElapsedMs: Date.now() - startedAt,
  };
}
