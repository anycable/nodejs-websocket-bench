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

    ws.once("open", () => {
      result.connected++;
    });
    ws.once("error", () => {
      result.failed++;
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
  await new Promise((r) => setTimeout(r, 5000));
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
