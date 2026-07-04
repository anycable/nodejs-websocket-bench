// Catch-up probe: does an AnyCable subscriber recover messages it wasn't
// present for? Distinguishes the two paths that behave very differently:
//
//   RESUME (old client): a client that already subscribed (has a session +
//     stream offset), drops, and reconnects. AnyCable restores the session and
//     replays the stream from the last offset. Expect: backfill of the gap.
//
//   COLD (new client): a brand-new client subscribing for the first time,
//     AFTER messages were already broadcast. No prior offset. Expect: this is
//     the open question, does it catch up on the backlog or start at "now"?
//
// Runs a handful of clients against the public gateway (no runner fleet).
// Env: CABLE_URL, BROADCAST_URL, BROADCAST_SECRET, CHANNEL.
import WebSocket from "ws";
import { createCable } from "@anycable/core";

const CABLE = process.env.CABLE_URL!;
const BCAST = process.env.BROADCAST_URL!;
const SECRET = process.env.BROADCAST_SECRET || "benchsecret";
const CHANNEL = process.env.CHANNEL || "BenchmarkChannel";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function mkCable(historyTimestamp?: number) {
  return createCable(CABLE, {
    websocketImplementation: WebSocket as unknown as typeof globalThis.WebSocket,
    protocol: "actioncable-v1-ext-json" as never,
    logLevel: "error" as never,
    ...(historyTimestamp ? { historyTimestamp } as never : {}),
  });
}

async function broadcast(stream: string, seqs: number[]) {
  for (const seq of seqs) {
    await fetch(BCAST, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${SECRET}` },
      body: JSON.stringify({ stream, data: JSON.stringify({ seq, sentAt: Date.now() }) }),
    });
    await sleep(40);
  }
}

// Extract our seq from whatever shape the message arrives in.
function seqOf(msg: unknown): number | null {
  let m: unknown = msg;
  if (typeof m === "string") {
    try { m = JSON.parse(m); } catch { return null; }
  }
  if (m && typeof m === "object" && typeof (m as { seq?: unknown }).seq === "number") {
    return (m as { seq: number }).seq;
  }
  return null;
}

function subscribe(cable: ReturnType<typeof mkCable>, stream: string, sink: number[]) {
  const ch = cable.subscribeTo(CHANNEL, { stream_name: stream });
  ch.on("message", (msg: unknown) => {
    const s = seqOf(msg);
    if (s !== null) sink.push(s);
  });
  return ch;
}

function uniqSorted(a: number[]) { return [...new Set(a)].sort((x, y) => x - y); }

async function coldCatchup(stamp: number, M: number) {
  const stream = `catchup-cold-${stamp}-${M}`;
  await broadcast(stream, Array.from({ length: M }, (_, i) => i + 1)); // broadcast FIRST
  await sleep(1200);
  const cable = mkCable();
  const got: number[] = [];
  subscribe(cable, stream, got);                                        // then subscribe (new client)
  await sleep(4500);
  cable.disconnect();
  const g = uniqSorted(got);
  console.log(`[COLD  M=${M}] new client subscribed AFTER ${M} broadcasts -> caught up ${g.length}/${M}` +
    (g.length ? `  (first=${g[0]} last=${g[g.length - 1]})` : ""));
  await sleep(400);
}

async function resumeReconnect(stamp: number) {
  const stream = `catchup-resume-${stamp}`;
  const cable = mkCable();
  const got: number[] = [];
  subscribe(cable, stream, got);
  await sleep(1500);                       // let subscription confirm
  await broadcast(stream, [1, 2, 3]);      // received while connected
  await sleep(1000);
  const beforeDrop = uniqSorted(got);
  // Force-drop the underlying socket (unclean), like a network blip / restart.
  const transport = (cable as unknown as { transport?: { ws?: { terminate?: () => void; close?: () => void } } }).transport;
  transport?.ws?.terminate?.() ?? transport?.ws?.close?.();
  await sleep(400);
  await broadcast(stream, [4, 5, 6, 7, 8]); // sent DURING the outage
  await sleep(200);
  await cable.connect().catch(() => {});    // reconnect (session restore + resume)
  await sleep(5000);
  cable.disconnect();
  const after = uniqSorted(got);
  const backfilled = [4, 5, 6, 7, 8].filter((s) => after.includes(s));
  console.log(`[RESUME] before drop: ${beforeDrop.join(",")}  | sent during outage: 4,5,6,7,8  | after reconnect total: ${after.join(",")}`);
  console.log(`[RESUME] gap backfilled on reconnect: ${backfilled.length}/5 (${backfilled.join(",")})`);
}

// Same as coldCatchup, but the client is created with historyTimestamp set to
// before the broadcasts, so it explicitly requests stream history on subscribe.
async function coldWithHistory(stamp: number, M: number) {
  const since = Math.floor(Date.now() / 1000) - 120; // 120s ago, before the broadcast
  const stream = `catchup-hist-${stamp}-${M}`;
  await broadcast(stream, Array.from({ length: M }, (_, i) => i + 1));
  await sleep(1200);
  const cable = mkCable(since);
  const got: number[] = [];
  subscribe(cable, stream, got);
  await sleep(5000);
  cable.disconnect();
  const g = uniqSorted(got);
  console.log(`[COLD+HIST M=${M}] new client WITH historyTimestamp -> caught up ${g.length}/${M}` +
    (g.length ? `  (first=${g[0]} last=${g[g.length - 1]})` : ""));
  await sleep(400);
}

async function baseline(stamp: number) {
  const stream = `catchup-base-${stamp}`;
  const cable = mkCable();
  const got: number[] = [];
  subscribe(cable, stream, got);
  await sleep(1500);
  await broadcast(stream, [1, 2, 3, 4, 5]);
  await sleep(2500);
  cable.disconnect();
  console.log(`[BASE ] subscribe-then-broadcast 5 -> received ${uniqSorted(got).length}/5 (sanity)`);
  await sleep(400);
}

async function main() {
  console.log(`Catch-up probe -> ${CABLE}  (channel ${CHANNEL})\n`);
  const stamp = Date.now();
  await coldCatchup(stamp, 20);          // control: cold client, no history request
  await coldWithHistory(stamp, 20);      // cold client that DOES request history
  await coldWithHistory(stamp, 150);     // ... and beyond history_limit=100
}
main().then(() => process.exit(0)).catch((e) => { console.error(e); process.exit(1); });
