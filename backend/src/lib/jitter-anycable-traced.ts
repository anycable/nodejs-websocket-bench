// Diagnostic variant of runJitterAnycable: same disruption pattern, plus
// per-cable timeline tracking so we can decompose the end-to-end latency
// into reconnect time, channel re-subscribe time, and replay-tail.
//
// Not used by the headline page. For Vladimir + Irina to read.
//
// Aggregates produced (across `traceSample` cables):
//   reconnectMs       — terminateCableWs() → cable's `connect` event fires
//   channelResubMs    — cable connect → channel `connect` event fires
//   replayLagMs       — channel connect → first message arrives after
//
// And per-message classification:
//   directMessages    — arrived without an active jitter cycle in flight
//   replayMessages    — arrived after a reconnect (delayed by it)
//
// The whole thing is opt-in (TRACE=1 / ?trace=1). Headline runner is
// untouched.
//
// Output: full per-cable trace gets written to a JSON file by the
// caller (bench-runner endpoint or local CLI), so downstream analysis
// can reconstruct anything we didn't think to aggregate.

import WebSocket from "ws";
import { createCable } from "@anycable/core";

import { ClientStat, JitterResult, newStat, recordMsg, summarize, percentile } from "./stats.js";
import type { JitterParams } from "./params.js";
import type { AnycableUrls } from "./jitter-runners.js";

import { settleAfterRamp } from "./timing.js";

interface JitterCycleTrace {
  terminateAt: number;
  cableConnectAt?: number;
  channelConnectAt?: number;
  firstReplayedMsgAt?: number;
  firstReplayedMsgSeq?: number;
}

interface CableTrace {
  cableIdx: number;
  jitterEvents: JitterCycleTrace[];
  messages: { ts: number; seq: number; sentAt?: number }[];
}

export interface PhaseStats {
  count: number;
  p50: number;
  p95: number;
  p99: number;
  max: number;
}

export interface JitterAnycableTracedResult extends JitterResult {
  trace: {
    sampleSize: number;
    totalCables: number;
    durationSec: number;
    aggregates: {
      reconnectMs: PhaseStats;
      channelResubMs: PhaseStats;
      replayLagMs: PhaseStats;
    };
    perMessageLatency: {
      // Latency split by whether the message arrived during steady-state
      // or after being caught in a reconnect window.
      direct: PhaseStats;
      replay: PhaseStats;
      directShareOfTotal: number;
    };
    cables: CableTrace[];
  };
}

function statsOf(values: number[]): PhaseStats {
  const sorted = values.slice().sort((a, b) => a - b);
  return {
    count: sorted.length,
    p50: percentile(sorted, 50),
    p95: percentile(sorted, 95),
    p99: percentile(sorted, 99),
    max: percentile(sorted, 100),
  };
}

function terminateCableWs(cable: ReturnType<typeof createCable>): boolean {
  const transport = (cable as unknown as { transport?: unknown }).transport as
    | { ws?: WebSocket & { terminate?: () => void; close?: () => void } }
    | undefined;
  const raw = transport?.ws;
  if (!raw) return false;
  if (typeof raw.terminate === "function") {
    raw.terminate();
    return true;
  }
  if (typeof raw.close === "function") {
    raw.close();
    return true;
  }
  return false;
}

let suppressed = false;
function suppressClientRejections() {
  if (suppressed) return;
  suppressed = true;
  process.on("unhandledRejection", () => {});
}

async function publish(opts: {
  url: string;
  secret?: string;
  total: number;
  intervalMs: number;
  stream: string;
}) {
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (opts.secret) headers["Authorization"] = `Bearer ${opts.secret}`;
  for (let seq = 1; seq <= opts.total; seq++) {
    const data = JSON.stringify({ seq, sentAt: Date.now(), text: `m${seq}` });
    try {
      await fetch(opts.url, {
        method: "POST",
        headers,
        body: JSON.stringify({ stream: opts.stream, data }),
      });
    } catch {
      /* skip — surface via lostDeliveries */
    }
    await new Promise((r) => setTimeout(r, opts.intervalMs));
  }
}

export async function runJitterAnycableTraced(
  p: JitterParams,
  urls: AnycableUrls,
  opts: { traceSample?: number } = {}
): Promise<JitterAnycableTracedResult> {
  suppressClientRejections();
  // Sample a subset of cables for full per-event tracing. Default: 100.
  // 10K × full trace would be ~10 MB of JSON; 100 cables × ~10 events
  // each ≈ a few KB.
  const sampleSize = Math.min(p.n, opts.traceSample ?? 100);
  console.log(`[jitter-ac-traced] params=${JSON.stringify(p)} traceSample=${sampleSize}`);

  const startedAt = Date.now();
  const stats: ClientStat[] = [];
  const cables: ReturnType<typeof createCable>[] = [];
  // Sparse: only the first `sampleSize` cables get a trace object.
  const traces: (CableTrace | null)[] = [];

  for (let i = 0; i < p.n; i++) {
    const stat = newStat();
    stats.push(stat);
    const trace: CableTrace | null =
      i < sampleSize
        ? { cableIdx: i, jitterEvents: [], messages: [] }
        : null;
    traces.push(trace);

    const cable = createCable(urls.cableUrl, {
      websocketImplementation: WebSocket as unknown as typeof globalThis.WebSocket,
      protocol: "actioncable-v1-ext-json",
      logLevel: "error" as never,
    });

    // Cable-level reconnect: this fires when the cable's transport
    // re-establishes the WebSocket and the AnyCable session is back.
    // Note: `ev.reconnect` is true on automatic reconnects; the very
    // first connect during ramp doesn't carry it. We mark
    // `stat.everConnected` on every connect so the connected-only
    // delivery rate captures clients that came up at any point.
    cable.on("connect", (ev?: { reconnect?: boolean }) => {
      stat.everConnected = true;
      if (!trace) return;
      if (!ev || !ev.reconnect) return;
      const ts = Date.now() - startedAt;
      const last = trace.jitterEvents[trace.jitterEvents.length - 1];
      if (last && last.cableConnectAt === undefined) {
        last.cableConnectAt = ts;
      }
    });

    const channel = cable.streamFrom(p.stream);
    channel.on("message", (msg: unknown, meta?: unknown) => {
      recordMsg(stat, msg);
      if (!trace) return;
      const ts = Date.now() - startedAt;
      // Message payload is the JSON we send: {seq, sentAt, text}
      let seq = -1;
      let sentAt: number | undefined = undefined;
      if (typeof msg === "object" && msg !== null) {
        const m = msg as { seq?: number; sentAt?: number };
        if (typeof m.seq === "number") seq = m.seq;
        if (typeof m.sentAt === "number") sentAt = m.sentAt;
      }
      trace.messages.push({ ts, seq, sentAt });
      // If the most recent jitter event hasn't logged its first replayed
      // message yet, this is it.
      const last = trace.jitterEvents[trace.jitterEvents.length - 1];
      if (last && last.channelConnectAt !== undefined && last.firstReplayedMsgAt === undefined) {
        last.firstReplayedMsgAt = ts;
        last.firstReplayedMsgSeq = seq;
      }
    });
    if (trace) {
      // Channel-level reconnect: fires after the cable reconnects AND
      // the channel resumes (history confirmed or new subscribe).
      channel.on("connect", (ev?: { reconnect?: boolean }) => {
        if (!ev || !ev.reconnect) return;
        const ts = Date.now() - startedAt;
        const last = trace.jitterEvents[trace.jitterEvents.length - 1];
        if (last && last.channelConnectAt === undefined) {
          last.channelConnectAt = ts;
        }
      });
    }

    cables.push(cable);

    if ((i + 1) % p.rampPerSec === 0) {
      await new Promise((r) => setTimeout(r, 1000));
      if ((i + 1) % 1000 === 0) {
        console.log(`[jitter-ac-traced] ramped ${i + 1}/${p.n}`);
      }
    }
  }

  await settleAfterRamp();
  console.log(`[jitter-ac-traced] all ramped; starting publisher and jitter loop`);

  const publishTask = publish({
    url: urls.broadcastUrl,
    secret: urls.broadcastSecret,
    total: p.totalMessages,
    intervalMs: p.intervalMs,
    stream: p.stream,
  });

  const endAt = Date.now() + p.durationSec * 1000;
  const jitterTasks = cables.map((cable, i) =>
    (async () => {
      const stat = stats[i];
      const trace = traces[i];
      let next = Date.now() + (5 + Math.random() * p.jitterIntervalSec) * 1000;
      while (Date.now() < endAt) {
        if (Date.now() >= next) {
          stat.jitterCount++;
          if (trace) {
            trace.jitterEvents.push({ terminateAt: Date.now() - startedAt });
          }
          terminateCableWs(cable);
          await new Promise((r) => setTimeout(r, p.jitterDurationMs));
          next = Date.now() + (p.jitterIntervalSec + Math.random() * 5) * 1000;
        }
        await new Promise((r) => setTimeout(r, 500));
      }
    })()
  );

  await Promise.all([publishTask, ...jitterTasks]);

  for (const c of cables) {
    try {
      c.disconnect();
    } catch {
      /* tear-down errors are not interesting */
    }
  }

  // -------------------------------------------------------------------------
  // Aggregate the trace data.

  const reconnectMs: number[] = [];
  const channelResubMs: number[] = [];
  const replayLagMs: number[] = [];
  // Per-message latency, split direct vs replay.
  const directLatencyMs: number[] = [];
  const replayLatencyMs: number[] = [];

  for (const trace of traces) {
    if (!trace) continue;
    for (const ev of trace.jitterEvents) {
      if (ev.cableConnectAt !== undefined) {
        reconnectMs.push(ev.cableConnectAt - ev.terminateAt);
        if (ev.channelConnectAt !== undefined) {
          channelResubMs.push(ev.channelConnectAt - ev.cableConnectAt);
          if (ev.firstReplayedMsgAt !== undefined) {
            replayLagMs.push(ev.firstReplayedMsgAt - ev.channelConnectAt);
          }
        }
      }
    }
    // Classify each message: was the receive timestamp inside a
    // disconnect → reconnect window for this cable? If the message
    // arrived after channelConnectAt of a jitter event but before the
    // next jitter terminate, count it as "replay". Otherwise direct.
    for (const m of trace.messages) {
      if (m.sentAt === undefined) continue;
      const latency = (m.ts + startedAt) - m.sentAt;
      let inReplay = false;
      for (const ev of trace.jitterEvents) {
        if (ev.channelConnectAt === undefined) continue;
        // Message landed within ~5s of channelConnect — treat as replayed.
        // Tighter than "before next jitter" because there could be a long
        // gap; we want messages that look like they came back via replay.
        const delta = m.ts - ev.channelConnectAt;
        if (delta >= 0 && delta < 5000) {
          inReplay = true;
          break;
        }
      }
      if (inReplay) replayLatencyMs.push(latency);
      else directLatencyMs.push(latency);
    }
  }

  const headline = summarize({
    label: "anycable-traced",
    totalMessages: p.totalMessages,
    stats,
    elapsedMs: Date.now() - startedAt,
    peakRssMb: Math.round(process.memoryUsage().rss / 1024 / 1024),
  });

  const directShareOfTotal =
    directLatencyMs.length + replayLatencyMs.length > 0
      ? directLatencyMs.length / (directLatencyMs.length + replayLatencyMs.length)
      : 0;

  const result: JitterAnycableTracedResult = {
    ...headline,
    trace: {
      sampleSize,
      totalCables: p.n,
      durationSec: p.durationSec,
      aggregates: {
        reconnectMs: statsOf(reconnectMs),
        channelResubMs: statsOf(channelResubMs),
        replayLagMs: statsOf(replayLagMs),
      },
      perMessageLatency: {
        direct: statsOf(directLatencyMs),
        replay: statsOf(replayLatencyMs),
        directShareOfTotal: Number(directShareOfTotal.toFixed(4)),
      },
      cables: traces.filter((t): t is CableTrace => t !== null),
    },
  };

  console.log(
    `[jitter-ac-traced] reconnect p50/p95/p99=${result.trace.aggregates.reconnectMs.p50}/${result.trace.aggregates.reconnectMs.p95}/${result.trace.aggregates.reconnectMs.p99}ms ` +
      `replayLag p50/p95=${result.trace.aggregates.replayLagMs.p50}/${result.trace.aggregates.replayLagMs.p95}ms ` +
      `direct/replay=${directLatencyMs.length}/${replayLatencyMs.length}`
  );

  return result;
}
