// AnyCable latency tracer — reusable library form.
//
// Decomposes the end-to-end broadcast latency into four observable
// phases on the bench-runner side:
//
//   T0  bench publishes (sentAt)
//        │  POST /_broadcast      ← http.broadcast span
//   T1  anycable-go acks fetch
//        │                        ← bench.wait-first span
//   T2  first WS client receives
//        │                        ← bench.fanout-tail span
//   T3  last WS client receives
//
// Output spans use the OpenTelemetry OTLP/JSON data model so a future
// real-OTel migration is exporter-only, not instrumentation rework.
// W3C `traceparent` is propagated on every /_broadcast POST; the
// anycable-go side will attach server spans to the same trace_id once
// it grows an OTel SDK.
//
// To eliminate Starlink/laptop-origin noise, run this from inside
// Railway via the bench-runner /bench-trace-anycable endpoint — that
// keeps the publisher and the anycable-go target on the same private
// network.

import { performance } from "node:perf_hooks";
import { randomBytes } from "node:crypto";
import { WebSocket } from "ws";

import { createCable } from "@anycable/core";

export interface AnycableTraceParams {
  cableUrl: string;
  broadcastUrl: string;
  broadcastSecret?: string;
  n: number;
  broadcasts: number;
  intervalMs: number;
  rampPerSec: number;
  stream?: string;
  // Optional cap on the spans returned in the result. The full set of
  // spans is large (broadcasts × 4); the endpoint can subset before
  // returning to keep responses manageable. Set to 0 to omit spans
  // entirely.
  includeSpans?: boolean;
  // Optional log sink so the endpoint can stream progress lines into
  // the job log without coupling to a particular stdout.
  log?: (line: string) => void;
}

export interface PhaseStats {
  p50: number;
  p95: number;
  p99: number;
  max: number;
}

export interface AnycableTraceResult {
  config: Omit<AnycableTraceParams, "log">;
  measuredBroadcasts: number;
  skippedBroadcasts: number;
  perBroadcastDeliveryMean: number;
  perBroadcastDeliveryMin: number;
  phases: {
    httpBroadcast: PhaseStats;
    waitFirst: PhaseStats;
    fanoutTail: PhaseStats;
    total: PhaseStats;
  };
  // Attribution at p99 — what fraction of total p99 each phase covers.
  attribution: {
    httpBroadcastPct: number;
    waitFirstPct: number;
    fanoutTailPct: number;
  };
  // OTLP-shaped spans, optional. Caller decides whether to include
  // them in the HTTP response.
  spans?: Span[];
}

// -----------------------------------------------------------------------------
// OpenTelemetry data model (OTLP/JSON)
// -----------------------------------------------------------------------------

const SpanKind = {
  INTERNAL: 1,
  SERVER: 2,
  CLIENT: 3,
  PRODUCER: 4,
  CONSUMER: 5,
} as const;
type SpanKindValue = (typeof SpanKind)[keyof typeof SpanKind];

const StatusCode = { UNSET: 0, OK: 1, ERROR: 2 } as const;
type StatusCodeValue = (typeof StatusCode)[keyof typeof StatusCode];

interface AttributeValue {
  stringValue?: string;
  intValue?: string;
  doubleValue?: number;
  boolValue?: boolean;
}
interface Attribute {
  key: string;
  value: AttributeValue;
}
export interface Span {
  traceId: string;
  spanId: string;
  parentSpanId?: string;
  name: string;
  kind: SpanKindValue;
  startTimeUnixNano: string;
  endTimeUnixNano: string;
  attributes: Attribute[];
  status: { code: StatusCodeValue; message?: string };
}

function newTraceId(): string {
  return randomBytes(16).toString("hex");
}
function newSpanId(): string {
  return randomBytes(8).toString("hex");
}
function w3cTraceparent(traceId: string, spanId: string): string {
  return `00-${traceId}-${spanId}-01`;
}

function attr(key: string, value: string | number | boolean): Attribute {
  if (typeof value === "boolean") return { key, value: { boolValue: value } };
  if (typeof value === "number") {
    return Number.isInteger(value)
      ? { key, value: { intValue: String(value) } }
      : { key, value: { doubleValue: value } };
  }
  return { key, value: { stringValue: value } };
}

// -----------------------------------------------------------------------------
// Stats helpers
// -----------------------------------------------------------------------------

function pct(values: number[], p: number): number {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.floor(sorted.length * p));
  return sorted[idx];
}

function summarize(values: number[]): PhaseStats {
  return {
    p50: pct(values, 0.5),
    p95: pct(values, 0.95),
    p99: pct(values, 0.99),
    max: pct(values, 1.0),
  };
}

// -----------------------------------------------------------------------------
// The trace runner
// -----------------------------------------------------------------------------

export async function runAnycableTrace(
  params: AnycableTraceParams,
): Promise<AnycableTraceResult> {
  const log = params.log ?? ((s) => process.stdout.write(s + "\n"));
  const stream = params.stream ?? `anycable-trace-${Date.now()}`;
  const includeSpans = params.includeSpans ?? false;

  // perf_hooks gives sub-ms monotonic timing. We convert to Unix nanos
  // for OTLP output using a one-time origin sample.
  const PERF_ORIGIN_MS = Date.now() - performance.now();
  function unixNano(perfMs: number): string {
    return BigInt(Math.round((PERF_ORIGIN_MS + perfMs) * 1_000_000)).toString();
  }

  // ---------------------------------------------------------------------------
  // Phase 1: ramp N subscribers
  // ---------------------------------------------------------------------------

  interface BroadcastTrace {
    seq: number;
    traceId: string;
    rootSpanId: string;
    publishStartPerfMs: number;
    fetchEndPerfMs?: number;
    fetchStatus?: number;
    firstReceivePerfMs?: number;
    lastReceivePerfMs?: number;
    receiveCount: number;
  }

  const traces = new Map<number, BroadcastTrace>();
  const cables: ReturnType<typeof createCable>[] = [];
  const rampStart = performance.now();

  function recordReceive(seq: number, perfMs: number): void {
    const t = traces.get(seq);
    if (!t) return;
    if (t.firstReceivePerfMs === undefined || perfMs < t.firstReceivePerfMs) {
      t.firstReceivePerfMs = perfMs;
    }
    if (t.lastReceivePerfMs === undefined || perfMs > t.lastReceivePerfMs) {
      t.lastReceivePerfMs = perfMs;
    }
    t.receiveCount++;
  }

  for (let i = 0; i < params.n; i++) {
    const cable = createCable(params.cableUrl, {
      websocketImplementation: WebSocket as unknown as typeof globalThis.WebSocket,
      protocol: "actioncable-v1-ext-json",
      logLevel: "error" as never,
    });
    cable.on("close", () => {});
    cable.on("disconnect", () => {});
    const channel = cable.streamFrom(stream);
    channel.on("message", (msg: unknown) => {
      const t = performance.now();
      let seq: number | undefined;
      if (typeof msg === "string") {
        try {
          const parsed = JSON.parse(msg) as { seq?: number };
          seq = typeof parsed.seq === "number" ? parsed.seq : undefined;
        } catch {
          return;
        }
      } else if (msg && typeof msg === "object" && "seq" in msg) {
        const candidate = (msg as { seq?: unknown }).seq;
        if (typeof candidate === "number") seq = candidate;
      }
      if (seq === undefined) return;
      recordReceive(seq, t);
    });
    cables.push(cable);

    if ((i + 1) % params.rampPerSec === 0) {
      await new Promise((r) => setTimeout(r, 1000));
      log(`ramped ${i + 1}/${params.n}`);
    }
  }
  log(`ramped ${params.n}/${params.n}`);

  // Settling window so all sockets are actually subscribed before we
  // start publishing. Skipping this races against the slowest connector.
  await new Promise((r) => setTimeout(r, 5000));
  log(`ramp + settle took ${((performance.now() - rampStart) / 1000).toFixed(1)}s`);

  // ---------------------------------------------------------------------------
  // Phase 2: publish with traceparent propagation
  // ---------------------------------------------------------------------------

  const publishStart = performance.now();
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
  };
  if (params.broadcastSecret) {
    headers["Authorization"] = `Bearer ${params.broadcastSecret}`;
  }

  for (let seq = 1; seq <= params.broadcasts; seq++) {
    const traceId = newTraceId();
    const rootSpanId = newSpanId();
    const trace: BroadcastTrace = {
      seq,
      traceId,
      rootSpanId,
      publishStartPerfMs: performance.now(),
      receiveCount: 0,
    };
    traces.set(seq, trace);

    const data = JSON.stringify({ seq });
    try {
      const resp = await fetch(params.broadcastUrl, {
        method: "POST",
        headers: {
          ...headers,
          // W3C Trace Context: anycable-go ignores this today; when
          // the fork adds otelhttp it'll attach server-side spans to
          // the same trace_id we already generate here.
          traceparent: w3cTraceparent(traceId, rootSpanId),
        },
        body: JSON.stringify({ stream, data }),
      });
      trace.fetchEndPerfMs = performance.now();
      trace.fetchStatus = resp.status;
    } catch (e) {
      traces.delete(seq);
      log(`seq=${seq} fetch failed: ${(e as Error).message}`);
    }
    if (params.intervalMs > 0) {
      await new Promise((r) => setTimeout(r, params.intervalMs));
    }
    if (seq % 100 === 0) {
      log(`published ${seq}/${params.broadcasts}`);
    }
  }
  log(`published ${params.broadcasts}/${params.broadcasts}`);

  // Wait for the last broadcast's fanout tail to land.
  await new Promise((r) => setTimeout(r, 5000));
  log(`publish + drain took ${((performance.now() - publishStart) / 1000).toFixed(1)}s`);

  // ---------------------------------------------------------------------------
  // Phase 3: decompose + emit
  // ---------------------------------------------------------------------------

  const baseAttrs: Attribute[] = [
    attr("service.name", "bench-runner-trace"),
    attr("messaging.system", "anycable"),
    attr("messaging.destination.name", stream),
  ];

  const spans: Span[] = [];
  interface Row {
    httpMs: number;
    waitFirstMs: number;
    fanoutTailMs: number;
    totalMs: number;
    receivedBy: number;
  }
  const rows: Row[] = [];
  let skipped = 0;

  for (const t of traces.values()) {
    if (
      t.fetchEndPerfMs === undefined ||
      t.firstReceivePerfMs === undefined ||
      t.lastReceivePerfMs === undefined
    ) {
      skipped++;
      continue;
    }
    const httpMs = t.fetchEndPerfMs - t.publishStartPerfMs;
    const waitFirstMs = t.firstReceivePerfMs - t.fetchEndPerfMs;
    const fanoutTailMs = t.lastReceivePerfMs - t.firstReceivePerfMs;
    const totalMs = t.lastReceivePerfMs - t.publishStartPerfMs;
    rows.push({
      httpMs,
      waitFirstMs,
      fanoutTailMs,
      totalMs,
      receivedBy: t.receiveCount,
    });

    if (includeSpans) {
      const msgAttrs: Attribute[] = [
        ...baseAttrs,
        attr("messaging.message.id", String(t.seq)),
      ];
      spans.push({
        traceId: t.traceId,
        spanId: t.rootSpanId,
        name: "bench.broadcast",
        kind: SpanKind.INTERNAL,
        startTimeUnixNano: unixNano(t.publishStartPerfMs),
        endTimeUnixNano: unixNano(t.lastReceivePerfMs),
        attributes: [
          ...msgAttrs,
          attr("bench.subscribers.expected", params.n),
          attr("bench.subscribers.received", t.receiveCount),
        ],
        status: { code: StatusCode.OK },
      });
      const httpSpanId = newSpanId();
      spans.push({
        traceId: t.traceId,
        spanId: httpSpanId,
        parentSpanId: t.rootSpanId,
        name: "http.broadcast",
        kind: SpanKind.CLIENT,
        startTimeUnixNano: unixNano(t.publishStartPerfMs),
        endTimeUnixNano: unixNano(t.fetchEndPerfMs),
        attributes: [
          ...msgAttrs,
          attr("http.request.method", "POST"),
          attr("http.url", params.broadcastUrl),
          attr("http.response.status_code", t.fetchStatus ?? 0),
          attr("messaging.operation.type", "publish"),
        ],
        status: { code: t.fetchStatus === 200 ? StatusCode.OK : StatusCode.ERROR },
      });
      spans.push({
        traceId: t.traceId,
        spanId: newSpanId(),
        parentSpanId: t.rootSpanId,
        name: "bench.wait-first",
        kind: SpanKind.INTERNAL,
        startTimeUnixNano: unixNano(t.fetchEndPerfMs),
        endTimeUnixNano: unixNano(t.firstReceivePerfMs),
        attributes: msgAttrs,
        status: { code: StatusCode.OK },
      });
      spans.push({
        traceId: t.traceId,
        spanId: newSpanId(),
        parentSpanId: t.rootSpanId,
        name: "bench.fanout-tail",
        kind: SpanKind.INTERNAL,
        startTimeUnixNano: unixNano(t.firstReceivePerfMs),
        endTimeUnixNano: unixNano(t.lastReceivePerfMs),
        attributes: [
          ...msgAttrs,
          attr("bench.subscribers.received", t.receiveCount),
        ],
        status: { code: StatusCode.OK },
      });
    }
  }

  // Close cables so the caller's process can shut down cleanly.
  for (const c of cables) {
    try {
      c.disconnect();
    } catch {
      /* ignore */
    }
  }

  const phases = {
    httpBroadcast: summarize(rows.map((r) => r.httpMs)),
    waitFirst: summarize(rows.map((r) => r.waitFirstMs)),
    fanoutTail: summarize(rows.map((r) => r.fanoutTailMs)),
    total: summarize(rows.map((r) => r.totalMs)),
  };
  const totalP99 = phases.total.p99 || 1;
  const attribution = {
    httpBroadcastPct: (phases.httpBroadcast.p99 / totalP99) * 100,
    waitFirstPct: (phases.waitFirst.p99 / totalP99) * 100,
    fanoutTailPct: (phases.fanoutTail.p99 / totalP99) * 100,
  };
  const deliveryCounts = rows.map((r) => r.receivedBy);

  return {
    config: {
      cableUrl: params.cableUrl,
      broadcastUrl: params.broadcastUrl,
      broadcastSecret: params.broadcastSecret,
      n: params.n,
      broadcasts: params.broadcasts,
      intervalMs: params.intervalMs,
      rampPerSec: params.rampPerSec,
      stream,
      includeSpans,
    },
    measuredBroadcasts: rows.length,
    skippedBroadcasts: skipped,
    perBroadcastDeliveryMean:
      deliveryCounts.length === 0
        ? 0
        : deliveryCounts.reduce((a, b) => a + b, 0) / deliveryCounts.length,
    perBroadcastDeliveryMin: deliveryCounts.length ? Math.min(...deliveryCounts) : 0,
    phases,
    attribution,
    spans: includeSpans ? spans : undefined,
  };
}
