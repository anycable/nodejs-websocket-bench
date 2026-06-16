// Latency tracing for AnyCable's broadcast pipeline.
//
// Decomposes the ~234 ms p99 we measure end-to-end into the phases we
// can observe from the bench-runner side:
//
//   T0  bench publishes (sentAt)
//        │  POST /_broadcast      ← http.broadcast span
//   T1  anycable-go acks fetch
//        │                        ← bench.wait-first span
//   T2  first WS client receives
//        │                        ← bench.fanout-tail span
//   T3  last WS client receives
//
// Wire format: OpenTelemetry data model + W3C trace context. The
// instrumentation produces real OTel-shaped spans now so that
// migrating to a real OTel collector later means swapping the exporter
// (jsonl file → OTLP HTTP) and adding more spans on the server side.
// We already emit `traceparent` headers on every /_broadcast POST;
// anycable-go just isn't picking them up yet (no OTel SDK in
// v1.6.14). When the anycable-go fork lands, server-side spans will
// attach to the same trace_id we already generate here.
//
// Span tree per broadcast (one trace per broadcast):
//
//   bench.broadcast              T0 → T3   root
//     ├─ http.broadcast          T0 → T1   client kind
//     ├─ bench.wait-first        T1 → T2   internal
//     └─ bench.fanout-tail       T2 → T3   internal
//
// Output: <runName>.jsonl, one span per line. To ship to a real OTel
// collector later: replace `appendSpan` body with an OTLP/HTTP export
// call. Span shape is already OTLP-compatible.
//
// Usage:
//   N=2500 BROADCASTS=2000 INTERVAL_MS=50 \
//   CABLE_URL=ws://anycable-go.railway.internal:8080/cable \
//   BROADCAST_URL=http://anycable-go.railway.internal:8080/_broadcast \
//   BROADCAST_KEY=<anycable-go's ANYCABLE_HTTP_BROADCAST_SECRET> \
//   OUTPUT=tmp/anycable-trace-{ts}.jsonl \
//     tsx src/bench/latency-trace-anycable.ts

import { appendFileSync, mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { performance } from "node:perf_hooks";
import { randomBytes } from "node:crypto";
import { WebSocket } from "ws";

import { createCable } from "@anycable/core";

import { percentile } from "../lib/core/stats.js";

// -----------------------------------------------------------------------------
// Config
// -----------------------------------------------------------------------------

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) {
    console.error(
      `${name} is required.\n` +
        "CABLE_URL, BROADCAST_URL, and BROADCAST_KEY all must be set.\n" +
        "BROADCAST_KEY must match anycable-go's ANYCABLE_HTTP_BROADCAST_SECRET.",
    );
    process.exit(1);
  }
  return v;
}
const cableUrl = requireEnv("CABLE_URL");
const broadcastUrl = requireEnv("BROADCAST_URL");
const broadcastKey = requireEnv("BROADCAST_KEY");

const N = parseInt(process.env.N || "2500", 10);
const BROADCASTS = parseInt(process.env.BROADCASTS || "2000", 10);
const INTERVAL_MS = parseInt(process.env.INTERVAL_MS || "50", 10);
const RAMP_PER_SEC = parseInt(process.env.RAMP_PER_SEC || "200", 10);
const STREAM = process.env.STREAM || `latency-trace-${Date.now()}`;
const RUN_NAME = process.env.RUN_NAME || `anycable-trace-${new Date().toISOString().replace(/[:.]/g, "-")}`;
const OUTPUT_PATH = process.env.OUTPUT || `tmp/${RUN_NAME}.jsonl`;

mkdirSync(dirname(OUTPUT_PATH), { recursive: true });

console.log(`AnyCable latency tracer (OpenTelemetry-shaped spans)`);
console.log(`  cable     = ${cableUrl}`);
console.log(`  broadcast = ${broadcastUrl}`);
console.log(`  N=${N}  broadcasts=${BROADCASTS}  interval=${INTERVAL_MS}ms`);
console.log(`  stream    = ${STREAM}`);
console.log(`  output    = ${OUTPUT_PATH}`);
console.log("");

// -----------------------------------------------------------------------------
// OpenTelemetry shape — W3C trace context + OTLP span data model.
//
// Anything in this section is intentionally aligned with the OTel JSON
// shape so a future exporter can ship these spans to a real collector
// without restructuring.
// -----------------------------------------------------------------------------

// W3C: 16-byte trace_id, 8-byte span_id, lowercase hex, no dashes.
function newTraceId(): string {
  return randomBytes(16).toString("hex");
}
function newSpanId(): string {
  return randomBytes(8).toString("hex");
}
function w3cTraceparent(traceId: string, spanId: string, sampled = true): string {
  // version-traceid-parentid-flags
  return `00-${traceId}-${spanId}-${sampled ? "01" : "00"}`;
}

// Span kind enum mirrors OTLP. We use CLIENT for the HTTP publish span,
// INTERNAL for everything we observe locally, and CONSUMER for receive
// spans (matches OpenTelemetry messaging semantic conventions).
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

// OTLP attribute shape — KeyValue with typed AnyValue.
interface AttributeValue {
  stringValue?: string;
  intValue?: string; // string-encoded int64
  doubleValue?: number;
  boolValue?: boolean;
}
interface Attribute {
  key: string;
  value: AttributeValue;
}

// OTLP-shaped Span. Matches the JSON form of an OTLP/HTTP Span ready to
// be wrapped in ResourceSpans→ScopeSpans→spans[] for OTLP export.
interface Span {
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

// `performance.now()` returns float ms since process origin. We convert
// to absolute Unix nanoseconds using a single origin sample, so all
// spans share a monotonic + comparable timebase even across thousands
// of broadcasts. Worth the one-time cost since real OTel exporters
// expect Unix nanos as strings.
const PERF_ORIGIN_MS = Date.now() - performance.now();
function unixNano(perfMs: number): string {
  // BigInt avoids precision loss when converting ms-since-origin to ns
  // since process start.
  const ns = BigInt(Math.round((PERF_ORIGIN_MS + perfMs) * 1_000_000));
  return ns.toString();
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

// One write per span. For the volume we generate (~4 spans × 2000
// broadcasts = 8000 lines) plain JSONL is fine; for orders-of-magnitude
// more we'd batch into a buffer.
function appendSpan(span: Span): void {
  appendFileSync(OUTPUT_PATH, JSON.stringify(span) + "\n");
}

// Common resource attributes attached to every span. Mirrors how a real
// OTel SDK would tag spans with service identification, so trace
// visualizers later (Jaeger / Tempo) group them correctly.
const RESOURCE_ATTRS: Attribute[] = [
  attr("service.name", "bench-runner-trace"),
  attr("service.version", "0.1"),
  attr("messaging.system", "anycable"),
];

// -----------------------------------------------------------------------------
// Per-broadcast state
// -----------------------------------------------------------------------------

// Each broadcast = one trace = up to four spans. We key by `seq` so the
// subscriber-side message handler can find its trace.
interface BroadcastTrace {
  seq: number;
  traceId: string;
  rootSpanId: string;
  publishStartPerfMs: number;       // T0
  fetchEndPerfMs?: number;          // T1
  fetchStatus?: number;
  firstReceivePerfMs?: number;      // T2
  lastReceivePerfMs?: number;       // T3
  receiveCount: number;
}

const traces = new Map<number, BroadcastTrace>();

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

function finalizeTrace(t: BroadcastTrace): void {
  if (
    t.fetchEndPerfMs === undefined ||
    t.firstReceivePerfMs === undefined ||
    t.lastReceivePerfMs === undefined
  ) {
    return; // missing data — drop, don't emit a half-trace
  }
  const baseAttrs: Attribute[] = [
    ...RESOURCE_ATTRS,
    attr("messaging.destination.name", STREAM),
    attr("messaging.message.id", String(t.seq)),
  ];

  // Root: covers the entire logical broadcast (T0 → T3).
  appendSpan({
    traceId: t.traceId,
    spanId: t.rootSpanId,
    name: "bench.broadcast",
    kind: SpanKind.INTERNAL,
    startTimeUnixNano: unixNano(t.publishStartPerfMs),
    endTimeUnixNano: unixNano(t.lastReceivePerfMs),
    attributes: [...baseAttrs, attr("bench.subscribers.expected", N), attr("bench.subscribers.received", t.receiveCount)],
    status: { code: StatusCode.OK },
  });

  // HTTP publish span (T0 → T1). CLIENT kind matches OTel semantic
  // conventions for outbound HTTP. Server-side broker.Store/fanout
  // spans would attach as children of this span when anycable-go grows
  // OTel — same trace_id, parent_span_id = httpSpanId.
  const httpSpanId = newSpanId();
  appendSpan({
    traceId: t.traceId,
    spanId: httpSpanId,
    parentSpanId: t.rootSpanId,
    name: "http.broadcast",
    kind: SpanKind.CLIENT,
    startTimeUnixNano: unixNano(t.publishStartPerfMs),
    endTimeUnixNano: unixNano(t.fetchEndPerfMs),
    attributes: [
      ...baseAttrs,
      attr("http.request.method", "POST"),
      attr("http.url", broadcastUrl),
      attr("http.response.status_code", t.fetchStatus ?? 0),
      attr("messaging.operation.type", "publish"),
    ],
    status: { code: t.fetchStatus === 200 ? StatusCode.OK : StatusCode.ERROR },
  });

  // Wait-for-first: T1 → T2, how long after the server acked the
  // publish did the first subscriber actually see the message?
  appendSpan({
    traceId: t.traceId,
    spanId: newSpanId(),
    parentSpanId: t.rootSpanId,
    name: "bench.wait-first",
    kind: SpanKind.INTERNAL,
    startTimeUnixNano: unixNano(t.fetchEndPerfMs),
    endTimeUnixNano: unixNano(t.firstReceivePerfMs),
    attributes: baseAttrs,
    status: { code: StatusCode.OK },
  });

  // Fanout tail: T2 → T3, how long did the server take to write the
  // remaining N-1 WS frames after the first one landed?
  appendSpan({
    traceId: t.traceId,
    spanId: newSpanId(),
    parentSpanId: t.rootSpanId,
    name: "bench.fanout-tail",
    kind: SpanKind.INTERNAL,
    startTimeUnixNano: unixNano(t.firstReceivePerfMs),
    endTimeUnixNano: unixNano(t.lastReceivePerfMs),
    attributes: [
      ...baseAttrs,
      attr("bench.subscribers.received", t.receiveCount),
    ],
    status: { code: StatusCode.OK },
  });
}

// -----------------------------------------------------------------------------
// Phase 1: ramp N subscribers
// -----------------------------------------------------------------------------

const cables: ReturnType<typeof createCable>[] = [];
const rampStart = performance.now();

for (let i = 0; i < N; i++) {
  const cable = createCable(cableUrl, {
    websocketImplementation: WebSocket as unknown as typeof globalThis.WebSocket,
    protocol: "actioncable-v1-ext-json",
    logLevel: "error" as never,
  });
  cable.on("close", () => {});
  cable.on("disconnect", () => {});
  const channel = cable.streamFrom(STREAM);
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

  if ((i + 1) % RAMP_PER_SEC === 0) {
    await new Promise((r) => setTimeout(r, 1000));
    process.stdout.write(`\r  ramped ${i + 1}/${N}`);
  }
}
process.stdout.write(`\r  ramped ${N}/${N}      \n`);

console.log(`  settling 5s before publishing...`);
await new Promise((r) => setTimeout(r, 5000));
console.log(`  ramp + settle took ${((performance.now() - rampStart) / 1000).toFixed(1)}s\n`);

// -----------------------------------------------------------------------------
// Phase 2: publish with traceparent propagation
// -----------------------------------------------------------------------------

console.log(`Publishing ${BROADCASTS} broadcasts at ${INTERVAL_MS}ms interval...`);
const publishStart = performance.now();

for (let seq = 1; seq <= BROADCASTS; seq++) {
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
    const resp = await fetch(broadcastUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${broadcastKey}`,
        // W3C Trace Context: anycable-go ignores this today (no OTel
        // SDK in v1.6.14), but the moment it adopts otelhttp it'll
        // pick this up and attach server-side spans to the same trace.
        traceparent: w3cTraceparent(traceId, rootSpanId, true),
      },
      body: JSON.stringify({ stream: STREAM, data }),
    });
    trace.fetchEndPerfMs = performance.now();
    trace.fetchStatus = resp.status;
  } catch (e) {
    traces.delete(seq);
    console.warn(`\n  seq=${seq} fetch failed: ${(e as Error).message}`);
  }

  if (seq % 200 === 0) process.stdout.write(`\r  published ${seq}/${BROADCASTS}`);
  if (INTERVAL_MS > 0) await new Promise((r) => setTimeout(r, INTERVAL_MS));
}
process.stdout.write(`\r  published ${BROADCASTS}/${BROADCASTS}      \n`);

console.log(`  draining 5s for tail receives...`);
await new Promise((r) => setTimeout(r, 5000));
console.log(
  `  publish + drain took ${((performance.now() - publishStart) / 1000).toFixed(1)}s\n`,
);

// -----------------------------------------------------------------------------
// Phase 3: emit spans
// -----------------------------------------------------------------------------

let emitted = 0;
let skipped = 0;
for (const trace of traces.values()) {
  if (
    trace.fetchEndPerfMs === undefined ||
    trace.firstReceivePerfMs === undefined ||
    trace.lastReceivePerfMs === undefined
  ) {
    skipped++;
    continue;
  }
  finalizeTrace(trace);
  emitted++;
}
console.log(`Spans emitted: ${emitted * 4} (${emitted} broadcasts × 4 spans)`);
console.log(`Skipped:       ${skipped} broadcasts missing data`);

// -----------------------------------------------------------------------------
// Phase 4: inline summary (analyzer lives in separate script)
// -----------------------------------------------------------------------------

interface Row {
  httpMs: number;        // T1 - T0
  waitFirstMs: number;   // T2 - T1
  fanoutTailMs: number;  // T3 - T2
  totalMs: number;       // T3 - T0
}
const rows: Row[] = [];
for (const t of traces.values()) {
  if (
    t.fetchEndPerfMs === undefined ||
    t.firstReceivePerfMs === undefined ||
    t.lastReceivePerfMs === undefined
  )
    continue;
  rows.push({
    httpMs: t.fetchEndPerfMs - t.publishStartPerfMs,
    waitFirstMs: t.firstReceivePerfMs - t.fetchEndPerfMs,
    fanoutTailMs: t.lastReceivePerfMs - t.firstReceivePerfMs,
    totalMs: t.lastReceivePerfMs - t.publishStartPerfMs,
  });
}

// Re-use the shared percentile implementation so this script and the
// bench-runner endpoint produce bit-identical numbers at p99.
function summarize(label: string, vs: number[]) {
  const sorted = [...vs].sort((a, b) => a - b);
  return {
    label,
    p50: percentile(sorted, 50),
    p95: percentile(sorted, 95),
    p99: percentile(sorted, 99),
    max: percentile(sorted, 100),
  };
}

const phases = [
  summarize("http.broadcast    ", rows.map((r) => r.httpMs)),
  summarize("bench.wait-first  ", rows.map((r) => r.waitFirstMs)),
  summarize("bench.fanout-tail ", rows.map((r) => r.fanoutTailMs)),
  summarize("bench.broadcast   ", rows.map((r) => r.totalMs)),
];

console.log("\n=== Per-phase percentiles (ms) ===");
console.log("  span                          p50      p95      p99      max");
for (const p of phases) {
  console.log(
    `  ${p.label}            ${p.p50.toFixed(1).padStart(7)}  ${p.p95.toFixed(1).padStart(7)}  ${p.p99.toFixed(1).padStart(7)}  ${p.max.toFixed(1).padStart(7)}`,
  );
}

const total = phases[3];
const httpP99 = phases[0].p99;
const waitP99 = phases[1].p99;
const tailP99 = phases[2].p99;
console.log("\n=== p99 attribution ===");
console.log(
  `  http.broadcast    / total   = ${total.p99 > 0 ? ((httpP99 / total.p99) * 100).toFixed(0) : "0"}%  (HTTP roundtrip + broker.Store + response write)`,
);
console.log(
  `  bench.wait-first  / total   = ${total.p99 > 0 ? ((waitP99 / total.p99) * 100).toFixed(0) : "0"}%  (server scheduling before fanout starts)`,
);
console.log(
  `  bench.fanout-tail / total   = ${total.p99 > 0 ? ((tailP99 / total.p99) * 100).toFixed(0) : "0"}%  (server writes ${N} WS frames)`,
);
console.log(`\nSpans on disk: ${OUTPUT_PATH}`);
console.log(
  `Inspect with:    npm run bench:trace:analyze -- ${OUTPUT_PATH}   (analyzer to come)`,
);

for (const c of cables) {
  try {
    c.disconnect();
  } catch {
    /* ignore */
  }
}
process.exit(0);
