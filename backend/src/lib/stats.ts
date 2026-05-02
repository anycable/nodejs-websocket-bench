// Shared stat collection + aggregation for the jitter benchmarks.
//
// Each client gets one ClientStat that records every message it received and
// any per-client events (jitter, CSR resume, connect failure). After a run,
// summarize() folds an array of ClientStats into a single JitterResult.

export interface ClientStat {
  received: Set<number>;
  highestSeq: number;
  jitterCount: number;
  recoveredCount: number; // CSR only — number of socket.recovered=true reconnects
  failedConnects: number;
  latencies: number[]; // receivedAt - sentAt per message, in ms
}

export function newStat(): ClientStat {
  return {
    received: new Set(),
    highestSeq: 0,
    jitterCount: 0,
    recoveredCount: 0,
    failedConnects: 0,
    latencies: [],
  };
}

// Mutates `stat` in place. Messages without a numeric `seq` are ignored.
export function recordMsg(stat: ClientStat, msg: unknown): void {
  if (!msg || typeof msg !== "object") return;
  const m = msg as { seq?: number; sentAt?: number };
  if (typeof m.seq !== "number") return;
  stat.received.add(m.seq);
  if (m.seq > stat.highestSeq) stat.highestSeq = m.seq;
  if (typeof m.sentAt === "number") stat.latencies.push(Date.now() - m.sentAt);
}

// Linear-interpolation percentile against a pre-sorted array.
function pct(sortedAsc: number[], p: number): number {
  if (sortedAsc.length === 0) return 0;
  const idx = Math.floor((sortedAsc.length - 1) * (p / 100));
  return sortedAsc[idx];
}

function avg(xs: number[]): number {
  if (xs.length === 0) return 0;
  return Math.round(xs.reduce((s, n) => s + n, 0) / xs.length);
}

export interface JitterResult {
  label: string;
  elapsedMs: number;
  clients: number;
  publishedMessages: number;
  expectedDeliveries: number;
  receivedDeliveries: number;
  lostDeliveries: number;
  deliveryRatePct: number;
  jitterEvents: number;
  avgJittersPerClient: number;
  csrResumes: number;
  csrResumeRatePct: number | null;
  connectFailures: number;
  // Raw latency includes wall-clock skew between publisher and clients.
  latencyRawMs: { avg: number; p50: number; p95: number; p99: number; max: number };
  // Min-normalized latency: subtract the minimum observed latency so the floor
  // is 0. Skew-resistant — useful when comparing variants that ran on
  // different hosts or against publishers in different processes.
  latencyOverMinMs: {
    avg: number;
    p50: number;
    p95: number;
    p99: number;
    max: number;
    skewFloor: number;
  };
  latencySamples: number;
  runnerPeakRssMb: number;
}

export interface SummarizeOptions {
  label: string;
  totalMessages: number;
  stats: ClientStat[];
  elapsedMs: number;
  peakRssMb?: number;
}

export function summarize(opts: SummarizeOptions): JitterResult {
  const { label, totalMessages, stats, elapsedMs } = opts;

  let received = 0;
  let lost = 0;
  let jitters = 0;
  let recovered = 0;
  let failedConnects = 0;
  const allLatencies: number[] = [];
  let maxSeq = 0;
  for (const s of stats) {
    received += s.received.size;
    lost += Math.max(0, s.highestSeq - s.received.size);
    jitters += s.jitterCount;
    recovered += s.recoveredCount;
    failedConnects += s.failedConnects;
    if (s.highestSeq > maxSeq) maxSeq = s.highestSeq;
    for (const v of s.latencies) allLatencies.push(v);
  }

  // We use the configured `totalMessages`, not maxSeq — clients that lost the
  // tail wouldn't move maxSeq even if they should have received those packets.
  const expected = totalMessages * stats.length;
  const deliveryRate = expected > 0 ? (received / expected) * 100 : 0;

  allLatencies.sort((a, b) => a - b);
  const lmin = allLatencies.length > 0 ? allLatencies[0] : 0;
  const norm = allLatencies.map((v) => v - lmin);

  const peakRssMb =
    opts.peakRssMb !== undefined
      ? Math.round(opts.peakRssMb)
      : Math.round(process.memoryUsage().rss / 1024 / 1024);

  return {
    label,
    elapsedMs,
    clients: stats.length,
    publishedMessages: maxSeq,
    expectedDeliveries: expected,
    receivedDeliveries: received,
    lostDeliveries: lost,
    deliveryRatePct: Number(deliveryRate.toFixed(2)),
    jitterEvents: jitters,
    avgJittersPerClient: Number((jitters / Math.max(1, stats.length)).toFixed(1)),
    csrResumes: recovered,
    csrResumeRatePct:
      jitters > 0 ? Number(((recovered / jitters) * 100).toFixed(1)) : null,
    connectFailures: failedConnects,
    latencyRawMs: {
      avg: avg(allLatencies),
      p50: pct(allLatencies, 50),
      p95: pct(allLatencies, 95),
      p99: pct(allLatencies, 99),
      max: pct(allLatencies, 100),
    },
    latencyOverMinMs: {
      avg: avg(norm),
      p50: pct(norm, 50),
      p95: pct(norm, 95),
      p99: pct(norm, 99),
      max: pct(norm, 100),
      skewFloor: lmin,
    },
    latencySamples: allLatencies.length,
    runnerPeakRssMb: peakRssMb,
  };
}

// Human-readable console output for the local CLI scripts. The bench-runner
// returns the JitterResult JSON directly; this format is for terminals only.
export function formatHumanReport(label: string, r: JitterResult): string {
  const lines = [
    "",
    `=== ${label} (${(r.elapsedMs / 1000).toFixed(1)}s) ===`,
    `Clients:           ${r.clients}`,
    `Messages sent:     ${r.publishedMessages}`,
    `Total jitters:     ${r.jitterEvents} (avg ${r.avgJittersPerClient} per client)`,
  ];
  if (r.csrResumeRatePct !== null) {
    lines.push(
      `CSR resumes:       ${r.csrResumes} / ${r.jitterEvents} (${r.csrResumeRatePct}%)`
    );
  }
  if (r.connectFailures > 0) {
    lines.push(`Connect failures:  ${r.connectFailures}`);
  }
  lines.push(
    `Messages received: ${r.receivedDeliveries}`,
    `Messages lost:     ${r.lostDeliveries}`,
    `Delivery rate:     ${r.deliveryRatePct}%`,
    `Latency raw (ms):  avg=${r.latencyRawMs.avg}  p50=${r.latencyRawMs.p50}  p95=${r.latencyRawMs.p95}  p99=${r.latencyRawMs.p99}  max=${r.latencyRawMs.max}  (n=${r.latencySamples})`,
    `Latency over min:  avg=${r.latencyOverMinMs.avg}  p50=${r.latencyOverMinMs.p50}  p95=${r.latencyOverMinMs.p95}  p99=${r.latencyOverMinMs.p99}  max=${r.latencyOverMinMs.max}  (skew floor=${r.latencyOverMinMs.skewFloor}ms)`,
    `Client peak RSS:   ${r.runnerPeakRssMb} MB`
  );
  return lines.join("\n");
}
