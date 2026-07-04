// Result self-flagging for multi-shard runs.
//
// Every retracted number in this project's history matched one of a small
// set of signatures: a saturated load generator, a dead or misconfigured
// target, a silently-401'd publisher, cross-clock skew, or a semantic bug
// (echo, duplicate counting). Runs used to be trusted until a human noticed
// one of these by staring at the output. This module encodes the signatures
// so every multi-shard driver prints them and the campaign can refuse to
// publish flagged results.
//
// Severity:
//   fatal — the run measured the wrong thing; do not publish, fix and rerun.
//   warn  — the run may be fine; re-check before publishing.

import type { JitterResult } from "./stats.js";

export type ValiditySeverity = "fatal" | "warn";

export interface ValidityFlag {
  severity: ValiditySeverity;
  code: string;
  message: string;
}

export interface ValidityContext {
  // Per-shard requested client count. Used to spot uniform shard ceilings.
  perShardN?: number;
  // Configured test duration; elapsed far beyond it means the runner's
  // event loop stalled (a 30s test taking 94s was the whispers signature).
  expectedDurationSec?: number;
}

const round2 = (n: number) => Math.round(n * 100) / 100;

// Checks that apply to a single JitterResult (per-shard or merged).
export function validateResult(
  r: JitterResult,
  ctx: ValidityContext = {},
): ValidityFlag[] {
  const flags: ValidityFlag[] = [];

  if (r.deliveryRatePct > 100.5) {
    flags.push({
      severity: "fatal",
      code: "delivery-over-100",
      message: `delivery ${r.deliveryRatePct}% exceeds 100%: sender echo or duplicate counting (the Centrifugo 111.11% bug class)`,
    });
  }

  if (r.clients > 0 && r.connectedClients === 0) {
    flags.push({
      severity: "fatal",
      code: "nothing-connected",
      message: `0/${r.clients} clients connected: target down or unreachable, not a performance result`,
    });
  }

  if (r.publishedMessages === 0 && r.clients > 0) {
    flags.push({
      severity: "fatal",
      code: "nothing-published",
      message:
        "publishedMessages=0 with clients connected: publisher misconfigured or silently 401'd (check ANYCABLE_BROADCAST_SECRET / BENCH_RUNNER_TOKEN on the shard)",
    });
  }

  if (r.latencyOverMinMs.skewFloor < 0) {
    flags.push({
      severity: "fatal",
      code: "negative-skew-floor",
      message: `skewFloor=${r.latencyOverMinMs.skewFloor}ms: sentAt and receivedAt came from different clocks; publish path is asymmetric`,
    });
  }

  if (
    ctx.expectedDurationSec &&
    r.elapsedMs > ctx.expectedDurationSec * 1000 * 1.5
  ) {
    flags.push({
      severity: "fatal",
      code: "elapsed-overrun",
      message: `elapsed ${round2(r.elapsedMs / 1000)}s vs configured ${ctx.expectedDurationSec}s: runner event loop saturated; latency and delivery are runner artifacts`,
    });
  }

  if (r.connectFailures > 0 && r.connectFailures >= r.clients * 0.01) {
    flags.push({
      severity: "warn",
      code: "connect-failures",
      message: `${r.connectFailures} connect failures (${round2((r.connectFailures / Math.max(1, r.clients)) * 100)}% of clients): check target health and shard port headroom`,
    });
  }

  // Arithmetic cross-foot: received + lost should not exceed expected by
  // more than rounding. A reconcile failure means the accounting is buggy,
  // and every derived percentage inherits the bug.
  if (
    r.expectedDeliveries > 0 &&
    r.receivedDeliveries > r.expectedDeliveries * 1.005
  ) {
    flags.push({
      severity: "fatal",
      code: "received-over-expected",
      message: `receivedDeliveries ${r.receivedDeliveries} exceeds expected ${r.expectedDeliveries}: duplicate delivery counting`,
    });
  }

  return flags;
}

// Checks that only make sense across shards.
export function validateShardSet(
  shardResults: JitterResult[],
  ctx: ValidityContext = {},
): ValidityFlag[] {
  const flags: ValidityFlag[] = [];
  if (shardResults.length < 2) return flags;

  // Uniform shard ceiling: every shard connected the same count, below what
  // was asked of it. That is the load generator's wall (ephemeral ports,
  // event loop), never the server's. The 600K idle cap (50 shards frozen at
  // exactly 12,002) and the 132K capacity cap (13 runners at ~10K) both
  // looked exactly like this.
  const connected = shardResults.map((r) => r.connectedClients ?? r.clients);
  const allEqual = connected.every((c) => c === connected[0]);
  if (
    allEqual &&
    ctx.perShardN !== undefined &&
    connected[0] < ctx.perShardN * 0.99
  ) {
    flags.push({
      severity: "fatal",
      code: "uniform-shard-ceiling",
      message: `every shard connected exactly ${connected[0]} of ${ctx.perShardN} requested: load-generator limit, not a server ceiling. Add shards or lower per-shard N; do not publish.`,
    });
  }

  // Identical anomaly across shards with healthy connects but zero publishes
  // on a subset: secret drift across the fleet.
  const silent = shardResults.filter(
    (r) => r.publishedMessages === 0 && (r.connectedClients ?? r.clients) > 0,
  );
  if (silent.length > 0 && silent.length < shardResults.length) {
    flags.push({
      severity: "fatal",
      code: "partial-publisher-silence",
      message: `${silent.length}/${shardResults.length} shards connected fine yet published nothing: secret missing on part of the fleet (the runners 14-50 bug class)`,
    });
  }

  return flags;
}

export function formatValidityReport(flags: ValidityFlag[]): string {
  if (flags.length === 0) return "Validity: no flags.";
  const lines = ["", "=== VALIDITY FLAGS ==="];
  for (const f of flags) {
    lines.push(`  [${f.severity.toUpperCase()}] ${f.code}: ${f.message}`);
  }
  const fatal = flags.filter((f) => f.severity === "fatal").length;
  if (fatal > 0) {
    lines.push(
      `  ${fatal} fatal flag(s): this run measured the rig or a misconfig, not the server. Fix and rerun before publishing.`,
    );
  }
  return lines.join("\n");
}

export function hasFatal(flags: ValidityFlag[]): boolean {
  return flags.some((f) => f.severity === "fatal");
}
