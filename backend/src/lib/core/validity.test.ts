import assert from "node:assert/strict";
import { test } from "node:test";

import type { JitterResult } from "./stats.js";
import { hasFatal, validateResult, validateShardSet } from "./validity.js";

function result(overrides: Partial<JitterResult> = {}): JitterResult {
  return {
    label: "test",
    elapsedMs: 100_000,
    clients: 250,
    connectedClients: 250,
    publishedMessages: 120,
    expectedDeliveries: 30_000,
    receivedDeliveries: 30_000,
    lostDeliveries: 0,
    deliveryRatePct: 100,
    deliveryRateOfConnectedPct: 100,
    jitterEvents: 0,
    avgJittersPerClient: 0,
    csrResumes: 0,
    csrResumeRatePct: null,
    connectFailures: 0,
    latencyRawMs: { avg: 10, p50: 8, p95: 20, p99: 30, max: 50 },
    latencyOverMinMs: { avg: 8, p50: 6, p95: 18, p99: 28, max: 48, skewFloor: 2 },
    latencySamples: 30_000,
    runnerPeakRssMb: 200,
    ...overrides,
  };
}

test("clean result produces no flags", () => {
  assert.deepEqual(validateResult(result()), []);
});

test("delivery over 100% is fatal (sender echo class)", () => {
  const flags = validateResult(result({ deliveryRatePct: 111.11 }));
  assert.ok(flags.some((f) => f.code === "delivery-over-100" && f.severity === "fatal"));
});

test("zero connected clients is fatal", () => {
  const flags = validateResult(result({ connectedClients: 0 }));
  assert.ok(flags.some((f) => f.code === "nothing-connected"));
});

test("zero published messages is fatal (silent 401 class)", () => {
  const flags = validateResult(result({ publishedMessages: 0 }));
  assert.ok(flags.some((f) => f.code === "nothing-published"));
});

test("negative skew floor is fatal (cross-clock class)", () => {
  const flags = validateResult(
    result({
      latencyOverMinMs: { avg: 8, p50: 6, p95: 18, p99: 28, max: 48, skewFloor: -3 },
    }),
  );
  assert.ok(flags.some((f) => f.code === "negative-skew-floor"));
});

test("elapsed far beyond configured duration is fatal (event-loop saturation)", () => {
  const flags = validateResult(result({ elapsedMs: 94_000 }), {
    expectedDurationSec: 30,
  });
  assert.ok(flags.some((f) => f.code === "elapsed-overrun"));
});

test("elapsed within 1.5x of configured duration passes", () => {
  const flags = validateResult(result({ elapsedMs: 200_000 }), {
    expectedDurationSec: 160,
  });
  assert.ok(!flags.some((f) => f.code === "elapsed-overrun"));
});

test("connect failures above 1% warn", () => {
  const flags = validateResult(result({ connectFailures: 10 }));
  const flag = flags.find((f) => f.code === "connect-failures");
  assert.ok(flag);
  assert.equal(flag.severity, "warn");
});

test("received above expected is fatal (duplicate counting)", () => {
  const flags = validateResult(
    result({ receivedDeliveries: 33_000, deliveryRatePct: 110 }),
  );
  assert.ok(flags.some((f) => f.code === "received-over-expected"));
});

test("uniform shard ceiling below requested N is fatal", () => {
  const shards = [
    result({ connectedClients: 12_002, clients: 12_002 }),
    result({ connectedClients: 12_002, clients: 12_002 }),
    result({ connectedClients: 12_002, clients: 12_002 }),
  ];
  const flags = validateShardSet(shards, { perShardN: 20_000 });
  assert.ok(flags.some((f) => f.code === "uniform-shard-ceiling"));
  assert.ok(hasFatal(flags));
});

test("uniform shard counts at requested N are fine", () => {
  const shards = [
    result({ connectedClients: 250 }),
    result({ connectedClients: 250 }),
  ];
  assert.deepEqual(validateShardSet(shards, { perShardN: 250 }), []);
});

test("partial publisher silence across the fleet is fatal (secret drift)", () => {
  const shards = [
    result(),
    result({ publishedMessages: 0 }),
    result({ publishedMessages: 0 }),
  ];
  const flags = validateShardSet(shards, { perShardN: 250 });
  assert.ok(flags.some((f) => f.code === "partial-publisher-silence"));
});
