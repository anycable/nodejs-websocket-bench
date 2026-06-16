// Unit tests for the stat aggregation that produces every headline number.
//
// Run with `npm test`. Uses Node's built-in test runner so we don't need
// to introduce a fourth tool (tsc + tsx + ws + vitest would be one too
// many for what this repo does).

import { describe, it } from "node:test";
import { strict as assert } from "node:assert";

import {
  avg,
  downsampleSorted,
  newStat,
  percentile,
  recordMsg,
  summarize,
  type ClientStat,
} from "./stats.js";

describe("percentile", () => {
  it("returns 0 for an empty array", () => {
    assert.equal(percentile([], 50), 0);
    assert.equal(percentile([], 99), 0);
  });

  it("returns the single value for a 1-element array", () => {
    assert.equal(percentile([42], 0), 42);
    assert.equal(percentile([42], 50), 42);
    assert.equal(percentile([42], 99), 42);
  });

  it("indexes 0..99 of a 100-element ramp at every decile", () => {
    const xs = Array.from({ length: 100 }, (_, i) => i);
    assert.equal(percentile(xs, 0), 0);
    assert.equal(percentile(xs, 50), 49);
    assert.equal(percentile(xs, 95), 94);
    assert.equal(percentile(xs, 99), 98);
    assert.equal(percentile(xs, 100), 99);
  });

  it("does not crash on p=100 (off-by-one guard)", () => {
    assert.equal(percentile([1, 2, 3, 4, 5], 100), 5);
  });

  it("assumes the input is sorted; does not sort on the caller's behalf", () => {
    // This is a contract the page numbers depend on: every caller passes
    // a pre-sorted view so they can reuse it across p50/p95/p99 calls
    // without paying for repeated sorts.
    assert.equal(percentile([5, 1, 3], 50), 1);
  });
});

describe("avg", () => {
  it("returns 0 for an empty array", () => {
    assert.equal(avg([]), 0);
  });

  it("rounds to the nearest integer (ms)", () => {
    assert.equal(avg([1, 2, 3, 4, 5]), 3);
    assert.equal(avg([1, 2]), 2); // 1.5 rounds to 2
    assert.equal(avg([1, 1, 2]), 1); // 1.33 rounds to 1
  });
});

describe("downsampleSorted", () => {
  it("returns an empty array for cap=0 or empty input", () => {
    assert.deepEqual(downsampleSorted([], 100), []);
    assert.deepEqual(downsampleSorted([1, 2, 3], 0), []);
  });

  it("returns a copy of the input when length <= cap", () => {
    const xs = [1, 2, 3];
    const out = downsampleSorted(xs, 100);
    assert.deepEqual(out, [1, 2, 3]);
    assert.notEqual(out, xs);
  });

  it("preserves the first and last elements", () => {
    const xs = Array.from({ length: 1000 }, (_, i) => i);
    const out = downsampleSorted(xs, 10);
    assert.equal(out[0], 0);
    assert.equal(out[out.length - 1], 999);
    assert.equal(out.length, 10);
  });

  it("preserves monotonic order", () => {
    const xs = Array.from({ length: 10000 }, (_, i) => i * 2);
    const out = downsampleSorted(xs, 100);
    for (let i = 1; i < out.length; i++) {
      assert.ok(out[i] >= out[i - 1], `out[${i}] < out[${i - 1}]`);
    }
  });

  it("preserves p99 within a sample of the bucket size", () => {
    // The multi-shard coordinator relies on this: downsampling 2.5M
    // samples to 5K should not move p99 by more than ~one bucket.
    const xs = Array.from({ length: 100000 }, (_, i) => i);
    const orig99 = percentile(xs, 99);
    const out = downsampleSorted(xs, 5000);
    const down99 = percentile(out, 99);
    // Original p99 = 98999, bucket spacing ~20. Difference should be tiny.
    assert.ok(Math.abs(orig99 - down99) < 30, `${orig99} vs ${down99}`);
  });
});

describe("recordMsg", () => {
  it("ignores non-object messages", () => {
    const s = newStat();
    recordMsg(s, null);
    recordMsg(s, "hello");
    recordMsg(s, 42);
    assert.equal(s.received.size, 0);
    assert.equal(s.latencies.length, 0);
  });

  it("ignores messages without a numeric seq", () => {
    const s = newStat();
    recordMsg(s, { sentAt: Date.now() });
    recordMsg(s, { seq: "not a number" });
    assert.equal(s.received.size, 0);
  });

  it("records seq into the received set + tracks highestSeq", () => {
    const s = newStat();
    recordMsg(s, { seq: 5 });
    recordMsg(s, { seq: 1 });
    recordMsg(s, { seq: 7 });
    assert.equal(s.received.size, 3);
    assert.equal(s.highestSeq, 7);
  });

  it("dedupes seqs (same packet arrives twice)", () => {
    const s = newStat();
    recordMsg(s, { seq: 3 });
    recordMsg(s, { seq: 3 });
    assert.equal(s.received.size, 1);
  });

  it("computes latency only when sentAt is a number", () => {
    const s = newStat();
    const t = Date.now() - 100;
    recordMsg(s, { seq: 1, sentAt: t });
    recordMsg(s, { seq: 2 }); // no sentAt
    assert.equal(s.latencies.length, 1);
    assert.ok(s.latencies[0] >= 100);
  });
});

function mkStat(opts: Partial<ClientStat> = {}): ClientStat {
  return { ...newStat(), ...opts };
}

describe("summarize", () => {
  it("returns 0 delivery rate when no clients connected (avoids divide-by-zero)", () => {
    const r = summarize({
      label: "empty",
      totalMessages: 100,
      stats: [],
      elapsedMs: 1000,
      peakRssMb: 50,
    });
    assert.equal(r.deliveryRatePct, 0);
    assert.equal(r.deliveryRateOfConnectedPct, null);
    assert.equal(r.clients, 0);
    assert.equal(r.connectedClients, 0);
  });

  it("computes 100% delivery when every client gets every message", () => {
    const stats = [0, 1, 2].map(() => {
      const s = mkStat({ everConnected: true });
      for (let seq = 1; seq <= 10; seq++) recordMsg(s, { seq });
      return s;
    });
    const r = summarize({
      label: "happy",
      totalMessages: 10,
      stats,
      elapsedMs: 1000,
      peakRssMb: 100,
    });
    assert.equal(r.clients, 3);
    assert.equal(r.connectedClients, 3);
    assert.equal(r.expectedDeliveries, 30);
    assert.equal(r.receivedDeliveries, 30);
    assert.equal(r.deliveryRatePct, 100);
    assert.equal(r.deliveryRateOfConnectedPct, 100);
    assert.equal(r.lostDeliveries, 0);
  });

  it("splits headline rate from connected-only rate when connect failures exist", () => {
    // 2 clients connected and delivered everything, 1 never connected.
    // Headline rate denominator = totalMessages * clients = 30, received = 20 → 66.67%.
    // Connected-only denominator = totalMessages * 2 = 20, received = 20 → 100%.
    const connected1 = mkStat({ everConnected: true });
    for (let seq = 1; seq <= 10; seq++) recordMsg(connected1, { seq });
    const connected2 = mkStat({ everConnected: true });
    for (let seq = 1; seq <= 10; seq++) recordMsg(connected2, { seq });
    const failed = mkStat({ failedConnects: 1 });

    const r = summarize({
      label: "split",
      totalMessages: 10,
      stats: [connected1, connected2, failed],
      elapsedMs: 1000,
      peakRssMb: 100,
    });
    assert.equal(r.clients, 3);
    assert.equal(r.connectedClients, 2);
    assert.equal(r.deliveryRatePct, 66.67);
    assert.equal(r.deliveryRateOfConnectedPct, 100);
    assert.equal(r.connectFailures, 1);
  });

  it("counts highestSeq - received for lost deliveries", () => {
    // Client received [1, 3, 5] but highestSeq = 5 → lost 2 messages (2 and 4).
    const s = mkStat({ everConnected: true });
    recordMsg(s, { seq: 1 });
    recordMsg(s, { seq: 3 });
    recordMsg(s, { seq: 5 });
    const r = summarize({
      label: "loss",
      totalMessages: 5,
      stats: [s],
      elapsedMs: 1000,
      peakRssMb: 50,
    });
    assert.equal(r.receivedDeliveries, 3);
    assert.equal(r.lostDeliveries, 2);
  });

  it("computes CSR resume rate as recovered / jitters", () => {
    const s = mkStat({
      everConnected: true,
      jitterCount: 10,
      recoveredCount: 9,
    });
    const r = summarize({
      label: "csr",
      totalMessages: 0,
      stats: [s],
      elapsedMs: 1000,
      peakRssMb: 50,
    });
    assert.equal(r.jitterEvents, 10);
    assert.equal(r.csrResumes, 9);
    assert.equal(r.csrResumeRatePct, 90);
  });

  it("returns null csrResumeRatePct when no jitter events were recorded", () => {
    const s = mkStat({ everConnected: true });
    const r = summarize({
      label: "no-jitter",
      totalMessages: 1,
      stats: [s],
      elapsedMs: 1000,
      peakRssMb: 50,
    });
    assert.equal(r.csrResumeRatePct, null);
  });

  it("computes min-normalized latency with skewFloor = min observed latency", () => {
    // Three latencies: 1000, 1010, 1100 ms. Min = 1000; norm = 0, 10, 100.
    const s = mkStat({ everConnected: true });
    s.latencies.push(1000, 1010, 1100);
    const r = summarize({
      label: "skew",
      totalMessages: 0,
      stats: [s],
      elapsedMs: 1000,
      peakRssMb: 50,
    });
    assert.equal(r.latencyRawMs.p50, 1010);
    assert.equal(r.latencyRawMs.max, 1100);
    assert.equal(r.latencyOverMinMs.p50, 10);
    assert.equal(r.latencyOverMinMs.max, 100);
    assert.equal(r.latencyOverMinMs.skewFloor, 1000);
  });

  it("includes downsampled sorted latencies when samplesCap is set", () => {
    const s = mkStat({ everConnected: true });
    for (let i = 0; i < 1000; i++) s.latencies.push(i);
    const r = summarize({
      label: "cap",
      totalMessages: 0,
      stats: [s],
      elapsedMs: 1000,
      peakRssMb: 50,
      samplesCap: 50,
    });
    assert.ok(r.latencySamplesSorted);
    assert.equal(r.latencySamplesSorted!.length, 50);
    assert.equal(r.latencySamplesSorted![0], 0);
    assert.equal(r.latencySamplesSorted![49], 999);
  });

  it("omits latencySamplesSorted when samplesCap is unset", () => {
    const s = mkStat({ everConnected: true });
    s.latencies.push(1, 2, 3);
    const r = summarize({
      label: "no-cap",
      totalMessages: 0,
      stats: [s],
      elapsedMs: 1000,
      peakRssMb: 50,
    });
    assert.equal(r.latencySamplesSorted, undefined);
  });
});
