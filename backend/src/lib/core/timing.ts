// Shared timing constants for the bench runners.
//
// Most runners follow the same three-phase shape:
//   1. ramp subscribers at `rampPerSec` per second
//   2. settle for SETTLE_AFTER_RAMP_MS so the slowest connector's first
//      message-handler binding has landed before publishing starts (and so
//      the server's subscriber index is fully populated)
//   3. publish / measure / drain
//
// The 5 s settle floor is empirical: at our scales (1K–50K subs per shard,
// 200/s ramp), the last connect handshake completes within ~2 s; we give
// it 3× margin because cheap. Don't shrink without measuring — too-short
// settle shows up as a low first-message delivery rate that's hard to
// diagnose from the result alone.

export const SETTLE_AFTER_RAMP_MS = 5000;

// Floor for the per-event offline window during jitter tests. Default
// Socket.io uses `reconnection: false` and the runner opens a fresh
// socket manually after the sleep, so without this floor its offline
// window would be just `jitterDurationMs` (1 s by default). The other
// configurations (CSR, AnyCable, uWS) are gated by their respective
// client library's reconnect backoff, which sits in the 2–5 s range.
// Pinning a 2 s floor on default Socket.io's manual fresh-socket path
// keeps the four configurations measured against the same disruption
// shape, so the delivery-rate comparison reflects protocol differences,
// not reconnect-delay differences.
export const MIN_OFFLINE_MS = 2000;

export function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

export function settleAfterRamp(): Promise<void> {
  return sleep(SETTLE_AFTER_RAMP_MS);
}
