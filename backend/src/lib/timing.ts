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

export function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

export function settleAfterRamp(): Promise<void> {
  return sleep(SETTLE_AFTER_RAMP_MS);
}
