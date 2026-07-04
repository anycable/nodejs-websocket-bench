// Client reconnect-backoff strategies, one explicit function per real client
// library, plus a shared "tuned" profile. The jitter/recovery test runs in two
// modes so we can separate a client-library default from a server's actual
// resume/replay capability:
//
//   DEFAULT — each server driven by its stock client's real reconnect backoff.
//             This is what users actually experience out of the box.
//   TUNED   — every client uses one uniform aggressive-but-storm-safe backoff,
//             so the recovery tail reflects the SERVER's resume speed, not the
//             client's stock delay.
//
// The recovery-tail "p99" a jitter run reports is roughly (offline window) +
// (client reconnect delay) + (server replay). At scale the reconnect delay
// dominates, so which backoff the client ships is the single biggest lever.
//
// Per Vladimir's PR review: express these as explicit named functions rather
// than reparametrizing `backoffWithJitter` to fake a specific client (which
// gets cryptic). Each function is `(attempts) => delayMs`.

import { backoffWithJitter } from "@anycable/core";

export type ReconnectMode = "default" | "tuned";

// Default base delay (ms) for the tuned profile's first reconnect attempt.
export const TUNED_BASE_MS = 500;

// @rails/actioncable — a direct port of connection_monitor.js `getPollInterval`
// (staleThreshold = 6 s, reconnectionBackoffRate = 0.15). First reconnect
// floors around 6 s, the slowest stock reconnect in the comparison, which is
// why Action Cable's out-of-box recovery tail is the longest.
// Ref: rails/rails actioncable/app/javascript/action_cable/connection_monitor.js
export function actionCableStrategy(attempts: number): number {
  const staleThreshold = 6;
  const reconnectionBackoffRate = 0.15;
  const backoff = Math.pow(1 + reconnectionBackoffRate, Math.min(attempts, 10));
  const jitterMax = attempts === 0 ? 1.0 : reconnectionBackoffRate;
  const jitter = jitterMax * Math.random();
  return staleThreshold * 1000 * backoff * (1 + jitter);
}

// @anycable/core — the library's built-in default is backoffWithJitter with the
// ping interval (3000 ms) as the base: first reconnect ~1.5–9 s (centered ~4.5 s).
// Exposed as a named strategy so the DEFAULT mode sets it explicitly.
export const anycableDefaultStrategy = backoffWithJitter(3000);

// centrifuge-js — full-jitter, min 500 ms / max 20000 ms: first reconnect
// ~500–1000 ms, the most aggressive stock default. Centrifugo is not a target
// in this repo currently; kept for reference and reuse if it's re-added.
export function centrifugeStrategy(attempts: number): number {
  const min = 500;
  const max = 20000;
  const cap = Math.min(max, min * Math.pow(2, Math.min(attempts, 31)));
  return Math.min(max, min + Math.floor(Math.random() * cap));
}

// Shared TUNED / "best" profile, applied uniformly to every client. Aggressive
// first attempt (~baseMs) with x2 growth capped at 5 s and FULL jitter — fast
// enough to shrink the resume tail, but jittered + capped so a large fleet does
// not reconnect in lockstep (the reconnect storm the conservative stock
// defaults deliberately avoid; verify it holds at the test's scale).
export function makeTunedStrategy(baseMs: number = TUNED_BASE_MS): (attempts: number) => number {
  const max = 5000;
  return (attempts: number): number => {
    const cap = Math.min(max, baseMs * Math.pow(2, Math.min(attempts, 10)));
    return Math.min(max, baseMs + Math.random() * cap);
  };
}
