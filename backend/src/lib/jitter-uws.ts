// Jitter runner against the uWebSockets.js server.
//
// uWS itself is just the WS layer — there's no client library and no
// replay/CSR-equivalent. To simulate a "typical hand-rolled uWS client"
// we use the `ws` package with a small reconnecting wrapper:
//
//   - exponential backoff 500ms / 1s / 2s / 4s, capped at 5s, +/-25% jitter
//   - re-send the {type:"subscribe"} frame on every reconnect
//   - no buffering: messages broadcast during the offline window are gone
//
// This is deliberately representative of the founder's setup (uWS embedded
// in the main app, no replay) — the diagnostic point is that a faster
// WebSocket layer doesn't restore lost messages on its own.
//
// Same publish path as socketio runners: by default the runner kicks off
// /publish-local on the uWS server (in-process publishing, the realistic
// path); falls back to HTTP /_broadcast for parity.

import WebSocket from "ws";

import { ClientStat, JitterResult, newStat, recordMsg, summarize } from "./core/stats.js";
import { trackPeakRss } from "./core/peak-rss.js";
import { log } from "./core/log.js";
import { settleAfterRamp } from "./core/timing.js";
import type { JitterParams } from "./core/params.js";

let suppressed = false;
function suppressClientRejections() {
  if (suppressed) return;
  suppressed = true;
  process.on("unhandledRejection", () => {});
}

async function maybePauseForRamp(p: JitterParams, i: number, label: string) {
  if ((i + 1) % p.rampPerSec === 0) {
    await new Promise((r) => setTimeout(r, 1000));
    if ((i + 1) % 1000 === 0) log.debug(`[${label}] ramped ${i + 1}/${p.n}`);
  }
}

// Backoff matched to socket.io-client's reconnect machinery
// (reconnectionDelay 2000, reconnectionDelayMax 5000) so the uWS
// reconnect storm doesn't get an unfair head-start from a tighter
// initial retry. Anything tighter than this — including @anycable/core's
// Monitor — is in the same 2s ballpark, so this gives a clean
// apples-to-apples comparison on jitter.
const RECONNECT_BASE_MS = 2000;
const RECONNECT_MAX_MS = 5000;

// Minimal reconnecting client. Exposes `terminate()` for the jitter loop and
// re-sends {type:"subscribe"} on every successful (re)connect.
class ReconnectingWs {
  private ws: WebSocket | null = null;
  private closed = false;
  private attempts = 0;
  private reconnectTimer: NodeJS.Timeout | null = null;

  constructor(
    private url: string,
    private topic: string,
    private onMessage: (msg: string) => void,
    private onFailedConnect: () => void,
    private onOpen?: () => void,
  ) {
    this.connect();
  }

  private connect() {
    if (this.closed) return;
    let ws: WebSocket;
    try {
      ws = new WebSocket(this.url);
    } catch {
      this.onFailedConnect();
      this.scheduleReconnect();
      return;
    }
    this.ws = ws;
    ws.on("open", () => {
      this.attempts = 0;
      this.onOpen?.();
      try {
        ws.send(JSON.stringify({ type: "subscribe", topic: this.topic }));
      } catch {
        /* close will follow */
      }
    });
    ws.on("message", (data) => {
      this.onMessage(data.toString("utf-8"));
    });
    ws.on("error", () => {
      // Don't increment failedConnects on error: 'close' will follow and
      // we count the cycle there. ws emits both for connection failures.
    });
    ws.on("close", () => {
      if (this.closed) return;
      this.scheduleReconnect();
    });
    ws.on("unexpected-response", () => {
      this.onFailedConnect();
    });
  }

  private scheduleReconnect() {
    if (this.closed) return;
    if (this.reconnectTimer) return;
    this.attempts++;
    const base = Math.min(
      RECONNECT_BASE_MS * Math.pow(2, this.attempts - 1),
      RECONNECT_MAX_MS
    );
    // ±25% jitter so 10K clients don't all retry on the same tick
    const delay = base * (0.75 + Math.random() * 0.5);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, delay);
  }

  // Returns true if there was a live socket to sever. False means the
  // wrapper was already mid-reconnect / pre-open — caller should not count
  // the event as a real jitter event (no disruption actually occurred).
  terminate(): boolean {
    if (!this.ws) return false;
    const live = this.ws.readyState === this.ws.OPEN ||
      this.ws.readyState === this.ws.CONNECTING;
    if (typeof this.ws.terminate === "function") this.ws.terminate();
    else this.ws.close();
    return live;
  }

  close() {
    this.closed = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    try {
      this.ws?.close();
    } catch {
      /* tear-down */
    }
  }
}

export interface UwsUrls {
  // ws:// URL, e.g. ws://uws-server.railway.internal:3000/ws
  serverWsUrl: string;
  // http:// URL of the uWS server's HTTP surface, e.g.
  // http://uws-server.railway.internal:3000 — used to kick the publisher.
  serverHttpUrl: string;
  // Default: per-message HTTP POST to /_broadcast (same as AnyCable). Set
  // to `true` to delegate publishing to the uWS server's /publish-local
  // endpoint (one HTTP trigger, then a 100-message app.publish() loop
  // inside the server process) — useful for isolating in-process fan-out
  // cost.
  publishViaServer?: boolean;
}

async function startUwsPublishing(p: JitterParams, urls: UwsUrls): Promise<void> {
  if (urls.publishViaServer !== true) {
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    for (let seq = 1; seq <= p.totalMessages; seq++) {
      const data = JSON.stringify({ seq, sentAt: Date.now(), text: `m${seq}` });
      try {
        await fetch(`${urls.serverHttpUrl}/_broadcast`, {
          method: "POST",
          headers,
          body: JSON.stringify({ stream: p.stream, data }),
        });
      } catch {
        /* lost individual broadcasts surface in delivery rate */
      }
      await new Promise((r) => setTimeout(r, p.intervalMs));
    }
    return;
  }
  const qs = new URLSearchParams({
    total: String(p.totalMessages),
    interval: String(p.intervalMs),
    stream: p.stream,
  });
  try {
    await fetch(`${urls.serverHttpUrl}/publish-local?${qs.toString()}`, {
      method: "POST",
    });
  } catch {
    /* publish kickoff failure shows up as zero deliveries */
  }
  // /publish-local runs publishing async on the server; sleep so this task
  // tracks the same time window.
  await new Promise((r) => setTimeout(r, p.totalMessages * p.intervalMs));
}

export async function runJitterUws(
  p: JitterParams,
  urls: UwsUrls
): Promise<JitterResult> {
  suppressClientRejections();
  log.info(`[jitter-uws] params=${JSON.stringify(p)}`);
  const startedAt = Date.now();
  const rss = trackPeakRss();

  const stats: ClientStat[] = [];
  const clients: ReconnectingWs[] = [];

  for (let i = 0; i < p.n; i++) {
    const stat = newStat();
    stats.push(stat);
    const client = new ReconnectingWs(
      urls.serverWsUrl,
      p.stream,
      (text) => {
        let parsed: unknown;
        try {
          parsed = JSON.parse(text);
        } catch {
          return;
        }
        recordMsg(stat, parsed);
      },
      () => {
        stat.failedConnects++;
      },
      () => {
        stat.everConnected = true;
      },
    );
    clients.push(client);
    await maybePauseForRamp(p, i, "jitter-uws");
  }

  await settleAfterRamp();
  log.info(`[jitter-uws] all ramped; starting publisher and jitter loop`);

  const publishTask = startUwsPublishing(p, urls);

  const endAt = Date.now() + p.durationSec * 1000;
  const jitterTasks = clients.map((client, i) =>
    (async () => {
      const stat = stats[i];
      let next = Date.now() + (5 + Math.random() * p.jitterIntervalSec) * 1000;
      while (Date.now() < endAt) {
        if (Date.now() >= next) {
          // Force-close at the TCP layer — same semantics as the Socket.io
          // and AnyCable jitter tests. ReconnectingWs handles the bring-back
          // with its built-in backoff. Only count when a live socket
          // existed to sever; clients mid-reconnect would otherwise inflate
          // jitterEvents without a real disruption.
          if (client.terminate()) {
            stat.jitterCount++;
          }
          await new Promise((r) => setTimeout(r, p.jitterDurationMs));
          next = Date.now() + (p.jitterIntervalSec + Math.random() * 5) * 1000;
        }
        await new Promise((r) => setTimeout(r, 500));
      }
    })()
  );

  await Promise.all([publishTask, ...jitterTasks]);

  for (const c of clients) {
    try {
      c.close();
    } catch {
      /* tear-down */
    }
  }

  const peakRssMb = rss.stop();
  const result = summarize({
    label: "uws",
    totalMessages: p.totalMessages,
    stats,
    elapsedMs: Date.now() - startedAt,
    peakRssMb,
    samplesCap: p.samplesCap,
  });
  log.info(`[jitter-uws] result: ${JSON.stringify(result)}`);
  return result;
}
