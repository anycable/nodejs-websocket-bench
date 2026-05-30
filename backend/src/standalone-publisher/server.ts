// Standalone publisher service.
//
// Long-running Express service that publishes sequential messages to a
// configured WebSocket service's HTTP /_broadcast endpoint. Deployed on
// Railway as its own service (`publisher`); the standalone deploy-impact
// test redeploys THIS service while WebSocket connections sit on the
// target WS service untouched.
//
// Differs from src/publisher.ts (one-shot CLI) by:
//  - Running continuously on container start
//  - Exposing /health for Railway probes
//  - Logging publish-loop progress + reconnect attempts
//
// Env vars:
//   WS_BROADCAST_URL  — target's /_broadcast endpoint (e.g.
//                        http://socketio-server.railway.internal:3000/_broadcast)
//   STREAM            — channel/stream name (default 'standalone-publisher')
//   INTERVAL_MS       — publish interval (default 500 = 2 msg/sec)
//   BROADCAST_SECRET  — optional Bearer auth (for AnyCable etc.)
//   START_DELAY_SEC   — wait before starting publish loop (so bench-runner
//                       can ramp clients first). Default 60s.
//   PORT              — HTTP port (default 8080)
//
// Operationally: when this service is restarted (via `railway redeploy`),
// publishing pauses for the container restart window. Bench-runner clients
// stay connected to the WS service throughout — that's the whole point.

import express from "express";
import http from "http";
import https from "https";

const wsBroadcastUrl = process.env.WS_BROADCAST_URL;
if (!wsBroadcastUrl) {
  console.error("WS_BROADCAST_URL is required");
  process.exit(1);
}
const stream = process.env.STREAM || "standalone-publisher";
const intervalMs = parseInt(process.env.INTERVAL_MS || "500", 10);
const broadcastSecret = process.env.BROADCAST_SECRET || "";
const startDelaySec = parseInt(process.env.START_DELAY_SEC || "60", 10);
const port = parseInt(process.env.PORT || "8080", 10);

const isHttps = wsBroadcastUrl.startsWith("https");
const agent = isHttps
  ? new https.Agent({ keepAlive: true, maxSockets: 4 })
  : new http.Agent({ keepAlive: true, maxSockets: 4 });

let publishedTotal = 0;
let publishErrors = 0;
let startedAt = 0;

async function publishOne(seq: number): Promise<void> {
  const data = JSON.stringify({ seq, sentAt: Date.now() });
  const body = JSON.stringify({ stream, data });
  const url = new URL(wsBroadcastUrl!);

  return new Promise((resolve) => {
    const mod = isHttps ? https : http;
    const req = mod.request(
      {
        hostname: url.hostname,
        port: url.port || (isHttps ? 443 : 80),
        path: url.pathname,
        method: "POST",
        agent,
        headers: {
          "Content-Type": "application/json",
          "Content-Length": Buffer.byteLength(body),
          ...(broadcastSecret ? { Authorization: `Bearer ${broadcastSecret}` } : {}),
        },
      },
      (res) => {
        res.resume();
        if (res.statusCode && res.statusCode >= 400) {
          publishErrors++;
          if (publishErrors % 50 === 1) {
            console.log(
              `[publisher] HTTP ${res.statusCode} on seq=${seq} (${publishErrors} total errors)`,
            );
          }
        }
        resolve();
      },
    );
    req.on("error", (err) => {
      publishErrors++;
      if (publishErrors % 50 === 1) {
        console.log(`[publisher] error seq=${seq}: ${err.message}`);
      }
      resolve();
    });
    req.write(body);
    req.end();
  });
}

async function publishLoop() {
  console.log(
    `[publisher] start-delay ${startDelaySec}s before first publish (so bench-runner can ramp)`,
  );
  await new Promise((r) => setTimeout(r, startDelaySec * 1000));
  startedAt = Date.now();
  console.log(`[publisher] loop start — interval=${intervalMs}ms target=${wsBroadcastUrl}`);
  let seq = 0;
  while (true) {
    seq++;
    publishedTotal = seq;
    await publishOne(seq);
    if (seq % 100 === 0) {
      const rateMsgSec = seq / ((Date.now() - startedAt) / 1000);
      console.log(
        `[publisher] seq=${seq} errors=${publishErrors} rate=${rateMsgSec.toFixed(1)} msg/sec`,
      );
    }
    await new Promise((r) => setTimeout(r, intervalMs));
  }
}

const app = express();
app.get("/health", (_req, res) =>
  res.json({
    status: "ok",
    mode: "standalone-publisher",
    target: wsBroadcastUrl,
    stream,
    intervalMs,
    publishedTotal,
    publishErrors,
    uptimeSec: startedAt > 0 ? Math.round((Date.now() - startedAt) / 1000) : 0,
  }),
);

app.listen(port, () => {
  console.log(`[publisher] HTTP /health listening on :${port}`);
  publishLoop().catch((err) => {
    console.error("[publisher] loop crashed:", err);
    process.exit(1);
  });
});
