// Railway-hosted bench-runner.
//
// Same source image as socketio-server, with SERVICE_ENTRY=bench-runner/server
// selecting this entry point. Targets default to *.railway.internal so client-
// side bottlenecks (NAT, dev-machine event loop) don't interfere with measuring
// server capacity at 10K+ scale.
//
// Endpoints:
//   POST /bench-jitter-anycable
//   POST /bench-jitter-socketio
//   POST /bench-jitter-socketio-csr
//
// Each runs synchronously and returns the full JitterResult JSON. Use a long
// `curl --max-time` when triggering. Console output is also captured by Railway.

import express from "express";

import { paramsFromQuery } from "../lib/params.js";
import {
  runJitterAnycable,
  runJitterSocketio,
  runJitterSocketioCsr,
} from "../lib/jitter-runners.js";
import { runJitterAnycableTraced } from "../lib/jitter-anycable-traced.js";
import { runIdleAnycable, runIdleSocketio, runIdleUws } from "../lib/idle-runner.js";
import { runAvalancheSocketio } from "../lib/avalanche-runner.js";
import { runJitterUws } from "../lib/jitter-uws.js";
import { runAvalancheUws } from "../lib/avalanche-uws.js";

const SOCKETIO_URL =
  process.env.SOCKETIO_URL || "http://socketio-server.railway.internal:3000";
const ANYCABLE_URL =
  process.env.ANYCABLE_URL || "ws://anycable-go.railway.internal:8080/cable";
const ANYCABLE_BROADCAST_URL =
  process.env.ANYCABLE_BROADCAST_URL ||
  "http://anycable-go.railway.internal:8080/_broadcast";
const ANYCABLE_BROADCAST_SECRET = process.env.ANYCABLE_BROADCAST_SECRET || "";
const UWS_WS_URL =
  process.env.UWS_WS_URL || "ws://uws-server.railway.internal:3000/ws";
const UWS_HTTP_URL =
  process.env.UWS_HTTP_URL || "http://uws-server.railway.internal:3000";

const app = express();
app.use(express.json());

app.get("/health", (_req, res) =>
  res.json({
    status: "ok",
    mode: "bench-runner",
    socketioUrl: SOCKETIO_URL,
    anycableUrl: ANYCABLE_URL,
  })
);

// `?cableUrl=` and `?broadcastUrl=` override the defaults so we can target
// either anycable-go (OSS) or anycable-go-pro within the same project.
app.post("/bench-jitter-anycable", async (req, res) => {
  const params = paramsFromQuery(req);
  const cableUrl = (req.query.cableUrl as string) || ANYCABLE_URL;
  const broadcastUrl = (req.query.broadcastUrl as string) || ANYCABLE_BROADCAST_URL;
  const result = await runJitterAnycable(params, {
    cableUrl,
    broadcastUrl,
    broadcastSecret: ANYCABLE_BROADCAST_SECRET || undefined,
  });
  res.json(result);
});

// Diagnostic-only: same disruption as /bench-jitter-anycable plus
// per-cable timeline tracing. Returns the same JitterResult shape
// (so callers stay compatible) with an extra `trace` field — for
// us to read offline. Sample size defaults to 100 to keep the
// payload small at 10K+ scale; override with ?traceSample=N.
app.post("/bench-jitter-anycable-traced", async (req, res) => {
  const params = paramsFromQuery(req);
  const cableUrl = (req.query.cableUrl as string) || ANYCABLE_URL;
  const broadcastUrl = (req.query.broadcastUrl as string) || ANYCABLE_BROADCAST_URL;
  const traceSample = parseInt((req.query.traceSample as string) || "100", 10);
  const result = await runJitterAnycableTraced(
    params,
    {
      cableUrl,
      broadcastUrl,
      broadcastSecret: ANYCABLE_BROADCAST_SECRET || undefined,
    },
    { traceSample }
  );
  res.json(result);
});

app.post("/bench-jitter-socketio", async (req, res) => {
  const params = paramsFromQuery(req);
  const result = await runJitterSocketio(params, { serverUrl: SOCKETIO_URL });
  res.json(result);
});

app.post("/bench-jitter-socketio-csr", async (req, res) => {
  const params = paramsFromQuery(req);
  const result = await runJitterSocketioCsr(params, { serverUrl: SOCKETIO_URL });
  res.json(result);
});

// uWebSockets.js jitter — `?wsUrl=` and `?httpUrl=` override defaults so
// the same bench-runner can target uws-server, uws-server-small, or any
// other uWS deployment over Railway's internal network.
app.post("/bench-jitter-uws", async (req, res) => {
  const params = paramsFromQuery(req);
  const wsUrl = (req.query.wsUrl as string) || UWS_WS_URL;
  const httpUrl = (req.query.httpUrl as string) || UWS_HTTP_URL;
  const result = await runJitterUws(params, {
    serverWsUrl: wsUrl,
    serverHttpUrl: httpUrl,
  });
  res.json(result);
});

// Synchronous idle-connection probe. To exceed the per-container outbound
// port limit (~64K), deploy multiple bench-runner instances and POST to
// each in parallel — each container has its own source IP and ephemeral
// port pool. See `bench/idle-multi.ts` for the coordinator.
//
// `?cableUrl=` overrides the default target so the same bench-runner can
// hit either anycable-go (OSS) or anycable-go-pro within the same project.
app.post("/bench-idle-anycable", async (req, res) => {
  const n = parseInt((req.query.n as string) || "10000", 10);
  const holdSec = parseInt((req.query.hold as string) || "60", 10);
  const rampPerSec = parseInt((req.query.ramp as string) || "200", 10);
  const stream = (req.query.stream as string) || "idle-probe";
  const shardLabel = (req.query.shard as string) || undefined;
  const cableUrl = (req.query.cableUrl as string) || ANYCABLE_URL;

  const result = await runIdleAnycable(
    { n, holdSec, rampPerSec, stream },
    cableUrl,
    shardLabel
  );
  res.json(result);
});

// Socket.io idle probe — same shape as the AnyCable variant. Useful for
// measuring where Node-based Socket.io tops out per single instance on
// the same hardware that anycable-go is benchmarked against.
//
// `?serverUrl=` overrides the default target so the same shard can hit
// either the existing socketio-server service or any other Socket.io
// endpoint over Railway's internal network.
app.post("/bench-idle-socketio", async (req, res) => {
  const n = parseInt((req.query.n as string) || "10000", 10);
  const holdSec = parseInt((req.query.hold as string) || "60", 10);
  const rampPerSec = parseInt((req.query.ramp as string) || "200", 10);
  const stream = (req.query.stream as string) || "idle-probe";
  const shardLabel = (req.query.shard as string) || undefined;
  const serverUrl = (req.query.serverUrl as string) || SOCKETIO_URL;

  const result = await runIdleSocketio(
    { n, holdSec, rampPerSec, stream },
    serverUrl,
    shardLabel
  );
  res.json(result);
});

// uWebSockets.js idle probe — same shape as the anycable/socketio
// variants. `?wsUrl=` overrides the default target.
app.post("/bench-idle-uws", async (req, res) => {
  const n = parseInt((req.query.n as string) || "10000", 10);
  const holdSec = parseInt((req.query.hold as string) || "60", 10);
  const rampPerSec = parseInt((req.query.ramp as string) || "200", 10);
  const stream = (req.query.stream as string) || "idle-probe";
  const shardLabel = (req.query.shard as string) || undefined;
  const wsUrl = (req.query.wsUrl as string) || UWS_WS_URL;

  const result = await runIdleUws(
    { n, holdSec, rampPerSec, stream },
    wsUrl,
    shardLabel
  );
  res.json(result);
});

// Avalanche probe — connect N socket.io-client sockets, wait for an
// externally-triggered server restart (caller fires `railway restart`
// during the prearm window), measure the recovery cycle. Returns once
// 95% are back or the recovery deadline passes.
//
// `?serverUrl=` overrides the default Socket.io target.
app.post("/bench-avalanche-socketio", async (req, res) => {
  const n = parseInt((req.query.n as string) || "1000", 10);
  const rampPerSec = parseInt((req.query.ramp as string) || "200", 10);
  const prearmSec = parseInt((req.query.prearm as string) || "60", 10);
  const recoveryWaitSec = parseInt(
    (req.query.recoveryWait as string) || "180",
    10
  );
  const stream = (req.query.stream as string) || "avalanche";
  const serverUrl = (req.query.serverUrl as string) || SOCKETIO_URL;

  const result = await runAvalancheSocketio(
    { n, rampPerSec, prearmSec, recoveryWaitSec, stream },
    serverUrl
  );
  res.json(result);
});

// uWebSockets.js avalanche — same shape and methodology as the Socket.io
// avalanche, just pointed at the uws-server. Operator triggers redeploy
// during the prearm window: `railway redeploy -s uws-server --yes`.
app.post("/bench-avalanche-uws", async (req, res) => {
  const n = parseInt((req.query.n as string) || "1000", 10);
  const rampPerSec = parseInt((req.query.ramp as string) || "200", 10);
  const prearmSec = parseInt((req.query.prearm as string) || "120", 10);
  const recoveryWaitSec = parseInt(
    (req.query.recoveryWait as string) || "180",
    10
  );
  const stream = (req.query.stream as string) || "avalanche-uws";
  const wsUrl = (req.query.wsUrl as string) || UWS_WS_URL;

  const result = await runAvalancheUws(
    { n, rampPerSec, prearmSec, recoveryWaitSec, stream },
    wsUrl
  );
  res.json(result);
});

const port = parseInt(process.env.PORT || "3001", 10);
app.listen(port, () => {
  console.log(`bench-runner listening on :${port}`);
  console.log(`  socketio target:    ${SOCKETIO_URL}`);
  console.log(`  anycable target:    ${ANYCABLE_URL}`);
  console.log(`  anycable broadcast: ${ANYCABLE_BROADCAST_URL}`);
  console.log(`  uws ws target:      ${UWS_WS_URL}`);
  console.log(`  uws http target:    ${UWS_HTTP_URL}`);
});
