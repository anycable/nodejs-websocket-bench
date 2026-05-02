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

const SOCKETIO_URL =
  process.env.SOCKETIO_URL || "http://socketio-server.railway.internal:3000";
const ANYCABLE_URL =
  process.env.ANYCABLE_URL || "ws://anycable-go.railway.internal:8080/cable";
const ANYCABLE_BROADCAST_URL =
  process.env.ANYCABLE_BROADCAST_URL ||
  "http://anycable-go.railway.internal:8080/_broadcast";
const ANYCABLE_BROADCAST_SECRET = process.env.ANYCABLE_BROADCAST_SECRET || "";

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

app.post("/bench-jitter-anycable", async (req, res) => {
  const params = paramsFromQuery(req);
  const result = await runJitterAnycable(params, {
    cableUrl: ANYCABLE_URL,
    broadcastUrl: ANYCABLE_BROADCAST_URL,
    broadcastSecret: ANYCABLE_BROADCAST_SECRET || undefined,
  });
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

const port = parseInt(process.env.PORT || "3001", 10);
app.listen(port, () => {
  console.log(`bench-runner listening on :${port}`);
  console.log(`  socketio target:    ${SOCKETIO_URL}`);
  console.log(`  anycable target:    ${ANYCABLE_URL}`);
  console.log(`  anycable broadcast: ${ANYCABLE_BROADCAST_URL}`);
});
