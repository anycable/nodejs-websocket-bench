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
import { spawn } from "node:child_process";

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
import {
  runThroughputAnycable,
  runThroughputAnycableCluster,
  runThroughputSocketio,
  runThroughputSocketioCsr,
  runThroughputSocketioRedis,
  runThroughputUws,
  type ThroughputParams,
} from "../lib/throughput.js";

const SOCKETIO_URL =
  process.env.SOCKETIO_URL || "http://socketio-server.railway.internal:3000";
const SOCKETIO_REDIS_URL_A =
  process.env.SOCKETIO_REDIS_URL_A ||
  "http://socketio-server-redis-a.railway.internal:3000";
const SOCKETIO_REDIS_URL_B =
  process.env.SOCKETIO_REDIS_URL_B ||
  "http://socketio-server-redis-b.railway.internal:3000";
const ANYCABLE_CLUSTER_URL_A =
  process.env.ANYCABLE_CLUSTER_URL_A ||
  "ws://anycable-go-cluster-a.railway.internal:8080/cable";
const ANYCABLE_CLUSTER_URL_B =
  process.env.ANYCABLE_CLUSTER_URL_B ||
  "ws://anycable-go-cluster-b.railway.internal:8080/cable";
const ANYCABLE_CLUSTER_BROADCAST_URL =
  process.env.ANYCABLE_CLUSTER_BROADCAST_URL ||
  "http://anycable-go-cluster-a.railway.internal:8080/_broadcast";
const ANYCABLE_CLUSTER_NATS_URL =
  process.env.ANYCABLE_CLUSTER_NATS_URL ||
  "nats://nats.railway.internal:4222";
const ANYCABLE_URL =
  process.env.ANYCABLE_URL || "ws://anycable-go.railway.internal:8080/cable";
const ANYCABLE_BROADCAST_URL =
  process.env.ANYCABLE_BROADCAST_URL ||
  "http://anycable-go.railway.internal:8080/_broadcast";
const ANYCABLE_BROADCAST_SECRET = process.env.ANYCABLE_BROADCAST_SECRET || "";
const ANYCABLE_NATS_URL = process.env.ANYCABLE_NATS_URL || "";
const ANYCABLE_NATS_SUBJECT = process.env.ANYCABLE_NATS_SUBJECT || "";
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

// ---------------------------------------------------------------------------
// Throughput (msg/sec) benches. 10K subscribers × N broadcasts at intervalMs.
// Sweep the rate from a CLI driver: 1, 10, 100, 1000 msg/sec target. Where
// each setup breaks (delivery drops, latency tail blows out) is the headline.
function throughputParamsFromQuery(req: express.Request, defaultStream: string): ThroughputParams {
  const publisherRaw = (req.query.publisher as string) || "";
  const publisher: "serial" | "pool" | "fireforget" | "nats" =
    publisherRaw === "pool" || publisherRaw === "fireforget" || publisherRaw === "nats"
      ? (publisherRaw as "pool" | "fireforget" | "nats")
      : "serial";
  return {
    n: parseInt((req.query.n as string) || "10000", 10),
    totalMessages: parseInt((req.query.total as string) || "100", 10),
    intervalMs: parseInt((req.query.intervalMs as string) || "100", 10),
    rampPerSec: parseInt((req.query.ramp as string) || "200", 10),
    stream: (req.query.stream as string) || defaultStream,
    drainSec: parseInt((req.query.drain as string) || "30", 10),
    publisher,
    publisherConcurrency: parseInt((req.query.publisherConcurrency as string) || "16", 10),
  };
}

app.post("/bench-throughput-anycable", async (req, res) => {
  const params = throughputParamsFromQuery(req, `tp-ac-${Date.now()}`);
  const cableUrl = (req.query.cableUrl as string) || ANYCABLE_URL;
  const broadcastUrl = (req.query.broadcastUrl as string) || ANYCABLE_BROADCAST_URL;
  const natsUrl = (req.query.natsUrl as string) || ANYCABLE_NATS_URL || undefined;
  const natsSubject =
    (req.query.natsSubject as string) || ANYCABLE_NATS_SUBJECT || undefined;
  const result = await runThroughputAnycable(params, {
    cableUrl,
    broadcastUrl,
    broadcastSecret: ANYCABLE_BROADCAST_SECRET || undefined,
    natsUrl,
    natsSubject,
  });
  res.json(result);
});

app.post("/bench-throughput-socketio", async (req, res) => {
  const params = throughputParamsFromQuery(req, `tp-sio-${Date.now()}`);
  const serverUrl = (req.query.serverUrl as string) || SOCKETIO_URL;
  const result = await runThroughputSocketio(params, { serverUrl });
  res.json(result);
});

app.post("/bench-throughput-socketio-csr", async (req, res) => {
  const params = throughputParamsFromQuery(req, `tp-csr-${Date.now()}`);
  const serverUrl = (req.query.serverUrl as string) || SOCKETIO_URL;
  const result = await runThroughputSocketioCsr(params, { serverUrl });
  res.json(result);
});

// AnyCable cluster — 2 anycable-go instances behind shared NATS. Clients
// split 50/50; publisher in bench-runner over HTTP /_broadcast (NATS fans
// out to both instances). Symmetric to the socketio+Redis HTTP test —
// answers "how does AnyCable scale horizontally vs Socket.io+Redis?".
// `?cableUrlA=`, `?cableUrlB=`, and `?broadcastUrl=` override defaults.
app.post("/bench-throughput-anycable-cluster", async (req, res) => {
  const params = throughputParamsFromQuery(req, `tp-ac-cluster-${Date.now()}`);
  const cableUrlA = (req.query.cableUrlA as string) || ANYCABLE_CLUSTER_URL_A;
  const cableUrlB = (req.query.cableUrlB as string) || ANYCABLE_CLUSTER_URL_B;
  const broadcastUrl = (req.query.broadcastUrl as string) || ANYCABLE_CLUSTER_BROADCAST_URL;
  const natsUrl = (req.query.natsUrl as string) || ANYCABLE_CLUSTER_NATS_URL || undefined;
  const natsSubject = (req.query.natsSubject as string) || undefined;
  const result = await runThroughputAnycableCluster(params, {
    cableUrlA,
    cableUrlB,
    broadcastUrl,
    broadcastSecret: ANYCABLE_BROADCAST_SECRET || undefined,
    natsUrl,
    natsSubject,
  });
  res.json(result);
});

// Socket.io with Redis adapter — clients split 50/50 across two instances
// (A and B) sharing one Redis; publisher runs in-process on A via
// /publish-local. Half of the deliveries fan out locally on A, half cross
// Redis pub/sub to B. This shape mirrors production multi-node Socket.io,
// which is what you'd run once you grow past one Node's socket budget.
// `?subscriberUrlA=` and `?subscriberUrlB=` override the defaults.
app.post("/bench-throughput-socketio-redis", async (req, res) => {
  const params = throughputParamsFromQuery(req, `tp-redis-${Date.now()}`);
  const subscriberUrlA = (req.query.subscriberUrlA as string) || SOCKETIO_REDIS_URL_A;
  const subscriberUrlB = (req.query.subscriberUrlB as string) || SOCKETIO_REDIS_URL_B;
  const result = await runThroughputSocketioRedis(params, {
    subscriberUrlA,
    subscriberUrlB,
  });
  res.json(result);
});

app.post("/bench-throughput-uws", async (req, res) => {
  const params = throughputParamsFromQuery(req, `tp-uws-${Date.now()}`);
  const wsUrl = (req.query.wsUrl as string) || UWS_WS_URL;
  const httpUrl = (req.query.httpUrl as string) || UWS_HTTP_URL;
  const result = await runThroughputUws(params, {
    serverWsUrl: wsUrl,
    serverHttpUrl: httpUrl,
  });
  res.json(result);
});

// Run Vladimir's stress_publications benchi binary baked into the image
// (Dockerfile stage `benchi`). It embeds the full anycable-go server
// in-process — no network between bench client and server — for an
// apples-to-apples comparison with the Socket.io / uWS in-process emit()
// / publish() tests. Knobs map 1:1 to the binary's flags.
app.post("/bench-benchi-anycable", async (req, res) => {
  const args = [
    "-c", String(parseInt((req.query.c as string) || "10000", 10)),
    "-r", String(parseInt((req.query.r as string) || "100", 10)),
    "-d", (req.query.d as string) || "10s",
    "-S", String(parseInt((req.query.S as string) || "1", 10)),
    "-s", String(parseInt((req.query.s as string) || "1", 10)),
  ];
  // --non-interactive suppresses lifecycle logs (incl. setup errors) on
  // stderr; off by default so we can see what's wrong on failures.
  if (req.query.quiet === "1") args.push("--non-interactive");
  const optionalFlags: Array<[string, string]> = [
    ["drain-timeout", (req.query.drainTimeout as string) || ""],
    ["max-inflight", (req.query.maxInflight as string) || ""],
    ["publish-workers", (req.query.publishWorkers as string) || ""],
    ["publish-batch", (req.query.publishBatch as string) || ""],
    ["setup-failure-tolerance", (req.query.tolerance as string) || ""],
    ["seed", (req.query.seed as string) || ""],
  ];
  for (const [flag, value] of optionalFlags) {
    if (value) { args.push(`--${flag}`, value); }
  }

  const startedAt = Date.now();
  console.log(`[benchi] stress_publications ${args.join(" ")}`);
  const child = spawn("stress_publications", args);
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk) => { stdout += chunk; });
  child.stderr.on("data", (chunk) => { stderr += chunk; });

  child.on("close", (code) => {
    const elapsedMs = Date.now() - startedAt;
    // Parse key=value lines from stdout into a flat object.
    const result: Record<string, number | string> = {};
    for (const line of stdout.split("\n")) {
      const eq = line.indexOf("=");
      if (eq === -1) continue;
      const key = line.slice(0, eq).trim();
      const raw = line.slice(eq + 1).trim();
      const num = Number(raw);
      result[key] = Number.isFinite(num) && raw !== "" ? num : raw;
    }
    result.exitCode = code ?? -1;
    result.elapsedMs = elapsedMs;
    result.args = args.join(" ");
    if (stderr) result.stderr = stderr.slice(-1000);
    console.log(`[benchi] done in ${elapsedMs}ms (exit=${code}) max=${result.throughput_max_msgs_per_sec} short=${result.clients_short}`);
    res.json(result);
  });
  child.on("error", (err) => {
    console.error(`[benchi] spawn error: ${err.message}`);
    res.status(500).json({ error: err.message, stderr });
  });
});

const port = parseInt(process.env.PORT || "3001", 10);
app.listen(port, () => {
  console.log(`bench-runner listening on :${port}`);
  console.log(`  socketio target:    ${SOCKETIO_URL}`);
  console.log(`  anycable target:    ${ANYCABLE_URL}`);
  console.log(`  anycable broadcast: ${ANYCABLE_BROADCAST_URL}`);
  console.log(`  uws ws target:      ${UWS_WS_URL}`);
  console.log(`  uws http target:    ${UWS_HTTP_URL}`);
  console.log(`  socketio redis A:   ${SOCKETIO_REDIS_URL_A}`);
  console.log(`  socketio redis B:   ${SOCKETIO_REDIS_URL_B}`);
  console.log(`  anycable cluster A: ${ANYCABLE_CLUSTER_URL_A}`);
  console.log(`  anycable cluster B: ${ANYCABLE_CLUSTER_URL_B}`);
});
