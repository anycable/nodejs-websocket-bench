// Railway-hosted bench-runner.
//
// Same source image as socketio-server, with SERVICE_ENTRY=bench-runner/server
// selecting this entry point. Targets default to *.railway.internal so client-
// side bottlenecks (NAT, dev-machine event loop) don't interfere with measuring
// server capacity at 10K+ scale.
//
// Two modes per POST endpoint:
//   - sync (default):  blocks until the test finishes, returns the full result.
//                      Best for short tests; Railway's edge proxy caps at 5 min.
//   - async (?async=1): returns 202 {jobId} immediately, work continues in
//                      background. Poll GET /jobs/:id for {status, result?, logTail}.
//                      Use this for any test that may run longer than 5 min,
//                      and for multi-shard coordination (driver fans out k jobs
//                      and joins their results).

import express from "express";
import { spawn } from "node:child_process";

import { getJob, startJob } from "../lib/core/job-queue.js";

import { paramsFromQuery } from "../lib/core/params.js";
import {
  runJitterAnycable,
  runJitterSocketio,
  runJitterSocketioCsr,
} from "../lib/jitter-runners.js";
import { runJitterAnycableTraced } from "../lib/jitter-anycable-traced.js";
import { runAnycableTrace } from "../lib/anycable-trace.js";
import { runIdleAnycable, runIdleSocketio, runIdleUws } from "../lib/idle-runner.js";
import { runAvalancheSocketio } from "../lib/avalanche-runner.js";
import { runAvalancheAnycable } from "../lib/avalanche-anycable-runner.js";
import { runDeployImpactSocketio } from "../lib/deploy-impact-runner.js";
import { runStandaloneDeployImpactSocketio } from "../lib/standalone-deploy-impact-runner.js";
import { runStandaloneDeployImpactAnycable } from "../lib/standalone-deploy-impact-anycable-runner.js";
import {
  runWhispersAnycable,
  runWhispersSocketio,
  runWhispersUws,
} from "../lib/whispers-runner.js";
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
import {
  runJitterCentrifugo,
  runThroughputCentrifugo,
  runWhispersCentrifugo,
  runIdleCentrifugo,
  runAvalancheCentrifugo,
  type CentrifugoUrls,
} from "../lib/centrifugo-runners.js";

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
// Centrifugo target (standalone Go WS server, like anycable-go). WS endpoint
// is /connection/websocket; server API + publish live on the same port.
const CENTRIFUGO_WS_URL =
  process.env.CENTRIFUGO_WS_URL ||
  "ws://centrifugo.railway.internal:8000/connection/websocket";
const CENTRIFUGO_HTTP_URL =
  process.env.CENTRIFUGO_HTTP_URL || "http://centrifugo.railway.internal:8000";
const CENTRIFUGO_API_KEY =
  process.env.CENTRIFUGO_API_KEY || "bench-centrifugo-api-key";
const CENTRIFUGO_TOKEN_SECRET =
  process.env.CENTRIFUGO_TOKEN_SECRET || "bench-centrifugo-secret";

// Bundle the Centrifugo target config from query overrides + env defaults, so
// every centrifugo endpoint targets the same service (or an override) the same
// way the anycable endpoints accept ?cableUrl=.
function centrifugoUrls(req: express.Request): CentrifugoUrls {
  return {
    wsUrl: (req.query.wsUrl as string) || CENTRIFUGO_WS_URL,
    httpBase: (req.query.httpUrl as string) || CENTRIFUGO_HTTP_URL,
    apiKey: (req.query.apiKey as string) || CENTRIFUGO_API_KEY,
    tokenSecret: (req.query.tokenSecret as string) || CENTRIFUGO_TOKEN_SECRET,
    channelNamespace: (req.query.namespace as string) || undefined,
  };
}

const app = express();
app.use(express.json());

// Shared bearer-token gate for every /bench-* and /jobs/* endpoint.
// `/health` stays open so Railway and uptime probes don't need the token.
//
// Without this, anyone who discovers the bench-runner's public Railway
// domain can fire 1M-connection idle tests against our private infra
// (and rack up the bill). Setting BENCH_RUNNER_TOKEN to empty disables
// the check — only do that for an internal-only deployment with no
// public domain.
const BENCH_RUNNER_TOKEN = process.env.BENCH_RUNNER_TOKEN || "";
const AUTH_OPEN_PATHS = new Set(["/health"]);

app.use((req, res, next) => {
  if (!BENCH_RUNNER_TOKEN) return next();
  if (AUTH_OPEN_PATHS.has(req.path)) return next();
  const header = req.get("authorization") || "";
  const expected = `Bearer ${BENCH_RUNNER_TOKEN}`;
  if (header !== expected) {
    res.status(401).json({ error: "unauthorized" });
    return;
  }
  next();
});

app.get("/health", (_req, res) =>
  res.json({
    status: "ok",
    mode: "bench-runner",
    socketioUrl: SOCKETIO_URL,
    anycableUrl: ANYCABLE_URL,
    authRequired: BENCH_RUNNER_TOKEN.length > 0,
  })
);

// Wrap each handler in this so we get both modes for free. `?async=1`
// switches the response to 202 {jobId} + background execution; without
// it the endpoint behaves identically to before this change.
async function respondAsync<T>(
  req: express.Request,
  res: express.Response,
  run: () => Promise<T>,
): Promise<void> {
  if (req.query.async === "1") {
    const jobId = startJob(run);
    res.status(202).json({ jobId });
    return;
  }
  try {
    const result = await run();
    res.json(result);
  } catch (err) {
    res
      .status(500)
      .json({ error: err instanceof Error ? err.message : String(err) });
  }
}

// Polled by drivers. `?logLines=N` overrides the default tail size.
app.get("/jobs/:id", (req, res) => {
  const job = getJob(req.params.id);
  if (!job) {
    res.status(404).json({ error: "job not found" });
    return;
  }
  const logLines = Math.min(
    Math.max(parseInt((req.query.logLines as string) || "50", 10) || 50, 1),
    500,
  );
  res.json({
    id: job.id,
    status: job.status,
    startedAt: job.startedAt,
    endedAt: job.endedAt,
    durationMs: (job.endedAt ?? Date.now()) - job.startedAt,
    logTail: job.log.slice(-logLines),
    result: job.result,
    error: job.error,
  });
});

// `?cableUrl=` and `?broadcastUrl=` override the defaults so we can target
// either anycable-go (OSS) or anycable-go-pro within the same project.
app.post("/bench-jitter-anycable", async (req, res) => {
  const params = paramsFromQuery(req);
  const cableUrl = (req.query.cableUrl as string) || ANYCABLE_URL;
  const broadcastUrl = (req.query.broadcastUrl as string) || ANYCABLE_BROADCAST_URL;
  // `?channel=BenchmarkChannel&acProtocol=actioncable-v1-json` targets a real
  // Rails app (Action Cable / Solid Cable); the defaults target anycable-go's
  // $pubsub channel over the extended protocol.
  const channel = (req.query.channel as string) || undefined;
  const acProtocol = (req.query.acProtocol as string) || undefined;
  // `?reconnectBaseMs=200` makes the client's first reconnect fire in ~200ms
  // (vs the multi-second @anycable/core default), shrinking the resume-tail p99.
  const reconnectBaseMs = req.query.reconnectBaseMs
    ? parseInt(req.query.reconnectBaseMs as string, 10)
    : undefined;
  // `?clientLib=actioncable` drives the official @rails/actioncable client
  // (for Action Cable / Solid Cable / Async::Cable); default @anycable/core.
  const clientLib =
    (req.query.clientLib as string) === "actioncable" ? "actioncable" : undefined;
  await respondAsync(req, res, () =>
    runJitterAnycable(params, {
      cableUrl,
      broadcastUrl,
      broadcastSecret: ANYCABLE_BROADCAST_SECRET || undefined,
      channel,
      acProtocol,
      reconnectBaseMs,
      clientLib,
    }),
  );
});

// Intra-Railway latency tracer for AnyCable broadcasts. Decomposes the
// end-to-end p99 into four observable phases — http.broadcast,
// bench.wait-first, bench.fanout-tail, bench.broadcast (total) —
// emitted as OpenTelemetry OTLP-shaped spans. Run from inside the
// bench-runner so anycable-go is on the same private network and the
// http.broadcast span isn't polluted by external-network RTT (Starlink,
// laptop hops, etc).
//
// Query: ?n=2500&broadcasts=2000&intervalMs=50&rampPerSec=200&includeSpans=1
// Use ?async=1 (recommended): returns 202 {jobId}, poll /jobs/:id.
app.post("/bench-trace-anycable", async (req, res) => {
  const cableUrl = (req.query.cableUrl as string) || ANYCABLE_URL;
  const broadcastUrl =
    (req.query.broadcastUrl as string) || ANYCABLE_BROADCAST_URL;
  const n = parseInt((req.query.n as string) || "2500", 10);
  const broadcasts = parseInt((req.query.broadcasts as string) || "2000", 10);
  const intervalMs = parseInt((req.query.intervalMs as string) || "50", 10);
  const rampPerSec = parseInt((req.query.rampPerSec as string) || "200", 10);
  const stream =
    (req.query.stream as string) || `anycable-trace-${Date.now()}`;
  const includeSpans = req.query.includeSpans === "1";

  await respondAsync(req, res, () =>
    runAnycableTrace({
      cableUrl,
      broadcastUrl,
      broadcastSecret: ANYCABLE_BROADCAST_SECRET || undefined,
      n,
      broadcasts,
      intervalMs,
      rampPerSec,
      stream,
      includeSpans,
      log: (line) => console.log(`[trace-ac] ${line}`),
    }),
  );
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
  await respondAsync(req, res, () =>
    runJitterAnycableTraced(
      params,
      {
        cableUrl,
        broadcastUrl,
        broadcastSecret: ANYCABLE_BROADCAST_SECRET || undefined,
      },
      { traceSample },
    ),
  );
});

app.post("/bench-jitter-socketio", async (req, res) => {
  const params = paramsFromQuery(req);
  const serverUrl = (req.query.serverUrl as string) || SOCKETIO_URL;
  await respondAsync(req, res, () =>
    runJitterSocketio(params, { serverUrl }),
  );
});

// CSR target separately overridable since CSR mode is a server-side env var
// that has to be wired at boot. In production the CSR server is
// socketio-server-csr; default is the same SOCKETIO_URL for back-compat.
app.post("/bench-jitter-socketio-csr", async (req, res) => {
  const params = paramsFromQuery(req);
  const serverUrl = (req.query.serverUrl as string) || SOCKETIO_URL;
  await respondAsync(req, res, () =>
    runJitterSocketioCsr(params, { serverUrl }),
  );
});

// uWebSockets.js jitter — `?wsUrl=` and `?httpUrl=` override defaults so
// the same bench-runner can target uws-server, uws-server-small, or any
// other uWS deployment over Railway's internal network.
app.post("/bench-jitter-uws", async (req, res) => {
  const params = paramsFromQuery(req);
  const wsUrl = (req.query.wsUrl as string) || UWS_WS_URL;
  const httpUrl = (req.query.httpUrl as string) || UWS_HTTP_URL;
  await respondAsync(req, res, () =>
    runJitterUws(params, { serverWsUrl: wsUrl, serverHttpUrl: httpUrl }),
  );
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
  const channel = (req.query.channel as string) || undefined;
  const acProtocol = (req.query.acProtocol as string) || undefined;

  await respondAsync(req, res, () =>
    runIdleAnycable({ n, holdSec, rampPerSec, stream, channel, acProtocol }, cableUrl, shardLabel),
  );
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

  await respondAsync(req, res, () =>
    runIdleSocketio({ n, holdSec, rampPerSec, stream }, serverUrl, shardLabel),
  );
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

  await respondAsync(req, res, () =>
    runIdleUws({ n, holdSec, rampPerSec, stream }, wsUrl, shardLabel),
  );
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

  await respondAsync(req, res, () =>
    runAvalancheSocketio(
      { n, rampPerSec, prearmSec, recoveryWaitSec, stream },
      serverUrl,
    ),
  );
});

// Action Cable avalanche — connect N cables (AnyCable / Action Cable / Solid
// Cable), wait for an externally-triggered redeploy, measure recovery. For the
// in-process adapters (redeploy Puma) connections drop and reconnect; for
// AnyCable (redeploy the Rails RPC backend) the gateway holds them and
// `disconnected` stays ~0. `?channel=` + `?acProtocol=` select the target.
app.post("/bench-avalanche-anycable", async (req, res) => {
  const n = parseInt((req.query.n as string) || "1000", 10);
  const rampPerSec = parseInt((req.query.ramp as string) || "200", 10);
  const prearmSec = parseInt((req.query.prearm as string) || "120", 10);
  const recoveryWaitSec = parseInt((req.query.recoveryWait as string) || "240", 10);
  const stream = (req.query.stream as string) || "avalanche-ac";
  const cableUrl = (req.query.cableUrl as string) || ANYCABLE_URL;
  const channel = (req.query.channel as string) || undefined;
  const acProtocol = (req.query.acProtocol as string) || undefined;

  await respondAsync(req, res, () =>
    runAvalancheAnycable(
      { n, rampPerSec, prearmSec, recoveryWaitSec, stream },
      { cableUrl, channel, acProtocol },
    ),
  );
});

// Deploy-impact for clustered Socket.io + Redis adapter. Holds N clients
// across the cluster nodes (round-robin), runs a publisher loop at a
// fixed rate, and measures per-client gap (last-msg-before-disconnect
// to first-msg-after-reconnect) plus messages lost in the gap window.
//
// Operator runs `railway redeploy -s socketio-server-redis-a --yes` then
// `... -s socketio-server-redis-b --yes` (or however many nodes) DURING
// the preDeploySec window. The runner detects deploys via the first
// disconnect event so no explicit coordination needed.
//
// Query params:
//   n               — clients (default 10000)
//   ramp            — clients/sec ramp-up (default 200)
//   stream          — broadcast channel (default deploy-impact)
//   pubRate         — publish rate msg/sec (default 2)
//   preDeploy       — steady-state seconds before operator triggers deploy (default 30)
//   postDeploy      — wait seconds after deploy starts for full recovery (default 120)
//   nodes           — comma-separated list of internal node URLs (default redis-a,redis-b)
app.post("/bench-deploy-impact-socketio", async (req, res) => {
  const n = parseInt((req.query.n as string) || "10000", 10);
  const rampPerSec = parseInt((req.query.ramp as string) || "200", 10);
  const stream = (req.query.stream as string) || "deploy-impact";
  const publishRatePerSec = parseInt((req.query.pubRate as string) || "2", 10);
  const preDeploySec = parseInt((req.query.preDeploy as string) || "30", 10);
  const postDeploySec = parseInt((req.query.postDeploy as string) || "120", 10);

  const defaultNodes = [SOCKETIO_REDIS_URL_A, SOCKETIO_REDIS_URL_B];
  const nodesParam = (req.query.nodes as string) || "";
  const serverUrls = nodesParam
    ? nodesParam.split(",").map((s) => s.trim()).filter(Boolean)
    : defaultNodes;
  // The publisher targets one node's /_broadcast — with the Redis
  // adapter, the broadcast fans out to all subscribers across the
  // cluster, not just the locally-connected ones. We pick the first
  // node deterministically so the test is reproducible; if that
  // specific node is restarting, the publish will transiently fail
  // and the runner's catch block continues without blowing up.
  const publishUrl = `${serverUrls[0]}/_broadcast`;

  const publish = async (seq: number): Promise<void> => {
    const r = await fetch(publishUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ stream, data: JSON.stringify({ seq }) }),
    });
    if (!r.ok) {
      throw new Error(`publish HTTP ${r.status}`);
    }
  };

  await respondAsync(req, res, () =>
    runDeployImpactSocketio(
      { n, rampPerSec, stream, publishRatePerSec, preDeploySec, postDeploySec },
      serverUrls,
      publish,
    ),
  );
});

// Standalone deploy-impact (A2c-standalone). Holds N WS clients connected
// to a WS service while a SEPARATE publisher service publishes broadcasts.
// The local driver triggers `railway redeploy -s publisher` mid-test.
// Expected outcome for properly standalone setups: 0 affected clients,
// publisher-downtime-sized gap in receive cadence, zero true losses.
//
// Query params (same shape as embedded version):
//   n               — clients (default 10000)
//   ramp            — ramp/sec (default 200)
//   stream          — broadcast channel (default standalone-publisher)
//   duration        — total test runtime seconds (default 240)
//   nodes           — comma-separated WS node URLs (default SOCKETIO_URL)
app.post("/bench-deploy-impact-standalone-socketio", async (req, res) => {
  const n = parseInt((req.query.n as string) || "10000", 10);
  const rampPerSec = parseInt((req.query.ramp as string) || "200", 10);
  const stream = (req.query.stream as string) || "standalone-publisher";
  const testDurationSec = parseInt(
    (req.query.duration as string) || "240",
    10,
  );

  const nodesParam = (req.query.nodes as string) || "";
  const serverUrls = nodesParam
    ? nodesParam.split(",").map((s) => s.trim()).filter(Boolean)
    : [SOCKETIO_URL];

  await respondAsync(req, res, () =>
    runStandaloneDeployImpactSocketio(
      { n, rampPerSec, stream, testDurationSec },
      serverUrls,
    ),
  );
});

// Standalone deploy-impact for AnyCable. Same shape as the Socket.io
// variant: clients hold connections to anycable-go, a separate publisher
// service publishes broadcasts via HTTP, and the driver redeploys the
// publisher mid-test. AnyCable is standalone by design, so we expect
// affectedClients=0.
app.post("/bench-deploy-impact-standalone-anycable", async (req, res) => {
  const n = parseInt((req.query.n as string) || "10000", 10);
  const rampPerSec = parseInt((req.query.ramp as string) || "200", 10);
  const stream = (req.query.stream as string) || "standalone-publisher";
  const testDurationSec = parseInt(
    (req.query.duration as string) || "240",
    10,
  );
  const cableUrl = (req.query.cableUrl as string) || ANYCABLE_URL;

  await respondAsync(req, res, () =>
    runStandaloneDeployImpactAnycable(
      { n, rampPerSec, stream, testDurationSec },
      cableUrl,
    ),
  );
});

// Whispers — client-to-client updates that bypass the backend (the
// Liveblocks/Yjs/PartyKit category). AnyCable native uses channel.whisper
// (anycable-go fans out without invoking app code). Socket.io emulates
// via socket.to(room).emit(...) in a "whisper" handler on the server.
// Query params:
//   n=10000 rooms=100 ramp=200 interval=100 duration=30 payload=64
//   roomPrefix=<shared>     — multi-shard runs pass the same prefix to
//                             every shard so peers fan out cross-shard
//   samplesCap=5000         — include downsampled sorted latencies in
//                             the result for cross-shard percentile merge
function whispersParamsFromQuery(req: express.Request) {
  const samplesCapEnv = parseInt((req.query.samplesCap as string) || "0", 10);
  return {
    n: parseInt((req.query.n as string) || "1000", 10),
    rooms: parseInt((req.query.rooms as string) || "10", 10),
    rampPerSec: parseInt((req.query.ramp as string) || "100", 10),
    whisperIntervalMs: parseInt((req.query.interval as string) || "100", 10),
    testDurationSec: parseInt((req.query.duration as string) || "30", 10),
    payloadBytes: parseInt((req.query.payload as string) || "64", 10),
    roomPrefix: (req.query.roomPrefix as string) || undefined,
    samplesCap: samplesCapEnv > 0 ? samplesCapEnv : undefined,
  };
}

app.post("/bench-whispers-anycable", async (req, res) => {
  const params = whispersParamsFromQuery(req);
  const cableUrl = (req.query.cableUrl as string) || ANYCABLE_URL;
  await respondAsync(req, res, () => runWhispersAnycable(params, cableUrl));
});

app.post("/bench-whispers-socketio", async (req, res) => {
  const params = whispersParamsFromQuery(req);
  const serverUrl = (req.query.serverUrl as string) || SOCKETIO_URL;
  await respondAsync(req, res, () => runWhispersSocketio(params, { serverUrl }));
});

app.post("/bench-whispers-uws", async (req, res) => {
  const params = whispersParamsFromQuery(req);
  const serverWsUrl = (req.query.wsUrl as string) || UWS_WS_URL;

  await respondAsync(req, res, () =>
    runWhispersUws(params, { serverWsUrl }),
  );
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

  await respondAsync(req, res, () =>
    runAvalancheUws(
      { n, rampPerSec, prearmSec, recoveryWaitSec, stream },
      wsUrl,
    ),
  );
});

// ---------------------------------------------------------------------------
// Centrifugo benches. Centrifugo is a standalone Go WS server like anycable-go
// (broadcast over HTTP), with built-in history/recovery, presence, and JWT —
// the closest comparator to AnyCable in this suite. Each endpoint mirrors its
// anycable twin; `?wsUrl=`, `?httpUrl=`, `?apiKey=`, `?tokenSecret=`,
// `?namespace=` override the target.

app.post("/bench-jitter-centrifugo", async (req, res) => {
  const params = paramsFromQuery(req);
  const urls = centrifugoUrls(req);
  await respondAsync(req, res, () => runJitterCentrifugo(params, urls));
});

app.post("/bench-idle-centrifugo", async (req, res) => {
  const n = parseInt((req.query.n as string) || "10000", 10);
  const holdSec = parseInt((req.query.hold as string) || "60", 10);
  const rampPerSec = parseInt((req.query.ramp as string) || "200", 10);
  const stream = (req.query.stream as string) || "idle-probe";
  const shardLabel = (req.query.shard as string) || undefined;
  const urls = centrifugoUrls(req);
  await respondAsync(req, res, () =>
    runIdleCentrifugo({ n, holdSec, rampPerSec, stream }, urls, shardLabel),
  );
});

app.post("/bench-whispers-centrifugo", async (req, res) => {
  const params = whispersParamsFromQuery(req);
  const urls = centrifugoUrls(req);
  // Whispers default to the "whisper" namespace (allow_publish_for_subscriber)
  // unless the caller overrides with ?namespace=.
  await respondAsync(req, res, () =>
    runWhispersCentrifugo(params, { ...urls, channelNamespace: urls.channelNamespace ?? "whisper" }),
  );
});

app.post("/bench-throughput-centrifugo", async (req, res) => {
  const base = throughputParamsFromQuery(req, `tp-cfgo-${Date.now()}`);
  const urls = centrifugoUrls(req);
  // Centrifugo's HTTP publisher supports serial/pool/fireforget; map the
  // AnyCable-only "nats" mode onto pool so a shared driver flag still works.
  const publisher =
    base.publisher === "pool" || base.publisher === "fireforget"
      ? base.publisher
      : base.publisher === "serial"
        ? "serial"
        : "pool";
  await respondAsync(req, res, () =>
    runThroughputCentrifugo(
      {
        n: base.n,
        totalMessages: base.totalMessages,
        intervalMs: base.intervalMs,
        rampPerSec: base.rampPerSec,
        stream: base.stream,
        drainSec: base.drainSec,
        publisher,
        publisherConcurrency: base.publisherConcurrency,
      },
      urls,
    ),
  );
});

// Centrifugo avalanche / deploy resilience. Like AnyCable, Centrifugo is a
// standalone process, so an app deploy never severs its connections (expected
// disconnected ~= 0). Redeploying the centrifugo service itself is the only
// thing that drops them; the operator triggers that during the prearm window.
app.post("/bench-avalanche-centrifugo", async (req, res) => {
  const n = parseInt((req.query.n as string) || "1000", 10);
  const rampPerSec = parseInt((req.query.ramp as string) || "200", 10);
  const prearmSec = parseInt((req.query.prearm as string) || "120", 10);
  const recoveryWaitSec = parseInt((req.query.recoveryWait as string) || "240", 10);
  const stream = (req.query.stream as string) || "avalanche-cfgo";
  const urls = centrifugoUrls(req);
  await respondAsync(req, res, () =>
    runAvalancheCentrifugo({ n, rampPerSec, prearmSec, recoveryWaitSec, stream }, urls),
  );
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
  // `?channel=BenchmarkChannel&acProtocol=actioncable-v1-json` targets a real
  // Rails app over the base protocol; defaults keep the anycable-go $pubsub
  // channel over the extended protocol.
  const channel = (req.query.channel as string) || undefined;
  const acProtocol = (req.query.acProtocol as string) || undefined;
  await respondAsync(req, res, () =>
    runThroughputAnycable(params, {
      cableUrl,
      broadcastUrl,
      broadcastSecret: ANYCABLE_BROADCAST_SECRET || undefined,
      natsUrl,
      natsSubject,
      channel,
      acProtocol,
    }),
  );
});

app.post("/bench-throughput-socketio", async (req, res) => {
  const params = throughputParamsFromQuery(req, `tp-sio-${Date.now()}`);
  const serverUrl = (req.query.serverUrl as string) || SOCKETIO_URL;
  await respondAsync(req, res, () =>
    runThroughputSocketio(params, { serverUrl }),
  );
});

app.post("/bench-throughput-socketio-csr", async (req, res) => {
  const params = throughputParamsFromQuery(req, `tp-csr-${Date.now()}`);
  const serverUrl = (req.query.serverUrl as string) || SOCKETIO_URL;
  await respondAsync(req, res, () =>
    runThroughputSocketioCsr(params, { serverUrl }),
  );
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
  await respondAsync(req, res, () =>
    runThroughputAnycableCluster(params, {
      cableUrlA,
      cableUrlB,
      broadcastUrl,
      broadcastSecret: ANYCABLE_BROADCAST_SECRET || undefined,
      natsUrl,
      natsSubject,
    }),
  );
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
  await respondAsync(req, res, () =>
    runThroughputSocketioRedis(params, { subscriberUrlA, subscriberUrlB }),
  );
});

app.post("/bench-throughput-uws", async (req, res) => {
  const params = throughputParamsFromQuery(req, `tp-uws-${Date.now()}`);
  const wsUrl = (req.query.wsUrl as string) || UWS_WS_URL;
  const httpUrl = (req.query.httpUrl as string) || UWS_HTTP_URL;
  await respondAsync(req, res, () =>
    runThroughputUws(params, {
      serverWsUrl: wsUrl,
      serverHttpUrl: httpUrl,
    }),
  );
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

  await respondAsync(req, res, () =>
    new Promise<Record<string, number | string>>((resolve, reject) => {
      const startedAt = Date.now();
      console.log(`[benchi] stress_publications ${args.join(" ")}`);
      const child = spawn("stress_publications", args);
      let stdout = "";
      let stderr = "";
      child.stdout.on("data", (chunk) => {
        stdout += chunk;
      });
      child.stderr.on("data", (chunk) => {
        stderr += chunk;
      });
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
        console.log(
          `[benchi] done in ${elapsedMs}ms (exit=${code}) max=${result.throughput_max_msgs_per_sec} short=${result.clients_short}`,
        );
        resolve(result);
      });
      child.on("error", (err) => {
        console.error(`[benchi] spawn error: ${err.message}`);
        reject(err);
      });
    }),
  );
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
