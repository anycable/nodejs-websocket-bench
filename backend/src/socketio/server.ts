// Socket.io mode: Node.js handles BOTH WebSocket connections AND publishing.
// This is the standard Socket.io deployment — everything in one process.

import express from "express";
import { createServer } from "http";
import { Server } from "socket.io";
import { createAdapter } from "@socket.io/redis-adapter";
import { createAdapter as createStreamsAdapter } from "@socket.io/redis-streams-adapter";
import { Redis } from "ioredis";

const app = express();
const httpServer = createServer(app);

const enableCSR = process.env.SOCKETIO_CSR === "1";
const csrMaxDisconnectionMs = parseInt(process.env.SOCKETIO_CSR_MAX_MS || "120000");

// Ping settings — env-overridable so we can flip between aggressive
// (fast failure detection, default for jitter tests) and Socket.io's
// production defaults (25s/20s, right for measuring publisher-restart
// architectural property in standalone deploy-impact).
const pingInterval = parseInt(process.env.SOCKETIO_PING_INTERVAL_MS || "3000");
const pingTimeout = parseInt(process.env.SOCKETIO_PING_TIMEOUT_MS || "6000");

const io = new Server(httpServer, {
  transports: ["websocket"],
  pingInterval,
  pingTimeout,
  ...(enableCSR
    ? {
        connectionStateRecovery: {
          maxDisconnectionDuration: csrMaxDisconnectionMs,
          skipMiddlewares: true,
        },
      }
    : {}),
});

// Redis adapter — turns this Socket.io instance into one node of a
// horizontally-scaled cluster. Two adapter shapes:
//   - Default (no CSR): @socket.io/redis-adapter — pub/sub. Canonical
//     multi-node setup. Incompatible with CSR.
//   - CSR enabled: @socket.io/redis-streams-adapter — Redis Streams.
//     Compatible with CSR; durable + ordered.
const redisUrl = process.env.REDIS_URL || "";
if (redisUrl) {
  if (enableCSR) {
    const streamsClient = new Redis(redisUrl);
    io.adapter(createStreamsAdapter(streamsClient));
    console.log(`Redis Streams adapter: ENABLED for CSR (url=${redisUrl})`);
  } else {
    const pubClient = new Redis(redisUrl);
    const subClient = pubClient.duplicate();
    io.adapter(createAdapter(pubClient, subClient));
    console.log(`Redis pub/sub adapter: ENABLED (url=${redisUrl})`);
  }
} else {
  console.log(`Redis adapter: disabled (single-process emit only)`);
}

if (enableCSR) {
  console.log(`Connection State Recovery: ENABLED (maxDisconnectionDuration=${csrMaxDisconnectionMs}ms)`);
} else {
  console.log(`Connection State Recovery: disabled (default)`);
}

app.get("/health", (_req, res) => res.json({ status: "ok", mode: "socketio" }));

// Track connections
let connectionCount = 0;
app.get("/stats", (_req, res) => res.json({ connections: connectionCount }));

let recoveredCount = 0;
app.get("/stats-csr", (_req, res) => res.json({ connections: connectionCount, recovered: recoveredCount }));

io.on("connection", (socket) => {
  connectionCount++;

  // When CSR succeeds, socket.recovered === true and rooms are restored
  // automatically. No need to re-join. Buffered packets are sent after this.
  if (socket.recovered) recoveredCount++;

  socket.on("join", (stream: string) => {
    socket.join(stream);
  });

  // Whisper: client emits "whisper" with (room, payload). Server forwards
  // to all OTHER sockets in the room (sender excluded). This is how
  // Socket.io emulates the "client-to-client without backend hop" pattern:
  // the WS server is still in the path (no native peer-to-peer), but no
  // app code runs. socket.to(room).emit broadcasts to room minus sender.
  socket.on("whisper", (room: string, payload: unknown) => {
    socket.to(room).emit("whisper", payload);
  });

  socket.on("disconnect", () => {
    connectionCount--;
  });
});

// Publishing endpoint — same as AnyCable's /_broadcast concept
// The publisher script POSTs here to broadcast messages
app.use(express.json());
app.post("/_broadcast", (req, res) => {
  const { stream, data } = req.body;
  if (!stream || !data) {
    res.status(400).json({ error: "stream and data required" });
    return;
  }
  io.to(stream).emit("message", typeof data === "string" ? JSON.parse(data) : data);
  res.json({ ok: true });
});

// In-process publisher: POST /publish-local?total=120&interval=500&stream=benchmark
// This is how Socket.io is actually used — io.to().emit() in the same process.
// No HTTP hop. The Node.js event loop handles both publishing and delivery.
app.post("/publish-local", async (req, res) => {
  const total = parseInt((req.query.total as string) || "120");
  const interval = parseInt((req.query.interval as string) || "500");
  const streamName = (req.query.stream as string) || "benchmark";
  const delay = parseInt((req.query.delay as string) || "0");

  res.json({ status: "publishing-local", total, interval, stream: streamName, delay });

  if (delay > 0) {
    await new Promise((r) => setTimeout(r, delay * 1000));
  }

  for (let seq = 1; seq <= total; seq++) {
    const sentAt = Date.now();
    io.to(streamName).emit("message", { seq, sentAt, text: `msg_${seq}` });
    await new Promise((r) => setTimeout(r, interval));
  }
  console.log(`Published ${total} messages locally to stream ${streamName}`);
});

// Remote publisher: POST /publish?target=<broadcast_url>&secret=<key>&total=120&interval=500&stream=benchmark
// This runs the publisher from Railway, avoiding local port exhaustion at high client counts.
app.post("/publish", async (req, res) => {
  const target = (req.query.target as string) || `http://localhost:${process.env.PORT || 3000}/_broadcast`;
  const secret = (req.query.secret as string) || "";
  const total = parseInt((req.query.total as string) || "120");
  const interval = parseInt((req.query.interval as string) || "500");
  const streamName = (req.query.stream as string) || "benchmark";

  const delay = parseInt((req.query.delay as string) || "0");

  res.json({ status: "publishing", total, interval, stream: streamName, target, delay });

  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (secret) headers["Authorization"] = `Bearer ${secret}`;

  if (delay > 0) {
    console.log(`Publisher waiting ${delay}s before starting...`);
    await new Promise((r) => setTimeout(r, delay * 1000));
    console.log(`Publisher delay complete, starting...`);
  }

  for (let seq = 1; seq <= total; seq++) {
    const data = JSON.stringify({ seq, sentAt: Date.now(), text: `msg_${seq}` });
    try {
      await fetch(target, { method: "POST", headers, body: JSON.stringify({ stream: streamName, data }) });
    } catch {}
    await new Promise((r) => setTimeout(r, interval));
  }
  console.log(`Published ${total} messages to ${target}`);
});

// Note: the idle-connection capacity probe used to live here; it's been
// moved to the bench-runner as POST /bench-idle-anycable. The bench-runner
// is the right home — it's the load-generation service, can be deployed
// in multiple instances for shard scale-out, and uses the synchronous
// runner pattern shared with the jitter benches.

const port = parseInt(process.env.PORT || "3000");
httpServer.listen(port, () => {
  console.log(`Socket.io server listening on :${port}`);
});
