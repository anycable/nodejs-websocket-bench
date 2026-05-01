// Socket.io mode: Node.js handles BOTH WebSocket connections AND publishing.
// This is the standard Socket.io deployment — everything in one process.

import express from "express";
import { createServer } from "http";
import { Server } from "socket.io";

const app = express();
const httpServer = createServer(app);

// Connection State Recovery (opt-in, experimental — Socket.io 4.6+).
// When enabled, the server stashes socket.id + rooms + socket.data on
// unexpected disconnects, and replays buffered packets when a client
// reconnects with the same private session id (pid) + last offset.
//
// Caveats (from the Socket.io docs):
//   - default in-memory adapter stores state per-process, lost on restart
//   - Redis PUB/SUB adapter is NOT compatible; use Redis Streams or MongoDB
//     to survive restarts / work across nodes
//   - "the recovery will not always be successful" — application-level
//     reconciliation is still required
const enableCSR = process.env.SOCKETIO_CSR === "1";
const csrMaxDisconnectionMs = parseInt(process.env.SOCKETIO_CSR_MAX_MS || "120000"); // 2 min

const io = new Server(httpServer, {
  transports: ["websocket"], // fair comparison — skip long-polling upgrade
  pingInterval: 3000,
  pingTimeout: 6000,
  ...(enableCSR
    ? {
        connectionStateRecovery: {
          maxDisconnectionDuration: csrMaxDisconnectionMs,
          skipMiddlewares: true,
        },
      }
    : {}),
});

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

// Idle connection capacity probe against anycable-go.
//
// Opens N raw WebSocket connections directly to anycable-go (using
// `actioncable-v1-ext-json` subprotocol so it's the same connection type a
// real client would establish), waits for "welcome", subscribes to a stream,
// then holds for `hold` seconds. This runs from inside Railway, so the
// connection count is bounded by anycable-go's capacity, not the local NAT
// table on a developer machine.
//
// POST /idle-anycable?n=10000&hold=60&url=ws://anycable-go.railway.internal:8080/cable
import WebSocket from "ws";

interface IdleResult {
  connected: number;
  failed: number;
  welcomed: number;
  subscribed: number;
}

app.post("/idle-anycable", async (req, res) => {
  const n = parseInt((req.query.n as string) || "1000");
  const holdSec = parseInt((req.query.hold as string) || "30");
  const url = (req.query.url as string) || "ws://anycable-go.railway.internal:8080/cable";
  const stream = (req.query.stream as string) || "idle-probe";
  const rampPerSec = parseInt((req.query.ramp as string) || "200");

  res.json({ status: "starting", n, holdSec, url, stream, rampPerSec });

  console.log(`[idle] connecting ${n} clients to ${url}, ramp=${rampPerSec}/s, hold=${holdSec}s`);

  const result: IdleResult = { connected: 0, failed: 0, welcomed: 0, subscribed: 0 };
  const sockets: WebSocket[] = [];

  for (let i = 0; i < n; i++) {
    const ws = new WebSocket(url, ["actioncable-v1-ext-json"]);
    sockets.push(ws);

    ws.once("open", () => {
      result.connected++;
    });
    ws.once("error", () => {
      result.failed++;
    });
    ws.on("message", (raw) => {
      try {
        const msg = JSON.parse(raw.toString());
        if (msg.type === "welcome") {
          result.welcomed++;
          // Subscribe to a $pubsub stream — the broker preset accepts this
          // without RPC since ANYCABLE_PUBLIC=true.
          const cmd = {
            command: "subscribe",
            identifier: JSON.stringify({ channel: "$pubsub", stream_name: stream }),
          };
          ws.send(JSON.stringify(cmd));
        } else if (msg.type === "confirm_subscription") {
          result.subscribed++;
        }
      } catch {}
    });

    if ((i + 1) % rampPerSec === 0) {
      await new Promise((r) => setTimeout(r, 1000));
      if ((i + 1) % 1000 === 0) {
        console.log(
          `[idle] ramped ${i + 1}/${n}  connected=${result.connected} welcomed=${result.welcomed} subscribed=${result.subscribed} failed=${result.failed}`
        );
      }
    }
  }

  // Stabilize, then hold.
  await new Promise((r) => setTimeout(r, 5000));
  console.log(
    `[idle] all ramped: connected=${result.connected}/${n} welcomed=${result.welcomed} subscribed=${result.subscribed} failed=${result.failed}`
  );
  console.log(`[idle] holding ${holdSec}s...`);
  await new Promise((r) => setTimeout(r, holdSec * 1000));

  console.log(
    `[idle] hold complete. final: connected=${result.connected} welcomed=${result.welcomed} subscribed=${result.subscribed} failed=${result.failed}`
  );

  // Tear down.
  for (const s of sockets) {
    try { s.close(); } catch {}
  }
});

const port = parseInt(process.env.PORT || "3000");
httpServer.listen(port, () => {
  console.log(`Socket.io server listening on :${port}`);
});
