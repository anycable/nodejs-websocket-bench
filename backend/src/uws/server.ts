// uWebSockets.js mode: a single Node process handling BOTH WebSocket
// connections AND publishing — the canonical "embedded uWS in your app"
// deployment. No replay buffer: messages broadcast during a client's
// disconnect window are gone, just like a typical hand-rolled uWS app.
//
// HTTP and WS share the same uWS App (single port, single listener).
// The HTTP surface mirrors socketio/server.ts so the bench-runner can
// drive the same publish/probe flows.
//
//   GET  /health              — liveness
//   GET  /stats               — { connections }
//   POST /_broadcast          — { stream, data } → app.publish(stream, ...)
//   POST /publish-local       — in-process publisher loop (the realistic path)
//   POST /publish             — out-of-process publisher loop (parity w/ socketio)
//   WS   /ws                  — connect + send {type:"subscribe", topic}
//
// Wire format on the WS:
//   client → server  {"type":"subscribe","topic":"<stream>"}
//   server → client  raw JSON message body, e.g. {"seq":1,"sentAt":...,"text":"m1"}

import uWS from "uWebSockets.js";

const port = parseInt(process.env.PORT || "3000", 10);

let connectionCount = 0;

const app = uWS.App();

interface WsData {
  topics: Set<string>;
}

app.ws<WsData>("/ws", {
  // Reasonable defaults matching socketio's pingInterval/pingTimeout shape.
  // uWS handles ping/pong itself; idleTimeout closes silent sockets.
  idleTimeout: 30,
  maxPayloadLength: 64 * 1024,
  // Use shared compression off for parity with socketio's transports:["websocket"].
  compression: uWS.DISABLED,

  open: (ws) => {
    connectionCount++;
    ws.getUserData().topics = new Set<string>();
  },

  message: (ws, message, _isBinary) => {
    let text: string;
    try {
      text = Buffer.from(message).toString("utf-8");
    } catch {
      return;
    }
    let parsed: unknown;
    try {
      parsed = JSON.parse(text);
    } catch {
      return;
    }
    if (!parsed || typeof parsed !== "object") return;
    const m = parsed as { type?: string; topic?: string };
    if (m.type === "subscribe" && typeof m.topic === "string") {
      ws.subscribe(m.topic);
      ws.getUserData().topics.add(m.topic);
      return;
    }
    if (m.type === "unsubscribe" && typeof m.topic === "string") {
      ws.unsubscribe(m.topic);
      ws.getUserData().topics.delete(m.topic);
      return;
    }
    // Whisper: client sends {type:"whisper", topic, payload}. Server
    // republishes as {type:"whisper", payload} to the topic. uWS
    // ws.publish broadcasts to subscribers minus sender by default, so
    // this emulates client-to-client fan-out on the topic.
    const w = parsed as {
      type?: string;
      topic?: string;
      payload?: unknown;
    };
    if (
      w.type === "whisper" &&
      typeof w.topic === "string" &&
      w.payload !== undefined
    ) {
      const out = JSON.stringify({ type: "whisper", payload: w.payload });
      ws.publish(w.topic, out);
    }
  },

  close: (_ws, _code, _msg) => {
    connectionCount--;
  },
});

// Tiny JSON-body reader. uWS has no built-in body parser; this is the
// canonical pattern (onData chunks + onAborted handler).
function readJsonBody(
  res: uWS.HttpResponse,
  cb: (body: unknown | null) => void
): void {
  let buffer = Buffer.alloc(0);
  let aborted = false;
  res.onAborted(() => {
    aborted = true;
  });
  res.onData((chunk, isLast) => {
    buffer = Buffer.concat([buffer, Buffer.from(chunk)]);
    if (isLast) {
      if (aborted) return;
      try {
        cb(JSON.parse(buffer.toString("utf-8")));
      } catch {
        cb(null);
      }
    }
  });
}

// All HTTP responses have to be cork()'d if anything writes outside the
// initial sync handler. We use res.cork() defensively in async paths.
app.get("/health", (res) => {
  res.writeHeader("Content-Type", "application/json");
  res.end(JSON.stringify({ status: "ok", mode: "uws" }));
});

app.get("/stats", (res) => {
  res.writeHeader("Content-Type", "application/json");
  res.end(JSON.stringify({ connections: connectionCount }));
});

// External broadcast: caller POSTs { stream, data }; we publish to that topic.
app.post("/_broadcast", (res, _req) => {
  readJsonBody(res, (body) => {
    if (!body || typeof body !== "object") {
      res.cork(() => {
        res.writeStatus("400 Bad Request");
        res.writeHeader("Content-Type", "application/json");
        res.end('{"error":"invalid body"}');
      });
      return;
    }
    const { stream, data } = body as { stream?: string; data?: unknown };
    if (!stream || data === undefined) {
      res.cork(() => {
        res.writeStatus("400 Bad Request");
        res.writeHeader("Content-Type", "application/json");
        res.end('{"error":"stream and data required"}');
      });
      return;
    }
    const payload = typeof data === "string" ? data : JSON.stringify(data);
    app.publish(stream, payload);
    res.cork(() => {
      res.writeHeader("Content-Type", "application/json");
      res.end('{"ok":true}');
    });
  });
});

// In-process publisher: response returns immediately; the publish loop
// runs as a detached promise. Mirrors socketio's /publish-local.
app.post("/publish-local", (res, req) => {
  const total = parseInt(req.getQuery("total") || "120", 10);
  const interval = parseInt(req.getQuery("interval") || "500", 10);
  const stream = req.getQuery("stream") || "benchmark";
  const delay = parseInt(req.getQuery("delay") || "0", 10);

  res.cork(() => {
    res.writeHeader("Content-Type", "application/json");
    res.end(
      JSON.stringify({ status: "publishing-local", total, interval, stream, delay })
    );
  });

  void (async () => {
    if (delay > 0) await new Promise((r) => setTimeout(r, delay * 1000));
    for (let seq = 1; seq <= total; seq++) {
      const msg = JSON.stringify({ seq, sentAt: Date.now(), text: `msg_${seq}` });
      app.publish(stream, msg);
      await new Promise((r) => setTimeout(r, interval));
    }
    console.log(`Published ${total} messages locally to topic ${stream}`);
  })();
});

// Out-of-process publisher: same shape as socketio's /publish — POSTs to a
// remote /_broadcast URL. Useful for cross-service load-gen on Railway.
app.post("/publish", (res, req) => {
  const target = req.getQuery("target") || `http://localhost:${port}/_broadcast`;
  const secret = req.getQuery("secret") || "";
  const total = parseInt(req.getQuery("total") || "120", 10);
  const interval = parseInt(req.getQuery("interval") || "500", 10);
  const stream = req.getQuery("stream") || "benchmark";
  const delay = parseInt(req.getQuery("delay") || "0", 10);

  res.cork(() => {
    res.writeHeader("Content-Type", "application/json");
    res.end(
      JSON.stringify({ status: "publishing", total, interval, stream, target, delay })
    );
  });

  void (async () => {
    if (delay > 0) {
      console.log(`Publisher waiting ${delay}s before starting...`);
      await new Promise((r) => setTimeout(r, delay * 1000));
    }
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (secret) headers["Authorization"] = `Bearer ${secret}`;
    for (let seq = 1; seq <= total; seq++) {
      const data = JSON.stringify({ seq, sentAt: Date.now(), text: `m${seq}` });
      try {
        await fetch(target, {
          method: "POST",
          headers,
          body: JSON.stringify({ stream, data }),
        });
      } catch {
        /* lost broadcasts surface in delivery rate */
      }
      await new Promise((r) => setTimeout(r, interval));
    }
    console.log(`Published ${total} messages to ${target}`);
  })();
});

app.listen(port, (token) => {
  if (token) {
    console.log(`uWebSockets.js server listening on :${port}`);
  } else {
    console.error(`Failed to listen on :${port}`);
    process.exit(1);
  }
});
