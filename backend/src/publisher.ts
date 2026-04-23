export {};
// Publisher: sends sequential numbered messages via HTTP POST.
// Uses a single persistent HTTP agent to avoid port exhaustion.

import http from "http";
import https from "https";

const broadcastUrl = process.env.BROADCAST_URL || "http://localhost:8090/_broadcast";
const stream = process.env.STREAM || "benchmark";
const intervalMs = parseInt(process.env.INTERVAL_MS || "200");
const totalMessages = parseInt(process.env.TOTAL_MESSAGES || "600");
const secret = process.env.BROADCAST_SECRET || "";

console.log(`Publishing ${totalMessages} messages to ${broadcastUrl} (stream: ${stream}, interval: ${intervalMs}ms)`);

// Use a persistent agent with keep-alive to reuse the same TCP connection
const isHttps = broadcastUrl.startsWith("https");
const agent = isHttps
  ? new https.Agent({ keepAlive: true, maxSockets: 1 })
  : new http.Agent({ keepAlive: true, maxSockets: 1 });

async function publish(seq: number): Promise<void> {
  const data = JSON.stringify({ seq, sentAt: Date.now(), text: `msg_${seq}` });
  const body = JSON.stringify({ stream, data });
  const url = new URL(broadcastUrl);

  return new Promise((resolve, reject) => {
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
          ...(secret ? { Authorization: `Bearer ${secret}` } : {}),
        },
      },
      (res) => {
        res.resume(); // drain
        resolve();
      }
    );
    req.on("error", (err) => {
      console.error(`Failed to publish seq ${seq}:`, err.message);
      resolve(); // don't crash, keep going
    });
    req.write(body);
    req.end();
  });
}

for (let seq = 1; seq <= totalMessages; seq++) {
  await publish(seq);
  if (seq % 100 === 0) console.log(`Published ${seq}/${totalMessages}`);
  await new Promise((r) => setTimeout(r, intervalMs));
}

agent.destroy();
console.log("Publishing complete");
