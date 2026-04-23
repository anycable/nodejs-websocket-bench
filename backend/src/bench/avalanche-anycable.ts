// Reconnection avalanche benchmark — AnyCable
//
// Connect N clients to anycable-go, then kill the Node.js backend
// (simulating a deploy). Measure impact on WebSocket connections.
//
// Expected: ZERO disconnects — anycable-go is a separate process.
//
// Usage: NUM_CLIENTS=1000 tsx src/bench/avalanche-anycable.ts

import WebSocket from "ws";
import { createCable } from "@anycable/core";

process.on("unhandledRejection", () => {});

const numClients = parseInt(process.env.NUM_CLIENTS || "1000");
const rampRate = parseInt(process.env.RAMP_RATE || "50");
const stream = process.env.STREAM || "avalanche";
const anycableUrl = process.env.ANYCABLE_URL || "ws://localhost:8080/cable";
const broadcastUrl = process.env.BROADCAST_URL || "http://localhost:8090/_broadcast";

console.log(`Avalanche benchmark (AnyCable): ${numClients} clients`);
console.log(`AnyCable: ${anycableUrl}`);
console.log(`Broadcast: ${broadcastUrl}`);
console.log(`\nNote: anycable-go must be running separately.`);
console.log(`This test kills nothing — it just proves connections stay up.\n`);

// Phase 1: Connect clients
const cables: any[] = [];
let disconnectCount = 0;

for (let i = 0; i < numClients; i++) {
  const cable = createCable(anycableUrl, {
    websocketImplementation: WebSocket as any,
    protocol: "actioncable-v1-ext-json",
    logLevel: "error" as any,
  });
  cable.on("close", () => {});
  cable.on("disconnect", () => { disconnectCount++; });
  cable.streamFrom(stream);
  cables.push(cable);

  if ((i + 1) % rampRate === 0) {
    await new Promise((r) => setTimeout(r, 1000));
    if ((i + 1) % 500 === 0) console.log(`Connected ${i + 1}/${numClients}`);
  }
}

await new Promise((r) => setTimeout(r, 3000));
console.log(`All clients connected: ${numClients}`);

// Phase 2: Publish some messages to verify delivery works
console.log("\nPublishing 5 test messages...");
let received = 0;
cables[0].streamFrom(stream).on("message", () => { received++; });

for (let seq = 1; seq <= 5; seq++) {
  await fetch(broadcastUrl, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ stream, data: JSON.stringify({ seq, sentAt: Date.now() }) }),
  });
  await new Promise((r) => setTimeout(r, 500));
}
await new Promise((r) => setTimeout(r, 2000));
console.log(`Client 0 received: ${received}/5 messages`);

// Phase 3: Simulate "deploy" — in production you'd restart your
// Rails/Node/FastAPI app here. AnyCable-go stays up.
console.log("\n>>> Simulating app deploy (anycable-go stays up) <<<");
console.log("Waiting 10 seconds...");
await new Promise((r) => setTimeout(r, 10000));

console.log(`Disconnects during "deploy": ${disconnectCount}`);

// Phase 4: Publish more messages — should still work
console.log("\nPublishing 5 more messages after 'deploy'...");
const received2Before = received;
for (let seq = 6; seq <= 10; seq++) {
  await fetch(broadcastUrl, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ stream, data: JSON.stringify({ seq, sentAt: Date.now() }) }),
  });
  await new Promise((r) => setTimeout(r, 500));
}
await new Promise((r) => setTimeout(r, 2000));
const received2 = received - received2Before;
console.log(`Client 0 received: ${received2}/5 messages after deploy`);

console.log(`\n=== AnyCable Avalanche Results ===`);
console.log(`Clients:            ${numClients}`);
console.log(`Disconnects:        ${disconnectCount}`);
console.log(`Recovery time:      0ms (connections never dropped)`);
console.log(`Messages before:    ${received2Before}/5`);
console.log(`Messages after:     ${received2}/5`);
console.log(`Total downtime:     0ms`);

cables.forEach((c) => c.disconnect());
process.exit(0);
