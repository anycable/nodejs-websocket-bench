// Central registry of query params each bench endpoint understands.
//
// Why: the runner's parsers silently fall back to defaults for any key they
// don't read. A driver sending `interval` to an endpoint that reads
// `intervalMs` runs the whole test at the default rate with no error
// (this exact bug shipped once). The registry lets the server compute
// `unknownParams` for every request and echo them back, so the driver can
// fail fast instead of trusting a run that ignored its parameters.
//
// Keep in sync with the parsers: params.ts (jitter), throughputParamsFromQuery
// and whispersParamsFromQuery in server.ts, and the ad-hoc parsing in each
// handler. Adding a query param to a handler without registering it here
// makes every driver that sends it fail loudly, which is the point.

const INFRA = ["async"];

const JITTER = [
  "n",
  "duration",
  "jitter",
  "jitterMs",
  "msgs",
  "interval",
  "ramp",
  "stream",
  "samplesCap",
];

const THROUGHPUT = [
  "n",
  "total",
  "intervalMs",
  "ramp",
  "stream",
  "drain",
  "publisher",
  "publisherConcurrency",
  "samplesCap",
];

const WHISPERS = [
  "n",
  "rooms",
  "ramp",
  "interval",
  "duration",
  "payload",
  "roomPrefix",
  "samplesCap",
];

const IDLE = ["n", "hold", "ramp", "stream", "shard"];

const AVALANCHE = ["n", "ramp", "prearm", "recoveryWait", "stream"];

const ANYCABLE_TARGET = [
  "cableUrl",
  "broadcastUrl",
  "channel",
  "acProtocol",
];

const KNOWN: Record<string, string[]> = {
  "/bench-jitter-anycable": [
    ...JITTER,
    ...ANYCABLE_TARGET,
    "reconnectMode",
    "reconnectBaseMs",
    "clientLib",
  ],
  "/bench-jitter-anycable-traced": [
    ...JITTER,
    "cableUrl",
    "broadcastUrl",
    "traceSample",
  ],
  "/bench-jitter-socketio": [...JITTER, "serverUrl"],
  "/bench-jitter-socketio-csr": [...JITTER, "serverUrl"],
  "/bench-jitter-uws": [...JITTER, "wsUrl", "httpUrl"],
  "/bench-trace-anycable": [
    "cableUrl",
    "broadcastUrl",
    "n",
    "broadcasts",
    "intervalMs",
    "rampPerSec",
    "stream",
    "includeSpans",
  ],
  "/bench-idle-anycable": [...IDLE, "cableUrl", "channel", "acProtocol"],
  "/bench-idle-socketio": [...IDLE, "serverUrl"],
  "/bench-idle-uws": [...IDLE, "wsUrl"],
  "/bench-avalanche-socketio": [...AVALANCHE, "serverUrl"],
  "/bench-avalanche-anycable": [
    ...AVALANCHE,
    "cableUrl",
    "channel",
    "acProtocol",
    "clientLib",
    "reconnectMode",
    "reconnectBaseMs",
  ],
  "/bench-avalanche-uws": [...AVALANCHE, "wsUrl"],
  "/bench-deploy-impact-socketio": [
    "n",
    "ramp",
    "stream",
    "pubRate",
    "preDeploy",
    "postDeploy",
    "nodes",
  ],
  "/bench-deploy-impact-standalone-socketio": [
    "n",
    "ramp",
    "stream",
    "duration",
    "nodes",
  ],
  "/bench-deploy-impact-standalone-anycable": [
    "n",
    "ramp",
    "stream",
    "duration",
    "cableUrl",
  ],
  "/bench-whispers-anycable": [...WHISPERS, "cableUrl"],
  "/bench-whispers-socketio": [...WHISPERS, "serverUrl"],
  "/bench-whispers-uws": [...WHISPERS, "wsUrl"],
  "/bench-throughput-anycable": [
    ...THROUGHPUT,
    ...ANYCABLE_TARGET,
    "natsUrl",
    "natsSubject",
  ],
  "/bench-throughput-socketio": [...THROUGHPUT, "serverUrl"],
  "/bench-throughput-socketio-csr": [...THROUGHPUT, "serverUrl"],
  "/bench-throughput-anycable-cluster": [
    ...THROUGHPUT,
    "cableUrlA",
    "cableUrlB",
    "broadcastUrl",
    "natsUrl",
    "natsSubject",
  ],
  "/bench-throughput-socketio-redis": [
    ...THROUGHPUT,
    "subscriberUrlA",
    "subscriberUrlB",
  ],
  "/bench-throughput-uws": [...THROUGHPUT, "wsUrl", "httpUrl"],
  "/bench-benchi-anycable": [
    "c",
    "r",
    "d",
    "S",
    "s",
    "quiet",
    "drainTimeout",
    "maxInflight",
    "publishWorkers",
    "publishBatch",
    "tolerance",
    "seed",
  ],
};

const KNOWN_SETS: Record<string, Set<string>> = Object.fromEntries(
  Object.entries(KNOWN).map(([path, keys]) => [
    path,
    new Set([...keys, ...INFRA]),
  ]),
);

// Query keys the given endpoint would silently ignore. Empty array for
// endpoints not in the registry (nothing to check against).
export function unknownParamsFor(
  path: string,
  query: Record<string, unknown>,
): string[] {
  const known = KNOWN_SETS[path];
  if (!known) return [];
  return Object.keys(query).filter((k) => !known.has(k));
}
