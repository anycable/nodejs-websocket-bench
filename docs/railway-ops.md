# Railway operations

Recipes for reproducing the 1M-connection, avalanche, and multi-shard
tests against Railway-hosted infrastructure. The bench-runner image is
the same on every shard; what changes is how many you deploy, how big
each one is, and which mutation you fire to disrupt them.

All commands assume `railway login` has been run and `RAILWAY_TOKEN`
is exported:

```bash
export RAILWAY_TOKEN=$(python3 -c "import json,os; \
  print(json.load(open(os.path.expanduser('~/.railway/config.json')))['user']['token'])")
```

## Find your project + environment + service IDs

```bash
railway status --json | jq '.environments.edges[0].node |
  {envId: .id,
   services: .serviceInstances.edges
     | map({name: .node.serviceName, id: .node.serviceId})}'
```

## Resize a service (vCPU + memory)

The CLI doesn't expose this; the GraphQL mutation does. Used to size each
test target the same as the comparison page (32 vCPU / 32 GB for the 1M
idle box, 0.5 GB / 1 vCPU for the avalanche cliff box).

```bash
curl -s -X POST https://backboard.railway.com/graphql/v2 \
  -H "Authorization: Bearer $RAILWAY_TOKEN" -H "Content-Type: application/json" \
  -d '{"query":"mutation Set($input: ServiceInstanceLimitsUpdateInput!) { serviceInstanceLimitsUpdate(input: $input) }",
       "variables":{"input":{"serviceId":"<svc-uuid>","environmentId":"<env-uuid>","memoryGB":32,"vCPUs":32}}}'
```

Limits apply on the next deployment. Trigger a redeploy with the same
image (no rebuild):

```bash
curl -s -X POST https://backboard.railway.com/graphql/v2 \
  -H "Authorization: Bearer $RAILWAY_TOKEN" -H "Content-Type: application/json" \
  -d '{"query":"mutation R($id: String!, $env: String!) { serviceInstanceRedeploy(serviceId: $id, environmentId: $env) }",
       "variables":{"id":"<svc-uuid>","env":"<env-uuid>"}}'
```

## Trigger an avalanche restart from a script

The avalanche tests need an externally-triggered server restart during
the test's `prearm` window. The bench-runner's avalanche probe waits for
the first disconnect; we trigger it via the same `serviceInstanceRedeploy`
mutation against the target server. The `bench/avalanche-railway-*.ts`
scripts print the exact `railway redeploy -s <svc> --yes` command, or
use the GraphQL form above for fully scripted runs.

Note: Railway's redeploy is zero-downtime (build new, swap, drain old).
For avalanche tests the swap is what creates the disruption, so expect a
30 to 90 s lag between firing the mutation and disconnects landing on
the bench-runner.

## Deploy code to all 50 shards in parallel

Each shard is its own Railway service running the same image. To push
new bench-runner code to all of them at once:

```bash
cd benchmark
for i in $(seq 2 50); do
  railway up --service "bench-runner-$i" --ci --detach 2>&1 | tail -1 &
done
wait
```

The first deploy populates Docker layer cache; subsequent shards
complete much faster. Use `railway status --json` to confirm all reached
`SUCCESS` before running the multi-shard test.

## Pause / downsize after tests (cost control)

Railway bills per-minute for allocated RAM and vCPU. After a test
session, downsize or pause the test-only services so you're not paying
for idle high-RAM allocations. Pausing keeps the service definition but
stops the container (and the billing).

Downsize via `serviceInstanceLimitsUpdate` (set `memoryGB` and `vCPUs`
to small values):

```bash
# Downsize uws-server from 32×32 to 0.5×1
curl -s -X POST https://backboard.railway.com/graphql/v2 \
  -H "Authorization: Bearer $RAILWAY_TOKEN" -H "Content-Type: application/json" \
  -d '{"query":"mutation Set($input: ServiceInstanceLimitsUpdateInput!) { serviceInstanceLimitsUpdate(input: $input) }",
       "variables":{"input":{"serviceId":"<svc-uuid>","environmentId":"<env-uuid>","memoryGB":0.5,"vCPUs":1}}}'
```

Or stop a service entirely. Two working ways:

```bash
# CLI (idempotent; "No deployments found" when already down)
railway down --service <name> --yes

# GraphQL: remove the latest deployment (halts billing, keeps the service shell)
curl -s -X POST https://backboard.railway.com/graphql/v2 \
  -H "Authorization: Bearer $RAILWAY_TOKEN" -H "Content-Type: application/json" \
  -d '{"query":"mutation S($id: String!) { deploymentRemove(id: $id) }",
       "variables":{"id":"<latest-deployment-id>"}}'
```

Do NOT use the `deploymentStop` mutation: it returns success and the
container keeps running (verified the hard way). After teardown, verify
externally — curl each public domain expecting 404/000 — and remember that
a limits change after a stop can respawn a fresh deployment, and that
deployment status `SUCCESS` is a build record which persists after a stop.
The `fleet-watchdog` GitHub Action (scripts/fleet-watchdog.mjs) opens an
issue if anything is left running between campaigns.

For the bench-runner shards specifically: they're ~64 MB each at idle,
so 50 of them is only ~3 GB total. The bigger savings come from the
high-RAM target services (`uws-server`, `anycable-go`, `socketio-server`
if they're sized 32×32 from a 1M run).

## Bench-runner auth (`BENCH_RUNNER_TOKEN`)

Every `/bench-*` and `/jobs/*` endpoint enforces a bearer-token check
when `BENCH_RUNNER_TOKEN` is set in the bench-runner's env. The driver
scripts read the same env var and send `Authorization: Bearer <token>`
on every request, so on the driver side you just export it once:

```bash
export BENCH_RUNNER_TOKEN=$(openssl rand -hex 32)   # one-time, save somewhere
# set the same value on every bench-runner service (and every shard)
# via Railway's UI or the GraphQL variableUpsert mutation.
```

`/health` stays open so Railway probes don't need the token. Leaving
`BENCH_RUNNER_TOKEN` empty disables the gate, which is only safe on an
internal-only deployment with no public domain.

## Server-side memory and CPU

`bench-runner` doesn't itself measure server resources. Use the
`railway-metrics.ts` helper to pull memory and CPU for each service over
the test window from Railway's GraphQL API:

```bash
PROJECT_ID=<project-id> SERVICE_ID=<service-id> SERVICE_NAME=anycable-go \
  START_DATE=2026-05-01T08:38:00Z END_DATE=2026-05-01T08:42:04Z SAMPLE_RATE=30 \
  npm run bench:metrics
```

Authentication uses `RAILWAY_TOKEN` if set, otherwise reads
`~/.railway/config.json` (the file the `railway` CLI writes after
`railway login`).
