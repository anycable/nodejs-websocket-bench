#!/usr/bin/env bash
# Latency sweep for Socket.io default, +CSR, uWS at n=1000 and n=10000.
# CSR requires SOCKETIO_CSR=1 + socketio-server redeploy; reset at end.
set -uo pipefail

BENCH_RUNNER_URL=https://bench-runner-production.up.railway.app
OUT=/Users/irinanazarova/Projects/anycable-web/tmp/v1.6.14-bench-results
cd /Users/irinanazarova/Projects/anycable-web/benchmark/backend

run_socketio() {
  local n=$1
  local tag=$2
  echo "=== Socket.io default n=$n ==="
  date
  N=$n TOTAL=100 INTERVAL_MS=100 RAMP_RATE=200 BENCH_RUNNER_URL=$BENCH_RUNNER_URL \
    npm run -s bench:throughput:socketio 2>&1 | tee "$OUT/$tag.log"
  echo
}

run_uws() {
  local n=$1
  local tag=$2
  echo "=== uWS n=$n ==="
  date
  N=$n TOTAL=100 INTERVAL_MS=100 RAMP_RATE=200 BENCH_RUNNER_URL=$BENCH_RUNNER_URL \
    npm run -s bench:throughput:uws 2>&1 | tee "$OUT/$tag.log"
  echo
}

run_csr() {
  local n=$1
  local tag=$2
  echo "=== Socket.io+CSR n=$n ==="
  date
  N=$n TOTAL=100 INTERVAL_MS=100 RAMP_RATE=200 BENCH_RUNNER_URL=$BENCH_RUNNER_URL \
    npm run -s bench:throughput:socketio-csr 2>&1 | tee "$OUT/$tag.log"
  echo
}

echo "=== Phase 1: Socket.io default + uWS (no toggle needed) ==="
run_socketio 1000 16-latency-socketio-default-1k
run_socketio 10000 17-latency-socketio-default-10k
run_uws 1000 18-latency-uws-1k
run_uws 10000 19-latency-uws-10k

echo "=== Phase 2: enable CSR + redeploy + run ==="
railway variable set SOCKETIO_CSR=1 --service socketio-server 2>&1 | tail -3
echo "waiting 75s for redeploy"
sleep 75
curl -s -m 10 https://socketio-server-production-da75.up.railway.app/health; echo

run_csr 1000 20-latency-socketio-csr-1k
run_csr 10000 21-latency-socketio-csr-10k

echo "=== Phase 3: reset CSR=0 ==="
railway variable delete SOCKETIO_CSR --service socketio-server 2>&1 | tail -3
railway redeploy --service socketio-server --yes 2>&1 | tail -3
echo "waiting 75s for reset redeploy"
sleep 75
curl -s -m 10 https://socketio-server-production-da75.up.railway.app/health; echo

date
echo "=== All latency tests done ==="
