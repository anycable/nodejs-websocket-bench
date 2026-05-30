#!/bin/bash
# Socket.io + CSR test sequence (run after default Socket.io tests finish).
#
# Toggles SOCKETIO_CSR=1, redeploys socketio-server, runs jitter + throughput
# sweep, toggles back to 0 to leave the service in the default state.
#
# Run with: cd backend && bash ../.csr-test-queue.sh

set -e

BENCH="https://bench-runner-production.up.railway.app"
RESULTS_DIR="../tmp/v1.6.14-bench-results"

cd "$(dirname "$0")"

echo "=== Step 1: Toggle CSR=1 ==="
railway service link socketio-server 2>&1 > /dev/null
railway variables --set "SOCKETIO_CSR=1" 2>&1 | tail -2

echo ""
echo "=== Step 2: Redeploy socketio-server ==="
railway redeploy -s socketio-server --yes 2>&1 | tail -2

echo "  waiting 75s for redeploy..."
sleep 75

# Confirm CSR=1 is set
railway variables --json 2>&1 | python3 -c "
import json, sys
d = json.load(sys.stdin)
v = d.get('SOCKETIO_CSR','<unset>')
print(f'  confirmed SOCKETIO_CSR={v}')
if v != '1':
  print('  WARNING: CSR not set to 1, aborting'); exit(1)
"

echo ""
echo "=== Step 3: Jitter Socket.io+CSR @ 10K ==="
date
curl --max-time 360 -X POST -s \
  "$BENCH/bench-jitter-socketio-csr?n=10000&duration=200&msgs=120&interval=500&jitter=15&jitterMs=1000&ramp=300&stream=jitter-csr-$(date +%s)" \
  > "$RESULTS_DIR/09-jitter-socketio-csr-10k.json"
echo "  exit=$?"
python3 -c "
import json
try:
  d = json.load(open('$RESULTS_DIR/09-jitter-socketio-csr-10k.json'))
  print(f'  delivery: {d.get(\"deliveryRatePct\")}%  lost: {d.get(\"lostDeliveries\")}  csrResumes: {d.get(\"csrResumes\")}')
  lat = d.get('latencyRawMs',{})
  print(f'  latency p50/p95/p99/max: {lat.get(\"p50\")}/{lat.get(\"p95\")}/{lat.get(\"p99\")}/{lat.get(\"max\")} ms')
except Exception as e:
  print(f'  FAIL: {e}')
"

echo ""
echo "=== Step 4: Throughput sweep ==="
for INTERVAL_MS in 1000 100 10; do
  case $INTERVAL_MS in
    1000) LABEL=10K-deliv-s;;
    100) LABEL=100K-deliv-s;;
    10) LABEL=1M-deliv-s;;
  esac
  echo "--- Socket.io+CSR $LABEL ---"
  date
  curl --max-time 600 -X POST -s \
    "$BENCH/bench-throughput-socketio-csr?n=10000&total=100&intervalMs=${INTERVAL_MS}&ramp=200&drain=30&stream=tp-csr-${LABEL}-$(date +%s)" \
    > "$RESULTS_DIR/10-throughput-socketio-csr-${LABEL}.json"
  python3 -c "
import json
try:
  d = json.load(open('$RESULTS_DIR/10-throughput-socketio-csr-${LABEL}.json'))
  print(f'  delivery: {d.get(\"deliveryRatePct\")}%  throughput: {d.get(\"outboundDeliveriesPerSec\",0):,} deliv/sec')
except Exception as e:
  print(f'  FAIL: {e}')
"
done

echo ""
echo "=== Step 5: Reset SOCKETIO_CSR=0 ==="
railway variables --set "SOCKETIO_CSR=0" 2>&1 | tail -1
railway redeploy -s socketio-server --yes 2>&1 | tail -1
echo "  Restored. CSR=0 again."

echo ""
date
echo "=== All Socket.io+CSR tests done ==="
