#!/usr/bin/env bash
# Consumer throughput with different fetch sizes. Run 01 first (it fills topic perf-test).
# Usage (repo root):  bash 20-performance-tuning/02_consumer_perf_test.sh
set -euo pipefail
cd "$(dirname "$0")/.."
K=bin/kafka-cli
BS=kafka1:29092,kafka2:29092,kafka3:29092
N=${1:-300000}
for cfg in "fetch.min.bytes=1 max.partition.fetch.bytes=65536" \
           "fetch.min.bytes=1 max.partition.fetch.bytes=1048576" \
           "fetch.min.bytes=1048576 fetch.max.wait.ms=500 max.partition.fetch.bytes=4194304"; do
  echo "### $cfg"
  # write the properties file INSIDE the container (readable by the container user)
  echo "$cfg" | tr ' ' '\n' | $K sh -c 'cat > /tmp/perf-consumer.properties'
  $K kafka-consumer-perf-test --bootstrap-server $BS --topic perf-test --messages "$N" \
     --group "perf-$RANDOM" --consumer.config /tmp/perf-consumer.properties --timeout 60000 2>&1 | grep -E '^[0-9]{4}-' | tail -1 || true
done
echo "Columns: start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec, rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec"
