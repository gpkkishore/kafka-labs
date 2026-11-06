#!/usr/bin/env bash
# End-to-end latency = produce -> broker -> replicate -> consumer receives. (DG ch.6, AKiA ch.6)
# Usage (repo root):  bash 20-performance-tuning/03_end_to_end_latency.sh
set -euo pipefail
cd "$(dirname "$0")/.."
K=bin/kafka-cli
$K kafka-topics --bootstrap-server kafka1:29092 --create --if-not-exists --topic perf-e2e --partitions 1 --replication-factor 3 >/dev/null
for acks in 1 all; do
  echo "### acks=$acks (2000 x 512B, one at a time)"
  $K kafka-run-class org.apache.kafka.tools.EndToEndLatency kafka1:29092 perf-e2e 2000 "$acks" 512 2>/dev/null | tail -2
done
