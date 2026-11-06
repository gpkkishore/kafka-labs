#!/usr/bin/env bash
# Kafka's own load generator (Java client) — the numbers ops teams quote. AKiA ch.6.2 / DG ch.2
# Usage (repo root):  bash 20-performance-tuning/01_producer_perf_test.sh [num_records]
set -euo pipefail
cd "$(dirname "$0")/.."
N=${1:-300000}
K=bin/kafka-cli
BS=kafka1:29092,kafka2:29092,kafka3:29092
$K kafka-topics --bootstrap-server kafka1:29092 --create --if-not-exists --topic perf-test --partitions 6 --replication-factor 3 >/dev/null

run() {
  local label="$1"; shift
  printf '\n### %-45s ' "$label"
  # last line of perf-test = summary: records/sec, MB/sec, avg latency, max latency, percentiles
  $K kafka-producer-perf-test --topic perf-test --num-records "$N" --record-size 1024 --throughput -1 \
     --producer-props bootstrap.servers=$BS "$@" | tail -1
}
run "acks=1   linger=0  batch=16K  none"   acks=1   linger.ms=0  batch.size=16384  compression.type=none
run "acks=all linger=0  batch=16K  none"   acks=all linger.ms=0  batch.size=16384  compression.type=none
run "acks=all linger=10 batch=128K none"   acks=all linger.ms=10 batch.size=131072 compression.type=none
run "acks=all linger=10 batch=128K lz4"    acks=all linger.ms=10 batch.size=131072 compression.type=lz4
run "acks=all linger=10 batch=128K zstd"   acks=all linger.ms=10 batch.size=131072 compression.type=zstd
echo
echo "NOTE: perf-test payloads are random bytes -> compress poorly. Real JSON compresses 4-10x (see 03-producers/04_*)."
