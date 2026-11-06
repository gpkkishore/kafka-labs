#!/usr/bin/env bash
# Broker failure walkthrough (KiA 6.4, DG ch.5 "Replication" + ch.6, AKiA 5.3 / 8.4)
#   bash 06-brokers-replication-failover/failover_scenario.sh
# Keep Grafana open: "Cluster health" row (Brokers up, Under-replicated partitions, Leader partitions per broker).
set -uo pipefail
cd "$(dirname "$0")/.."
PY=${PY:-.venv/bin/python}
D=06-brokers-replication-failover
step() { printf '\n\033[1;97;44m ### %s \033[0m\n' "$*"; }
watch1() { $PY $D/01_watch_leaders.py --once; }

# process substitution (not a pipe) so that $! is the PYTHON process, not sed
$PY -u $D/02_durable_producer.py --rate 40 > >(sed -u 's/^/[producer] /') 2>&1 &
PROD=$!
trap 'kill $PROD 2>/dev/null; docker start kafka1 kafka2 kafka3 >/dev/null' EXIT
bin/kafka-cli kafka-leader-election --bootstrap-server kafka1:29092 --election-type PREFERRED --all-topic-partitions >/dev/null 2>&1
sleep 4

step "1. Healthy: every partition has 3 replicas in ISR, leaders spread over brokers 1,2,3"; watch1
step "2. docker stop kafka2  -> leaders on broker 2 move, ISR shrinks to 2 (>= min.insync.replicas=2: writes continue)"
docker stop kafka2 >/dev/null; sleep 8; watch1
step "3. docker start kafka2 -> it catches up from the leaders and re-joins the ISR (but is NOT leader of anything yet)"
docker start kafka2 >/dev/null; sleep 25; watch1
step "4. Preferred leader election -> leadership goes back to the first replica in each list (balanced again)"
bin/kafka-cli kafka-leader-election --bootstrap-server kafka1:29092 --election-type PREFERRED --all-topic-partitions 2>&1 | tail -2
sleep 3; watch1
step "5. Stop TWO brokers -> ISR=1 < min.insync.replicas=2 => acks=all writes fail with NOT_ENOUGH_REPLICAS (no silent loss)"
docker stop kafka2 kafka3 >/dev/null; sleep 15; watch1
step "6. Bring them back -> producer recovers (queued records are retried and acked; anything older than delivery.timeout.ms=120s would fail loudly)"
docker start kafka2 kafka3 >/dev/null; sleep 30; watch1
kill -TERM $PROD 2>/dev/null; wait $PROD 2>/dev/null   # producer flushes, then exits
step "7. Verify: every ACKNOWLEDGED record must be in the topic"
$PY $D/03_verify_no_data_loss.py
