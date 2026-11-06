#!/usr/bin/env bash
# DG ch.9 / KiA 6.5.2 "Adding a broker": move partition replicas between brokers with kafka-reassign-partitions.
# Here we move kinaction_reassign (RF=2) off broker 3 completely, then back.
set -euo pipefail
cd "$(dirname "$0")/.."
K=bin/kafka-cli; BS=kafka1:29092
$K kafka-topics --bootstrap-server $BS --create --if-not-exists --topic kinaction_reassign --partitions 3 --replication-factor 2
$K kafka-topics --bootstrap-server $BS --describe --topic kinaction_reassign

echo '{"topics":[{"topic":"kinaction_reassign"}],"version":1}' | docker exec -i kafka1 sh -c 'cat > /tmp/topics.json'
echo "--- 1) let Kafka PROPOSE a plan that only uses brokers 1,2"
$K kafka-reassign-partitions --bootstrap-server $BS --topics-to-move-json-file /tmp/topics.json --broker-list 1,2 --generate \
  | tee /dev/stderr | sed -n '/Proposed/{n;p;}' | docker exec -i kafka1 sh -c 'cat > /tmp/plan.json'
echo "--- 2) execute it (with a replication throttle of 10 MB/s so moves don't starve clients)"
$K kafka-reassign-partitions --bootstrap-server $BS --reassignment-json-file /tmp/plan.json --execute --throttle 10000000
sleep 5
echo "--- 3) verify (also REMOVES the throttle once complete)"
$K kafka-reassign-partitions --bootstrap-server $BS --reassignment-json-file /tmp/plan.json --verify
$K kafka-topics --bootstrap-server $BS --describe --topic kinaction_reassign
echo "Broker 3 no longer holds any replica of kinaction_reassign. (To decommission a broker you move ALL its replicas this way.)"
