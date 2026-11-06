#!/usr/bin/env bash
# AKiA 7.1 / KiA 6.2 — with KRaft, cluster metadata is itself a Kafka log: __cluster_metadata-0
# (a Raft-replicated log on the controller quorum). Everything ZooKeeper used to hold is a record here.
#   bash 07-topics-partitions-internals/05_kraft_cluster_metadata.sh
set -euo pipefail
cd "$(dirname "$0")/.."
K=bin/kafka-cli; BS=kafka1:29092
echo "=== quorum status: who is the active controller (Raft leader), followers' lag"
$K kafka-metadata-quorum --bootstrap-server $BS describe --status
$K kafka-metadata-quorum --bootstrap-server $BS describe --replication
echo; echo "=== files of the metadata log"
docker exec kafka1 ls -la /var/lib/kafka/data/__cluster_metadata-0
SEG=$(docker exec kafka1 sh -c 'ls /var/lib/kafka/data/__cluster_metadata-0/*.log | head -1')   # first segment has the bootstrap + topic creations
echo; echo "=== record types in the first metadata segment (counts)"
$K kafka-dump-log --cluster-metadata-decoder --files "$SEG" 2>/dev/null | grep -o '"type":"[A-Z_]*"' | sort | uniq -c | sort -rn
echo; echo "=== a few TopicRecord / PartitionRecord / RegisterBrokerRecord payloads"
$K kafka-dump-log --cluster-metadata-decoder --files "$SEG" 2>/dev/null | grep -E '"type":"(TOPIC_RECORD|PARTITION_RECORD|REGISTER_BROKER_RECORD|PARTITION_CHANGE_RECORD)"' | sed 's/.*payload: //' | head -6 | cut -c1-260
cat <<'TXT'

PARTITION_CHANGE_RECORD = leader/ISR changes (try it while running 06-brokers-replication-failover).
BROKER_REGISTRATION_CHANGE_RECORD / FENCE_BROKER_RECORD = broker liveness.
Snapshots (*.checkpoint files) compact this log periodically. Deeper dive: 21-kraft-vs-zookeeper.
TXT
