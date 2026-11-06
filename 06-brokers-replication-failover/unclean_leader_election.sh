#!/usr/bin/env bash
# DG ch.6 "Unclean leader election" — availability vs consistency.
#  unclean.leader.election.enable=false (default): if all ISR replicas are gone, the partition stays OFFLINE
#  until an ISR member returns  -> no data loss, but downtime.
#  =true: an OUT-OF-SYNC replica may become leader -> partition is back fast, but records it never copied are LOST.
set -uo pipefail
cd "$(dirname "$0")/.."
K=bin/kafka-cli; BS=kafka1:29092; T=kinaction_unclean
trap 'docker start kafka1 kafka2 kafka3 >/dev/null 2>&1' EXIT
$K kafka-topics --bootstrap-server $BS --delete --topic $T 2>/dev/null; sleep 2
$K kafka-topics --bootstrap-server $BS --create --topic $T --partitions 1 --replica-assignment 2:3 --config min.insync.replicas=1
$K kafka-topics --bootstrap-server $BS --describe --topic $T
echo "--- stop follower kafka3, then write 5 records that ONLY broker 2 has"
docker stop kafka3 >/dev/null; sleep 12
seq 1 5 | $K kafka-console-producer --bootstrap-server $BS --topic $T --producer-property acks=1
echo "--- now stop the leader kafka2 and start the stale kafka3"
docker stop kafka2 >/dev/null; docker start kafka3 >/dev/null; sleep 20
$K kafka-topics --bootstrap-server $BS --describe --topic $T     # Leader: none  (offline, waiting for broker 2)
echo "--- enable unclean election for this topic -> kafka3 becomes leader WITHOUT records 1..5"
$K kafka-configs --bootstrap-server $BS --alter --entity-type topics --entity-name $T --add-config unclean.leader.election.enable=true
sleep 10
$K kafka-topics --bootstrap-server $BS --describe --topic $T
$K kafka-get-offsets --bootstrap-server $BS --topic $T              # end offset 0 -> the 5 records are GONE
docker start kafka2 >/dev/null
echo "When kafka2 returns it TRUNCATES its log to match the new leader: the loss is permanent."
