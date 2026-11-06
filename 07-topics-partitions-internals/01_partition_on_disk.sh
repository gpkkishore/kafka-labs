#!/usr/bin/env bash
# KiA 7.2.1 "Partition location" / DG ch.5 "Physical storage": what a partition looks like on a broker's disk.
#   bash 07-topics-partitions-internals/01_partition_on_disk.sh
set -euo pipefail
cd "$(dirname "$0")/.."
K=bin/kafka-cli; BS=kafka1:29092; T=kinaction_internals
$K kafka-topics --bootstrap-server $BS --create --if-not-exists --topic $T --partitions 3 --replication-factor 3
printf 'a:apple\nb:banana\nc:cherry\na:avocado\n' | $K kafka-console-producer --bootstrap-server $BS --topic $T \
  --property parse.key=true --property key.separator=:

echo; echo "=== log.dirs on kafka1: one DIRECTORY per partition replica (<topic>-<partition>) + checkpoint files"
docker exec kafka1 sh -c 'ls -1 /var/lib/kafka/data | grep -v "^__consumer_offsets" | head -40; echo "... + 50 __consumer_offsets-N dirs"'
for P in 0 1 2; do
  echo; echo "=== inside $T-$P   (keys hash to partitions, so some may hold no data yet)"
  docker exec kafka1 ls -la /var/lib/kafka/data/$T-$P | tail -n +4
done
cat <<'TXT'

  00000000000000000000.log        the records (segment named after its FIRST offset = base offset)
  00000000000000000000.index      sparse offset -> byte position index (memory-mapped)
  00000000000000000000.timeindex  sparse timestamp -> offset index (offsets_for_times)
  leader-epoch-checkpoint         epoch -> start offset history (used to truncate safely after leader change)
  partition.metadata              topic id (UUID) — detects a deleted+recreated topic with the same name
  (the ACTIVE segment's .index/.timeindex are pre-allocated to 10 MB and trimmed when the segment rolls)

  broker-level files:
  meta.properties                    node.id + cluster.id of this log dir
  recovery-point-offset-checkpoint   up to which offset each partition is flushed (no recovery needed below)
  replication-offset-checkpoint      high watermark of every partition
  log-start-offset-checkpoint        first retained offset (after deleteRecords / retention)
  cleaner-offset-checkpoint          how far compaction got per compacted partition
  __cluster_metadata-0               KRaft metadata log (replaces ZooKeeper)
TXT
echo "=== checkpoint file contents (version, count, then 'topic partition offset')"
docker exec kafka1 sh -c "grep $T /var/lib/kafka/data/replication-offset-checkpoint /var/lib/kafka/data/recovery-point-offset-checkpoint || true"
