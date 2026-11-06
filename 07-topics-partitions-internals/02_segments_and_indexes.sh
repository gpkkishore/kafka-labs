#!/usr/bin/env bash
# DG ch.5 "File management / Indexes", AKiA 8.3: segments roll, indexes are sparse.
#   bash 07-topics-partitions-internals/02_segments_and_indexes.sh
set -euo pipefail
cd "$(dirname "$0")/.."
K=bin/kafka-cli; BS=kafka1:29092; T=kinaction_segments
$K kafka-topics --bootstrap-server $BS --delete --topic $T >/dev/null 2>&1 || true; sleep 2
# tiny segments (2 KB) and an index entry every 256 bytes so you can see everything
$K kafka-topics --bootstrap-server $BS --create --topic $T --partitions 1 --replication-factor 3 \
  --config segment.bytes=2048 --config index.interval.bytes=256
seq -f "record-number-%03g-with-some-padding-xxxxxxxxxxxxxxxx" 1 120 | \
  $K kafka-console-producer --bootstrap-server $BS --topic $T --batch-size 1 >/dev/null
echo "=== 120 records x ~60B with segment.bytes=2048 -> several segments, named by base offset"
docker exec kafka1 ls -la /var/lib/kafka/data/$T-0 | grep -E '\.(log|index|timeindex)$'
SEG=$(docker exec kafka1 sh -c "ls /var/lib/kafka/data/$T-0/*.log | sed -n 2p")
echo; echo "=== kafka-dump-log on 2nd segment $SEG (batches; add --print-data-log for payloads)"
$K kafka-dump-log --files "$SEG" | head -8
echo; echo "=== its offset index: relative offset -> byte position. SPARSE: one entry per ~index.interval.bytes"
$K kafka-dump-log --files "${SEG%.log}.index" | head -8
echo; echo "=== time index: timestamp -> offset"
$K kafka-dump-log --files "${SEG%.log}.timeindex" | head -5
cat <<'TXT'

How a fetch for offset N is served:
  1. pick the segment whose base offset <= N (binary search over segment file names, kept in a skip-list in memory)
  2. binary-search that segment's .index for the largest entry <= N  -> byte position
  3. scan forward in the .log from that position to offset N, then sendfile() the bytes to the socket (zero copy)
Only the LAST ("active") segment is written to. Retention & compaction work on whole CLOSED segments.
TXT
