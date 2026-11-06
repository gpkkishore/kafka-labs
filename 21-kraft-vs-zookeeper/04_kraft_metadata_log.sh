#!/usr/bin/env bash
# The ZooKeeper tree is replaced by an event LOG: topic __cluster_metadata (1 partition, Raft-replicated).
# Every change (topic created, partition leader moved, broker registered, config changed) is a record.
# Usage (repo root):  bash 21-kraft-vs-zookeeper/04_kraft_metadata_log.sh [topic-to-show]
set -euo pipefail
cd "$(dirname "$0")/.."
K=bin/kafka-cli
TOPIC=${1:-kinaction_helloworld}
DIR=/var/lib/kafka/data/__cluster_metadata-0
echo "### files of the metadata log (segments, quorum-state, snapshots *.checkpoint)"
$K ls -la $DIR
SEG=$($K sh -c "ls $DIR/*.log | tail -1" | tr -d '\r')

echo; echo "### record types in $SEG (count)"
$K kafka-dump-log --cluster-metadata-decoder --files "$SEG" 2>/dev/null | grep -oE '"type":"[A-Z_]+"' | sort | uniq -c | sort -rn

echo; echo "### last 12 decoded metadata records, ignoring NO_OP heartbeats"
$K kafka-dump-log --cluster-metadata-decoder --files "$SEG" 2>/dev/null | grep -E '^\| offset' \
  | grep -v NO_OP_RECORD | sed -E 's/.*payload: //' | tail -12

# kafka-metadata-shell refuses to open a directory a running broker has locked -> work on a COPY.
$K sh -c "rm -rf /tmp/mdcopy && mkdir -p /tmp/mdcopy/__cluster_metadata-0 && cp $DIR/*.log $DIR/*.checkpoint /tmp/mdcopy/__cluster_metadata-0/ 2>/dev/null || true"
SRC=$($K sh -c "ls /tmp/mdcopy/__cluster_metadata-0/*.checkpoint 2>/dev/null | tail -1 || ls /tmp/mdcopy/__cluster_metadata-0/*.log | tail -1" | tr -d '\r')
[ -z "$SRC" ] && SRC=$($K sh -c "ls /tmp/mdcopy/__cluster_metadata-0/*.log | tail -1" | tr -d '\r')
shell() { echo; echo "\$ kafka-metadata-shell $*"; $K kafka-metadata-shell --snapshot "$SRC" "$@" 2>/dev/null; }
echo; echo "### kafka-metadata-shell: a ZooKeeper-like TREE view rebuilt from the log ($SRC)"
shell ls /image
shell ls /image/cluster/brokers
shell ls /image/topics/byName
shell tree /image/topics/byName/"$TOPIC"
