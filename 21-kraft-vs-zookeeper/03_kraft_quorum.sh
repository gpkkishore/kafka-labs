#!/usr/bin/env bash
# KRaft mode (core lab cluster): the controllers form a Raft quorum. No ZooKeeper anywhere. (AKiA ch.7.1)
# Usage (repo root):  bash 21-kraft-vs-zookeeper/03_kraft_quorum.sh
set -euo pipefail
cd "$(dirname "$0")/.."
echo "### quorum status (LeaderId = active controller, HighWatermark = metadata log offset)"
bin/kafka-cli kafka-metadata-quorum --bootstrap-server kafka1:29092 describe --status
echo; echo "### replication of the metadata log across voters (LogEndOffset / Lag per node)"
bin/kafka-cli kafka-metadata-quorum --bootstrap-server kafka1:29092 describe --replication
