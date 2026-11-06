#!/usr/bin/env bash
# Look INSIDE the log: transactional batches + COMMIT/ABORT control records (run 01 first).
#   bash 13-transactions-exactly-once/05_dump_control_records.sh
set -euo pipefail
cd "$(dirname "$0")/.."
LEADER=$(bin/kafka-cli kafka-topics --bootstrap-server kafka1:29092 --describe --topic tx_payments \
         | awk '/Partition: 0/ {for(i=1;i<=NF;i++) if($i=="Leader:") print $(i+1)}')
echo "== tx_payments-0 on leader kafka$LEADER"
BROKER=kafka$LEADER bin/kafka-cli kafka-dump-log --print-data-log \
  --files /var/lib/kafka/data/tx_payments-0/00000000000000000000.log \
  | grep -E 'baseOffset|endTxnMarker|payload' | sed -E 's/ (CreateTime|crc|size|compresscodec|magic|position|lastOffsetDelta|deleteHorizonMs|partitionLeaderEpoch|isvalid|keySize|valueSize|sequence|headerKeys): [^ ]+//g'
echo
echo "== open/ongoing transactions known to the coordinators"
bin/kafka-cli kafka-transactions --bootstrap-server kafka1:29092 list
echo
echo "== state of transactional.id payments-service-1"
bin/kafka-cli kafka-transactions --bootstrap-server kafka1:29092 describe --transactional-id payments-service-1
