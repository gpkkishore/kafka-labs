#!/usr/bin/env bash
# Show the broker settings that matter for performance (DG ch.2 "Broker Configuration", AKiA ch.6.3)
# Usage (repo root):  bash 20-performance-tuning/04_broker_config_inspect.sh [broker_id]
set -euo pipefail
cd "$(dirname "$0")/.."
ID=${1:-1}
bin/kafka-cli kafka-configs --bootstrap-server kafka1:29092 --describe --all --entity-type brokers --entity-name "$ID" \
 | grep -E ' (num\.network\.threads|num\.io\.threads|num\.replica\.fetchers|num\.recovery\.threads\.per\.data\.dir|socket\.(send|receive)\.buffer\.bytes|socket\.request\.max\.bytes|log\.segment\.bytes|log\.flush\.interval\.(messages|ms)|log\.retention\.(hours|bytes|check\.interval\.ms)|replica\.lag\.time\.max\.ms|replica\.fetch\.max\.bytes|message\.max\.bytes|queued\.max\.requests|compression\.type|num\.partitions|default\.replication\.factor|min\.insync\.replicas|unclean\.leader\.election\.enable|background\.threads)=' \
 | sed -E 's/ sensitive=.*//' | sort
echo
echo "Change dynamically (no restart), e.g.:"
echo "  bin/kafka-cli kafka-configs --bootstrap-server kafka1:29092 --alter --entity-type brokers --entity-default --add-config num.io.threads=16"
