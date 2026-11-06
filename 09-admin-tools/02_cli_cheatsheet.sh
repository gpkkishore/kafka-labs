#!/usr/bin/env bash
# DG ch.9 / KiA 9.1 — the CLI tools you'll use daily. Run section by section or all at once.
set -x
K=bin/kafka-cli; BS=kafka1:29092; T=kinaction_cli
cd "$(dirname "$0")/.."
# --- topics
$K kafka-topics --bootstrap-server $BS --create --if-not-exists --topic $T --partitions 3 --replication-factor 3 --config retention.ms=86400000
$K kafka-topics --bootstrap-server $BS --list --exclude-internal
$K kafka-topics --bootstrap-server $BS --describe --under-replicated-partitions     # empty = healthy
$K kafka-topics --bootstrap-server $BS --describe --unavailable-partitions
$K kafka-topics --bootstrap-server $BS --describe --at-min-isr-partitions
# --- configs (topic / broker / client quotas)
$K kafka-configs --bootstrap-server $BS --entity-type topics --entity-name $T --alter --add-config max.message.bytes=2097152
$K kafka-configs --bootstrap-server $BS --entity-type topics --entity-name $T --describe
$K kafka-configs --bootstrap-server $BS --entity-type brokers --entity-name 1 --describe            # dynamic overrides only
$K kafka-configs --bootstrap-server $BS --entity-type brokers --entity-default --alter --add-config log.cleaner.threads=2   # cluster-wide dynamic
$K kafka-configs --bootstrap-server $BS --entity-type topics --entity-name $T --alter --delete-config max.message.bytes
# --- data
printf 'k1:v1\nk2:v2\n' | $K kafka-console-producer --bootstrap-server $BS --topic $T --property parse.key=true --property key.separator=:
$K kafka-console-consumer --bootstrap-server $BS --topic $T --from-beginning --timeout-ms 4000 \
   --property print.timestamp=true --property print.key=true --property print.partition=true --property print.offset=true
$K kafka-get-offsets --bootstrap-server $BS --topic $T --time -1      # latest   (-2 = earliest)
# --- groups
$K kafka-consumer-groups --bootstrap-server $BS --list
$K kafka-consumer-groups --bootstrap-server $BS --describe --all-groups --state
# reset: --to-earliest | --to-latest | --to-offset N | --shift-by -N | --to-datetime 2026-01-01T00:00:00.000 | --by-duration PT10M
# $K kafka-consumer-groups --bootstrap-server $BS --group G --topic $T --reset-offsets --to-earliest --dry-run|--execute
# --- disks / logs
$K kafka-log-dirs --bootstrap-server $BS --describe --topic-list $T
# --- cluster
$K kafka-metadata-quorum --bootstrap-server $BS describe --status
$K kafka-features --bootstrap-server $BS describe
$K kafka-broker-api-versions --bootstrap-server $BS | grep -c '(id:'
# --- cleanup
$K kafka-topics --bootstrap-server $BS --delete --topic $T
