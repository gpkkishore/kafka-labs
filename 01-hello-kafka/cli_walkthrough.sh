#!/usr/bin/env bash
# Ch.2 — the same thing with the Kafka command line tools (what the book does in listings 2.x).
# Run from the repo root:  bash 01-hello-kafka/cli_walkthrough.sh
set -x
K=bin/kafka-cli
BS=kafka1:29092

# 1. Create a topic: 3 partitions, each copied to 3 brokers
$K kafka-topics --bootstrap-server $BS --create --if-not-exists --topic kinaction_helloworld --partitions 3 --replication-factor 3

# 2. Describe it: which broker LEADS each partition, where the replicas live, which are in-sync (ISR)
$K kafka-topics --bootstrap-server $BS --describe --topic kinaction_helloworld

# 3. Produce 3 lines (key:value)
printf 'k1:hello\nk2:from\nk1:the cli\n' | $K kafka-console-producer --bootstrap-server $BS --topic kinaction_helloworld \
  --property parse.key=true --property key.separator=:

# 4. Consume from the beginning, printing partition + offset + key
$K kafka-console-consumer --bootstrap-server $BS --topic kinaction_helloworld --from-beginning \
  --property print.partition=true --property print.offset=true --property print.key=true --timeout-ms 5000

# 5. Who is in the cluster? Which node is the active KRaft controller?
$K kafka-metadata-quorum --bootstrap-server $BS describe --status
$K kafka-broker-api-versions --bootstrap-server $BS | grep -E '^kafka'
