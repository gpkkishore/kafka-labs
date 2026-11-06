#!/usr/bin/env bash
# Walk the ZooKeeper tree that a ZK-mode Kafka cluster uses (KiA ch.6.2, DG ch.5 "Cluster Membership").
# Usage (repo root):  bash 21-kraft-vs-zookeeper/01_zk_inspect.sh
set -uo pipefail
zk() { echo; echo "\$ zookeeper-shell $*"; docker exec zookeeper zookeeper-shell localhost:2181 "$@" 2>/dev/null | grep -vE '^(Connecting|WATCHER|WatchedEvent|$)'; }

docker exec -e KAFKA_OPTS= zk-kafka1 kafka-topics --bootstrap-server zk-kafka1:29092 --create --if-not-exists \
  --topic zk-demo --partitions 2 --replication-factor 2 >/dev/null

zk ls /                                   # top level znodes Kafka created
zk ls /brokers/ids                        # EPHEMERAL znode per live broker (disappears when its session dies)
zk get /brokers/ids/1                     # host/port/listeners/rack of broker 1
zk get /controller                        # EPHEMERAL: which broker is the controller (first to create it wins)
zk get /controller_epoch                  # bumps on every controller change (fences zombie controllers)
zk ls /brokers/topics                     # all topics
zk get /brokers/topics/zk-demo            # partition -> replica assignment
zk get /brokers/topics/zk-demo/partitions/0/state   # leader, ISR, leader_epoch, controller_epoch
zk ls /config/topics                      # per-topic config overrides
zk get /cluster/id
