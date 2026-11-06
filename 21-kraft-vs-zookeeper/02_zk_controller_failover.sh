#!/usr/bin/env bash
# ZK mode: the controller is just a broker that won the race to create the /controller ephemeral znode.
# Stop it -> its ZK session expires -> znode deleted -> other brokers race again. (DG ch.5 "The Controller")
# Usage (repo root):  bash 21-kraft-vs-zookeeper/02_zk_controller_failover.sh
set -uo pipefail
ctl() { docker exec zookeeper zookeeper-shell localhost:2181 get /controller 2>/dev/null | grep brokerid; }
echo "controller now: $(ctl)"
ID=$(ctl | sed -E 's/.*"brokerid":([0-9]+).*/\1/')
echo "stopping zk-kafka$ID ..."; docker stop "zk-kafka$ID" >/dev/null
for i in $(seq 1 20); do
  sleep 1; c=$(ctl); echo "  t+${i}s /controller = ${c:-<none>}"
  [ -n "$c" ] && ! echo "$c" | grep -q "\"brokerid\":$ID" && break
done
echo "epoch now: $(docker exec zookeeper zookeeper-shell localhost:2181 get /controller_epoch 2>/dev/null | tail -1)"
echo "starting zk-kafka$ID again"; docker start "zk-kafka$ID" >/dev/null
echo "The new controller must load ALL metadata from ZooKeeper before acting -> slow with many partitions."
