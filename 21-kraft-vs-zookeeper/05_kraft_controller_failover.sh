#!/usr/bin/env bash
# KRaft: stop the ACTIVE controller and watch the Raft quorum elect a new leader (usually < 1-2 s).
# The core cluster keeps working (RF=3, min.insync=2); the stopped node is started again at the end.
# Usage (repo root):  bash 21-kraft-vs-zookeeper/05_kraft_controller_failover.sh
set -euo pipefail
cd "$(dirname "$0")/.."
status() { BROKER=$1 bin/kafka-cli kafka-metadata-quorum --bootstrap-server "$1":29092 describe --status 2>/dev/null; }
LEADER=$(status kafka1 | awk '/^LeaderId/{print $2}')
EPOCH=$(status kafka1 | awk '/^LeaderEpoch/{print $2}')
echo "active controller: node $LEADER (epoch $EPOCH)"
OTHER=kafka1; [ "$LEADER" = 1 ] && OTHER=kafka2
echo "stopping kafka$LEADER ..."; T0=$(date +%s); docker stop "kafka$LEADER" >/dev/null
for i in $(seq 1 30); do
  L=$(status $OTHER | awk '/^LeaderId/{print $2}'); E=$(status $OTHER | awk '/^LeaderEpoch/{print $2}')
  echo "  t+$(( $(date +%s) - T0 ))s leader=$L epoch=$E"
  if [ -n "$L" ] && [ "$L" != "$LEADER" ] && [ "$L" != "-1" ]; then break; fi
  sleep 1
done
echo "starting kafka$LEADER again ..."; docker start "kafka$LEADER" >/dev/null
sleep 8; status $OTHER | grep -E 'LeaderId|LeaderEpoch|CurrentVoters'
echo "Epoch increased: a new Raft term. Controllers already have the full metadata log in memory -> no reload."
