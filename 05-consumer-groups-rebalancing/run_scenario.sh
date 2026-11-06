#!/usr/bin/env bash
# Automated rebalancing walkthrough. Interleaves the output of every member into ONE timeline.
#
#   bash 05-consumer-groups-rebalancing/run_scenario.sh                     # eager (range)
#   bash 05-consumer-groups-rebalancing/run_scenario.sh cooperative-sticky  # incremental
#   STATIC=1 bash 05-consumer-groups-rebalancing/run_scenario.sh            # static membership restart
#
# Open Grafana (Consumer groups row) + `python 05-consumer-groups-rebalancing/group_watch.py` side by side.
set -uo pipefail
cd "$(dirname "$0")/.."
PY=${PY:-.venv/bin/python}
STRATEGY=${1:-range}
STATIC=${STATIC:-0}
DIR=05-consumer-groups-rebalancing
RUN=$(mktemp -d)
GROUP="rebalance-demo-$STRATEGY-$$"
EXTRA=(--strategy "$STRATEGY" --group "$GROUP" --pid-dir "$RUN")
[ "$STATIC" = 1 ] && EXTRA+=(--static)

step() { printf '\n\033[1;97;44m ### %s \033[0m\n' "$*"; }
start() { local n=$1; shift; ($PY -u $DIR/consumer_worker.py --name "$n" "${EXTRA[@]}" "$@" 2>&1 | sed -u "s/^/[$n] /") & sleep 0.5; }
pidof() { cat "$RUN/$1.pid"; }
cleanup() { for f in "$RUN"/*.pid; do kill "$(cat "$f")" 2>/dev/null; done; kill "$PROD" 2>/dev/null; wait 2>/dev/null; rm -rf "$RUN"; }
trap cleanup EXIT

# process substitution (not a pipe) so that $! is the PYTHON process, not sed
$PY -u $DIR/producer_steady.py --rate 30 > >(sed -u 's/^/[producer] /') 2>&1 &
PROD=$!
sleep 3

step "1. A joins alone -> A gets all 6 partitions";                 start A; sleep 10
step "2. B joins -> rebalance: partitions split A/B";               start B; sleep 10
step "3. C joins -> 2 partitions each";                             start C; sleep 10
if [ "$STATIC" = 1 ]; then
  step "4. STATIC: kill -9 B and restart it within session.timeout (10s) -> NO rebalance, B gets the same partitions back"
  kill -9 "$(pidof B)"; sleep 4; start B; sleep 14
else
  step "4. B stops GRACEFULLY (SIGTERM -> LeaveGroup) -> rebalance happens immediately"
  kill -TERM "$(pidof B)"; sleep 10
  step "5. C CRASHES (kill -9) -> its partitions are NOT consumed until session.timeout.ms (10s) expires"
  kill -9 "$(pidof C)"; sleep 18
  step "6. D..H join -> 7 members but only 6 partitions -> one member sits IDLE (owns nothing)"
  start D; start E; start F; start G; start H; sleep 14
fi
step "done — final state"
$PY $DIR/group_watch.py --group "$GROUP" --once
