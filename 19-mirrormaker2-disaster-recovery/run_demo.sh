#!/usr/bin/env bash
# End-to-end DR demo. Run from repo root with the core cluster up (make up).
set -euo pipefail
cd "$(dirname "$0")/.."
PY=${PY:-.venv/bin/python}
D=19-mirrormaker2-disaster-recovery
docker compose -f $D/docker-compose.yml up -d
$PY $D/01_produce_on_primary.py -n 300
$PY $D/02_consume_on_primary.py --max 120
echo "waiting 30s for MM2 to mirror data + emit checkpoints..."; sleep 30
$PY $D/03_inspect_dr.py
$PY $D/04_failover_consumer.py --mode translate
echo "Tear down with: docker compose -f $D/docker-compose.yml down -v"
