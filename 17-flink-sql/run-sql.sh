#!/usr/bin/env bash
# Submit a Flink SQL file to the session cluster (the init file with all CREATE TABLEs is loaded first).
#   bash 17-flink-sql/run-sql.sh 02_tumbling_window.sql
# INSERT INTO ... statements start a continuous streaming JOB and return immediately; see http://localhost:8085
set -euo pipefail
FILE="${1:?usage: run-sql.sh <file in 17-flink-sql/sql/>}"
docker exec -i flink-jobmanager /opt/flink/bin/sql-client.sh -i /sql/00_init_tables.sql -f "/sql/$(basename "$FILE")" 2>&1 \
  | grep -v -E '^WARNING: Unknown module' || true
echo "On failure: docker exec flink-jobmanager sh -c 'tail -50 /opt/flink/log/*sql-client*.log'"
