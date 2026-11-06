#!/usr/bin/env bash
# ksqlDB REST API (what the CLI uses).  bash 14-stream-processing-ksqldb/ksqldb/rest_examples.sh
K=${KSQL_URL:-http://localhost:8088}
H='Content-Type: application/vnd.ksql.v1+json; charset=utf-8'
set -x
curl -s -H "$H" $K/ksql -d '{"ksql":"SHOW QUERIES;"}' | python3 -m json.tool | head -40
# pull query via /query (returns immediately)
curl -s -H "$H" $K/query -d '{"ksql":"SELECT * FROM user_activity WHERE user_id = '\''u1'\'';","streamsProperties":{}}'; echo
# push query with LIMIT (stops after 5 rows)
curl -s -H "$H" $K/query -d '{"ksql":"SELECT * FROM purchases EMIT CHANGES LIMIT 5;","streamsProperties":{"ksql.streams.auto.offset.reset":"earliest"}}'; echo
