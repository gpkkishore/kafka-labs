#!/usr/bin/env bash
# KiA 9.1.3 — REST Proxy v2/v3 API walkthrough. Start it first:
#   docker compose -f 09-admin-tools/docker-compose.rest-proxy.yml up -d
set -euo pipefail
R=http://localhost:8082
for i in $(seq 1 30); do curl -sf $R/v3/clusters >/dev/null && break; sleep 2; done
CID=$(curl -s $R/v3/clusters | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"][0]["cluster_id"])')
echo "cluster id: $CID"
echo "--- v3: list brokers / topics"
curl -s $R/v3/clusters/$CID/brokers | python3 -c 'import json,sys;[print(" broker",b["broker_id"],b["host"],b["rack"]) for b in json.load(sys.stdin)["data"]]'
curl -s -X POST -H 'Content-Type: application/json' $R/v3/clusters/$CID/topics \
     -d '{"topic_name":"kinaction_rest","partitions_count":3,"replication_factor":3}' >/dev/null || true
echo "--- v3: produce a JSON record"
curl -s -X POST -H 'Content-Type: application/json' $R/v3/clusters/$CID/topics/kinaction_rest/records \
     -d '{"key":{"type":"JSON","data":"order-1"},"value":{"type":"JSON","data":{"amount":42}}}'; echo
echo "--- v2: create a consumer instance, subscribe, fetch, delete"
G=rest_group; C=rest_consumer
curl -s -X POST -H 'Content-Type: application/vnd.kafka.v2+json' $R/consumers/$G \
     -d "{\"name\":\"$C\",\"format\":\"json\",\"auto.offset.reset\":\"earliest\"}" >/dev/null
curl -s -X POST -H 'Content-Type: application/vnd.kafka.v2+json' $R/consumers/$G/instances/$C/subscription -d '{"topics":["kinaction_rest"]}'
# the first fetches only join the group / fetch metadata -> poll until records arrive
for i in $(seq 1 15); do
  OUT=$(curl -s -H 'Accept: application/vnd.kafka.json.v2+json' $R/consumers/$G/instances/$C/records)
  [ "$OUT" != "[]" ] && break; sleep 2
done
echo "$OUT"
curl -s -X DELETE -H 'Content-Type: application/vnd.kafka.v2+json' $R/consumers/$G/instances/$C
echo "REST Proxy is stateful per consumer instance -> stick clients to one proxy node behind a load balancer."
