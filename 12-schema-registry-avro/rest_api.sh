#!/usr/bin/env bash
# KiA 11.3.1 — the Schema Registry REST API with curl.  bash 12-schema-registry-avro/rest_api.sh
SR=${SCHEMA_REGISTRY_URL:-http://localhost:8081}
J='-H Content-Type:application/vnd.schemaregistry.v1+json'
set -x
curl -s $SR/subjects; echo                                     # all subjects (<topic>-key / <topic>-value)
curl -s $SR/subjects/sr_orders-value/versions; echo            # version numbers of a subject
curl -s $SR/subjects/sr_orders-value/versions/latest; echo     # latest schema + id
curl -s $SR/schemas/ids/1; echo                                # schema by global id (what consumers do)
curl -s $SR/config; echo                                       # global compatibility level
curl -s $SR/config/sr_orders-value; echo                       # per-subject override (404 if none)
# register a new schema version manually
curl -s $J -X POST $SR/subjects/sr_manual-value/versions \
  -d '{"schema": "{\"type\":\"record\",\"name\":\"Ping\",\"fields\":[{\"name\":\"ts\",\"type\":\"long\"}]}"}'; echo
# test compatibility WITHOUT registering
curl -s $J -X POST $SR/compatibility/subjects/sr_manual-value/versions/latest \
  -d '{"schema": "{\"type\":\"record\",\"name\":\"Ping\",\"fields\":[{\"name\":\"ts\",\"type\":\"long\"},{\"name\":\"host\",\"type\":\"string\"}]}"}'; echo
# change compatibility for one subject
curl -s $J -X PUT $SR/config/sr_manual-value -d '{"compatibility": "FULL"}'; echo
# soft delete then hard delete a subject
curl -s -X DELETE $SR/subjects/sr_manual-value; echo
curl -s -X DELETE "$SR/subjects/sr_manual-value?permanent=true"; echo
