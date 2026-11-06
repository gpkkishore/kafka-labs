#!/usr/bin/env python3
"""AKiA 11.5 / DG ch.7 "Running Connect" — a guided tour of the Kafka Connect REST API.

    docker compose -f 15-kafka-connect/docker-compose.yml up -d --build
    python 15-kafka-connect/01_rest_api_tour.py

Everything here can also be done with curl, e.g.:
    curl -s localhost:8083/connectors?expand=status | jq
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from connect_api import Connect, print_status  # noqa: E402

from common.kafka_helpers import banner  # noqa: E402

c = Connect()
NAME = "tour-file-source"

banner("GET /  — which worker answered, which Kafka cluster it belongs to")
print(" ", c.info())

banner("GET /connector-plugins  — what is installed on the plugin.path")
for p in c.plugins():
    print(f"  {p['type']:6s} {p['class']:60s} {p.get('version', '')}")

banner("PUT /connector-plugins/FileStreamSourceConnector/config/validate  — catch mistakes BEFORE creating")
v = c.validate("org.apache.kafka.connect.file.FileStreamSourceConnector", {"name": NAME, "tasks.max": "1"})
print(f"  error_count={v['error_count']}")
for cfg in v["configs"]:
    if cfg["value"]["errors"]:
        print(f"  {cfg['value']['name']}: {cfg['value']['errors']}")

banner(f"PUT /connectors/{NAME}/config  — create (or update) a connector")
c.put(NAME, {
    "connector.class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
    "tasks.max": 1,
    "file": "/data/tour.txt",               # path INSIDE the worker container (= 15-kafka-connect/data on your Mac)
    "topic": "kinaction_connect_tour",
})
Path(__file__).resolve().parent.joinpath("data/tour.txt").write_text("line one\nline two\n")
print_status(c.wait_state(NAME))

banner(f"GET /connectors/{NAME}/tasks  — how the connector split the work into task configs")
for t in c.tasks(NAME):
    print(f"  task {t['id']['task']}: {t['config']}")

banner("PUT .../pause  -> PAUSED (tasks stop polling, keep their assignment)")
c.pause(NAME)
print_status(c.wait_state(NAME, "PAUSED"))
c.resume(NAME)
print_status(c.wait_state(NAME, "RUNNING"))

banner("POST .../restart?includeTasks=true  — restart connector + tasks (e.g. after fixing a FAILED task)")
c.restart(NAME)
print_status(c.wait_state(NAME))

banner(f"GET /connectors/{NAME}/topics and /offsets  — where it wrote and how far it got")
import time  # noqa: E402
time.sleep(6)                               # offsets are flushed every offset.flush.interval.ms (5s here)
print("  topics :", c.topics(NAME))
print("  offsets:", c.offsets(NAME))

banner("GET /connectors?expand=status  — the whole Connect cluster at a glance")
for name, v in c.list().items():
    print_status(v["status"])

banner(f"PUT /connectors/{NAME}/stop + DELETE /connectors/{NAME}/offsets  (KIP-875: reset a source)")
c.stop(NAME)
print_status(c.wait_state(NAME, "STOPPED", min_tasks=0))
print("  ", c.delete_offsets(NAME))

banner(f"DELETE /connectors/{NAME}")
c.delete(NAME)
print("  deleted. The topic remains. Without the offset reset, a new connector with the same name would")
print("  resume from the old file position (offsets live in _connect_offsets, keyed by connector name).")
