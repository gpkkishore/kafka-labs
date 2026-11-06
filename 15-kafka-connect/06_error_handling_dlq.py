#!/usr/bin/env python3
"""DG ch.7 "Failure handling" / AKiA 11.6 — what happens with a poison message, and the Dead Letter Queue.

errors.tolerance=none (default) -> the task FAILS on the first bad record and stops the whole pipeline.
errors.tolerance=all            -> bad records are skipped; with errors.deadletterqueue.topic.name they're
                                   written to a DLQ topic together with headers describing the error.
(DLQ works for SINK connectors: failures in the converter or SMTs.)

    python 15-kafka-connect/06_error_handling_dlq.py
"""
import sys
import time
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE))
from confluent_kafka import Consumer, Producer  # noqa: E402
from connect_api import Connect, print_status  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner, recreate_topic  # noqa: E402

IN, DLQ = "kinaction_connect_dlq_in", "kinaction_connect_dlq"
recreate_topic(IN, 1, 3)
recreate_topic(DLQ, 1, 3)
p = Producer({"bootstrap.servers": BOOTSTRAP})
for v in ['{"id": 1, "ok": true}', 'THIS IS NOT JSON', '{"id": 2, "ok": true}', '{broken', '{"id": 3, "ok": true}']:
    p.produce(IN, value=v)
p.flush()
print("produced 5 records, 2 of them are not valid JSON")

c = Connect()
BASE = {"connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector", "topics": IN, "tasks.max": 1,
        "value.converter": "org.apache.kafka.connect.json.JsonConverter", "value.converter.schemas.enable": "false",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter"}

banner("1) errors.tolerance=none  -> task FAILS")
c.delete("dlq-strict")
c.put("dlq-strict", {**BASE, "file": "/data/dlq-strict.txt"})
time.sleep(8)                     # let the task start and hit the bad record at offset 1
st = c.status("dlq-strict")
print_status(st)
for t in st["tasks"]:
    if t["state"] == "FAILED":
        causes = [ln.strip() for ln in t["trace"].splitlines() if "Caused by" in ln or "Tolerance exceeded" in ln]
        print("   ", "\n    ".join(x[:150] for x in causes[:3]))
print("  The task is dead: NOTHING after offset 1 is processed until you fix it and POST .../restart.")
print("  (FileStreamSink hadn't even flushed record 0 — sinks flush/commit on offset.flush.interval.ms.)")
c.delete("dlq-strict")

banner("2) errors.tolerance=all + DLQ  -> good records flow, bad ones parked in the DLQ")
c.delete("dlq-tolerant")
c.put("dlq-tolerant", {**BASE, "file": "/data/dlq-tolerant.txt",
                       "errors.tolerance": "all",
                       "errors.deadletterqueue.topic.name": DLQ,
                       "errors.deadletterqueue.topic.replication.factor": 3,
                       "errors.deadletterqueue.context.headers.enable": "true",
                       "errors.log.enable": "true", "errors.log.include.messages": "true",
                       "errors.retry.timeout": 0})
print_status(c.wait_state("dlq-tolerant"))
time.sleep(6)
print("  data/dlq-tolerant.txt:\n   " + (HERE / "data/dlq-tolerant.txt").read_text().replace("\n", "\n   "))

cons = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"dlq-{uuid.uuid4()}", "auto.offset.reset": "earliest"})
cons.subscribe([DLQ])
got = 0
for _ in range(15):
    m = cons.poll(1)
    if m is None or m.error():
        continue
    got += 1
    h = {k: v.decode() for k, v in m.headers() or []}
    print(f"\n  DLQ record value={m.value().decode()!r}")
    for k in ("__connect.errors.topic", "__connect.errors.partition", "__connect.errors.offset",
              "__connect.errors.stage", "__connect.errors.class.name", "__connect.errors.exception.message"):
        print(f"     {k:38s} = {h.get(k, '')[:90]}")
    if got == 2:
        break
cons.close()
c.delete("dlq-tolerant")
print("\nTypical ops loop: alert on DLQ growth -> inspect headers -> fix -> re-produce DLQ records to the input topic.")
