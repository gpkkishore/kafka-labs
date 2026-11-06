#!/usr/bin/env python3
"""Ch.3 — consume alerts and prove the ordering guarantee: per stage (key) the timestamps never go backwards.

    python 02-designing-events/alert_consumer.py
"""
import json
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner, log  # noqa: E402

c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "alert-dashboard", "auto.offset.reset": "earliest"})
c.subscribe(["kinaction_alert", "kinaction_alerttrend"])
last_ts = {}
partitions_per_stage = defaultdict(set)
counts = defaultdict(int)
banner("Reading alerts + trend (stops after 5s idle)")
idle = 0
while idle < 5:
    m = c.poll(1.0)
    if m is None:
        idle += 1 if c.assignment() else 0
        continue
    if m.error():
        continue
    idle = 0
    e = json.loads(m.value())
    stage = m.key().decode()
    partitions_per_stage[(m.topic(), stage)].add(m.partition())
    counts[m.topic()] += 1
    k = (m.topic(), m.partition(), stage)
    if k in last_ts and e["ts"] < last_ts[k]:
        log(f"OUT OF ORDER?! {k}")     # never printed: a single partition is strictly ordered
    last_ts[k] = e["ts"]
    if m.topic() == "kinaction_alert":
        log(f"ALERT {stage} {e['sensor_id']} {e['temperature_c']}C  (p{m.partition()}@{m.offset()})")
c.close()
print("\nrecords read:", dict(counts))
print("Each stage (key) is in exactly ONE partition per topic:")
for (topic, stage), ps in sorted(partitions_per_stage.items()):
    print(f"  {topic:22s} {stage}: partitions {sorted(ps)}")
