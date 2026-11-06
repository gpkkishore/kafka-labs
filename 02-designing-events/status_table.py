#!/usr/bin/env python3
"""Ch.3 / 5.5 — rebuild "current status of every sensor" from the compacted topic.

A compacted topic is a *table*: the latest value per key wins. Re-reading from offset 0 rebuilds state.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer, TopicPartition  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, admin  # noqa: E402

TOPIC = "kinaction_sensor_status"
c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "unused", "enable.auto.commit": False})
parts = admin().list_topics(TOPIC).topics[TOPIC].partitions
# assign() (no group membership) + start at the beginning of every partition
c.assign([TopicPartition(TOPIC, p, 0) for p in parts])
table, read = {}, 0
ends = {p: c.get_watermark_offsets(TopicPartition(TOPIC, p))[1] for p in parts}
done = {p for p, hi in ends.items() if hi == 0}
while len(done) < len(parts):
    m = c.poll(2.0)
    if m is None:
        break
    if m.error():
        continue
    read += 1
    table[m.key().decode()] = json.loads(m.value())
    if m.offset() + 1 >= ends[m.partition()]:
        done.add(m.partition())
c.close()
print(f"read {read} records -> {len(table)} distinct sensors (only latest value per key matters)\n")
for k in sorted(table):
    print(f"  {k:22s} {table[k]['temperature_c']:6.1f} C")
