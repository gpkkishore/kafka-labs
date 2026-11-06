#!/usr/bin/env python3
"""Ch.5.5 — reading a compacted topic: build a key -> latest value table, honour tombstones (value=None).

    python 04-consumers/07_compacted_topic_reader.py
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer, Producer, TopicPartition  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner, recreate_topic  # noqa: E402

TOPIC = "kinaction_customer_profile"
recreate_topic(TOPIC, 1, 3, {"cleanup.policy": "compact", "segment.ms": 5000,
                             "min.cleanable.dirty.ratio": 0.01, "delete.retention.ms": 5000})
p = Producer({"bootstrap.servers": BOOTSTRAP})
for k, v in [("alice", "gold"), ("bob", "silver"), ("alice", "platinum"), ("carol", "bronze"),
             ("bob", None), ("dave", "silver")]:     # bob -> None is a TOMBSTONE (delete bob)
    p.produce(TOPIC, key=k, value=v)
p.flush()


def read_table():
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "x", "enable.auto.commit": False})
    c.assign([TopicPartition(TOPIC, 0, 0)])
    table, raw = {}, []
    hi = c.get_watermark_offsets(TopicPartition(TOPIC, 0))[1]
    while True:
        m = c.poll(2)
        if m is None:
            break
        raw.append((m.offset(), m.key().decode(), m.value().decode() if m.value() else None))
        if m.value() is None:
            table.pop(m.key().decode(), None)
        else:
            table[m.key().decode()] = m.value().decode()
        if m.offset() + 1 >= hi:
            break
    c.close()
    return raw, table


raw, table = read_table()
banner("before compaction: every record is still there")
for r in raw:
    print("  offset=%d key=%-6s value=%s" % r)
print("  TABLE:", table)

print("\nwaiting ~40s for the log cleaner (segment must roll, then cleaner runs)...")
for _ in range(8):
    time.sleep(5)
    p.produce(TOPIC, key="heartbeat", value="x")    # new records force the active segment to roll
    p.flush()
raw, table = read_table()
banner("after compaction: older values per key are gone, offsets keep their GAPS")
for r in raw:
    print("  offset=%d key=%-9s value=%s" % r)
print("  TABLE:", table)
print("\nOffsets are never renumbered — compaction leaves holes. The latest value per key always survives.")
