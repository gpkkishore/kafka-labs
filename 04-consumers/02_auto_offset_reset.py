#!/usr/bin/env python3
"""Ch.5.1 — auto.offset.reset only matters when the GROUP HAS NO COMMITTED OFFSET.

    python 04-consumers/02_auto_offset_reset.py
"""
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer, Producer  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner  # noqa: E402

TOPIC = "kinaction_orders"


def read_for(group, reset, seconds=6, produce_after_join=False):
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": group, "auto.offset.reset": reset})
    c.subscribe([TOPIC])
    got, sent = [], False
    deadline = time.time() + seconds
    while time.time() < deadline:
        m = c.poll(0.2)
        if c.assignment() and produce_after_join and not sent:
            p = Producer({"bootstrap.servers": BOOTSTRAP})
            p.produce(TOPIC, key="new", value="a NEW record produced after the consumer joined", partition=0)
            p.flush()
            sent = True
        if m and not m.error():
            got.append(m.value().decode())
    c.close()
    return got


g1 = f"reset-earliest-{uuid.uuid4().hex[:4]}"
banner(f"new group {g1}, auto.offset.reset=earliest")
r = read_for(g1, "earliest")
print(f"  read {len(r)} records (everything retained in the topic)")

g2 = f"reset-latest-{uuid.uuid4().hex[:4]}"
banner(f"new group {g2}, auto.offset.reset=latest  (we produce 1 record after joining)")
r = read_for(g2, "latest", produce_after_join=True)
print(f"  read {len(r)} record(s): {r}")

banner(f"SAME group {g1} again, this time with auto.offset.reset=latest")
r = read_for(g1, "latest")
print(f"  read {len(r)} records -> the group already has committed offsets, so the reset policy is ignored")
print("""
auto.offset.reset values: earliest | latest (default) | error (raise if no offset).
It is ALSO used when a committed offset is out of range (e.g. data deleted by retention).
""")
