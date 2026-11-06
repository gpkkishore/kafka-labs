#!/usr/bin/env python3
"""Step 2 — group mm2-billing consumes PART of the topic on the primary and commits.
MM2's checkpoint connector will translate these committed offsets for the DR cluster.

    python 19-mirrormaker2-disaster-recovery/02_consume_on_primary.py --max 120
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from confluent_kafka import Consumer  # noqa: E402

from common.kafka_helpers import log  # noqa: E402
from mm2_common import GROUP, PRIMARY, TOPIC  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--max", type=int, default=120)
args = ap.parse_args()

c = Consumer({"bootstrap.servers": PRIMARY, "group.id": GROUP, "auto.offset.reset": "earliest",
              "enable.auto.commit": False})
c.subscribe([TOPIC])
n, idle = 0, 0
while n < args.max and idle < 5:
    m = c.poll(1.0)
    if m is None:
        idle += 1 if c.assignment() else 0
        continue
    if m.error():
        continue
    n += 1
c.commit(asynchronous=False)
committed = c.committed(c.assignment(), timeout=10)
c.close()
log(f"consumed {n} records on PRIMARY as group {GROUP}; committed:")
for tp in sorted(committed, key=lambda t: t.partition):
    print(f"   {tp.topic}[{tp.partition}] -> next offset {tp.offset}")
