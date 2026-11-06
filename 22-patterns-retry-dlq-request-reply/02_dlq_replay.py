#!/usr/bin/env python3
"""Operate the DLQ: list records with their error headers, and REPLAY selected ones to the main topic
(after you fixed the bug / the downstream system is back). Run 01 first.

    python 22-patterns-retry-dlq-request-reply/02_dlq_replay.py                 # list
    python 22-patterns-retry-dlq-request-reply/02_dlq_replay.py --replay o5     # re-send key o5 to the main topic
"""
import argparse
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer, Producer  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner, log  # noqa: E402

DLQ, MAIN = "patterns.orders.dlq", "patterns.orders"
ap = argparse.ArgumentParser()
ap.add_argument("--replay", nargs="*", default=[], help="keys to replay")
args = ap.parse_args()

c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"dlq-tool-{uuid.uuid4()}", "auto.offset.reset": "earliest",
              "enable.auto.commit": False})
c.subscribe([DLQ])
p = Producer({"bootstrap.servers": BOOTSTRAP})
banner(f"records in {DLQ}")
idle = 0
while idle < 3:
    m = c.poll(1)
    if m is None:
        idle += 1 if c.assignment() else 0
        continue
    if m.error():
        continue
    h = {k: v.decode() for k, v in (m.headers() or [])}
    key = m.key().decode()
    print(f"  @{m.offset()} key={key} error={h.get('error')} value={m.value()[:60]!r}")
    if key in args.replay:
        p.produce(MAIN, key=m.key(), value=m.value(), headers=[("replayed-from", f"{DLQ}@{m.offset()}")])
        log(f"  replayed {key} -> {MAIN}")
p.flush()
c.close()
print("""
DLQ hygiene: alert on DLQ growth (it's a production incident, not a trash can), keep enough retention to
investigate, store WHY (headers), and make replay a tool, not an ad-hoc script.
Kafka Connect has this built in: errors.tolerance=all + errors.deadletterqueue.topic.name (see 15-kafka-connect).
""")
