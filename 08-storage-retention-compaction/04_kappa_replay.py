#!/usr/bin/env python3
"""KiA 8.5 "Lambda vs Kappa architecture" — in Kappa there is ONE pipeline; to change logic you simply
REPLAY the retained log with a new consumer group and build a new view side by side.

    python 08-storage-retention-compaction/04_kappa_replay.py
"""
import json
import random
import sys
import uuid
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer, Producer  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner, recreate_topic  # noqa: E402

TOPIC = "kinaction_purchases"
recreate_topic(TOPIC, 3, 3, {"retention.ms": -1})   # keep forever: the log IS the source of truth
p = Producer({"bootstrap.servers": BOOTSTRAP})
random.seed(7)
for i in range(300):
    e = {"user": f"u{random.randint(1, 5)}", "amount": round(random.uniform(1, 200), 2),
         "country": random.choice(["US", "DE", "IN"]), "refund": random.random() < 0.1}
    p.produce(TOPIC, key=e["user"], value=json.dumps(e))
p.flush()


def build_view(version):
    """Replay the full topic with a FRESH group id -> starts at offset 0."""
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"view-{version}-{uuid.uuid4().hex[:4]}",
                  "auto.offset.reset": "earliest"})
    c.subscribe([TOPIC])
    view, idle = defaultdict(float), 0
    while idle < 3:
        m = c.poll(1)
        if m is None:
            idle += 1 if c.assignment() else 0
            continue
        e = json.loads(m.value())
        if version == "v1":
            view[e["user"]] += e["amount"]                                   # naive: refunds counted as revenue
        else:
            view[(e["user"], e["country"])] += -e["amount"] if e["refund"] else e["amount"]   # new logic
    c.close()
    return dict(view)


banner("view v1 (original logic): revenue per user")
for k, v in sorted(build_view("v1").items()):
    print(f"  {k}: {v:9.2f}")
banner("view v2 (bug fix + new dimension) built by REPLAYING the same log")
for k, v in sorted(build_view("v2").items())[:8]:
    print(f"  {k}: {v:9.2f}")
print("""
Lambda = batch layer (recompute everything from a data lake) + speed layer (stream) + merge -> two codebases.
Kappa  = only the stream layer; reprocess = replay the log. Needs long retention (or tiered storage) on the topic.
""")
