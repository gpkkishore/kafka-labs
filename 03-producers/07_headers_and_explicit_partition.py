#!/usr/bin/env python3
"""Ch.4 — record headers (metadata like trace ids) and choosing the partition yourself.

    python 03-producers/07_headers_and_explicit_partition.py
"""
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer, Producer  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner, recreate_topic  # noqa: E402

TOPIC = "kinaction_headers"
recreate_topic(TOPIC, 3, 3)
p = Producer({"bootstrap.servers": BOOTSTRAP})
for region, part in [("us", 0), ("eu", 1), ("apac", 2)]:
    # partition=... bypasses the partitioner entirely (a "custom partitioner" in Java terms)
    p.produce(TOPIC, key=f"order-{region}", value=f"order from {region}", partition=part,
              headers=[("trace-id", str(uuid.uuid4())), ("source", b"web"), ("region", region)])
p.flush()

c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"hdr-{uuid.uuid4()}", "auto.offset.reset": "earliest"})
c.subscribe([TOPIC])
banner("Read back")
got = 0
while got < 3:
    m = c.poll(5)
    if m is None or m.error():
        continue
    got += 1
    hdrs = {k: v.decode() for k, v in (m.headers() or [])}
    print(f"  p{m.partition()} {m.value().decode():22s} headers={hdrs}")
c.close()
print("\nHeaders are perfect for tracing (W3C traceparent), content-type, schema hints — not for business data.")
