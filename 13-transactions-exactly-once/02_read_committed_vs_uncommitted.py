#!/usr/bin/env python3
"""KiA 2.6.2 / AKiA 5.2 — isolation.level: read_uncommitted vs read_committed.

    python 13-transactions-exactly-once/02_read_committed_vs_uncommitted.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer, TopicPartition  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner  # noqa: E402

TOPIC = "tx_payments"


def read_all(isolation):
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "tx-reader", "enable.auto.commit": False,
                  "isolation.level": isolation})
    c.assign([TopicPartition(TOPIC, 0, 0)])
    out = []
    while True:
        m = c.poll(3)
        if m is None:
            break
        if not m.error():
            out.append((m.offset(), m.value().decode()))
    lo, hi = c.get_watermark_offsets(TopicPartition(TOPIC, 0))
    c.close()
    return out, hi


for iso in ["read_uncommitted", "read_committed"]:
    rows, hi = read_all(iso)
    banner(f"isolation.level={iso}: {len(rows)} records")
    for off, v in rows:
        print(f"  offset {off:2d}  {v}")
print(f"""
* read_committed hides aborted records (it reads up to the Last Stable Offset, LSO).
* Offsets have GAPS (e.g. 3, 7, 9): each COMMIT/ABORT marker is a 'control record' that takes an offset
  but is never handed to applications. High watermark here = {hi}.
* Default isolation.level is read_uncommitted (Java & librdkafka) -> always set read_committed for EOS!
""")
