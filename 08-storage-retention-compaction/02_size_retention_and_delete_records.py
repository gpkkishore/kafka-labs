#!/usr/bin/env python3
"""retention.bytes (per PARTITION, not per topic!) and AdminClient.delete_records (truncate the head on demand).

    python 08-storage-retention-compaction/02_size_retention_and_delete_records.py
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer, Producer, TopicPartition  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, admin, banner, log, recreate_topic  # noqa: E402

a = admin()
c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "wm"})
p = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 0, "batch.num.messages": 1})

T1 = "kinaction_retention_bytes"
recreate_topic(T1, 1, 3, {"retention.bytes": 4096, "segment.bytes": 1024}, client=a)
banner("retention.bytes=4096, segment.bytes=1024 : produce ~20 KB")
for i in range(200):
    p.produce(T1, value=f"record-{i:04d}-" + "x" * 80)
    p.poll(0)
p.flush()
for _ in range(5):
    lo, hi = c.get_watermark_offsets(TopicPartition(T1, 0), timeout=5)
    log(f"low={lo} high={hi}  retained={hi - lo} records")
    time.sleep(5)
log("-> retention.bytes keeps the newest ~4 KB (+ the active segment) per partition; whole old segments are dropped")

T2 = "kinaction_delete_records"
recreate_topic(T2, 1, 3, client=a)
for i in range(100):
    p.produce(T2, value=f"r{i}")
p.flush()
banner("delete_records(): drop everything BEFORE offset 60 (e.g. GDPR purge, or 'skip poison data')")
log(f"before: {c.get_watermark_offsets(TopicPartition(T2, 0), timeout=5)}")
res = a.delete_records([TopicPartition(T2, 0, 60)])
for tp, f in res.items():
    r = f.result()
    log(f"new low watermark = {getattr(r, 'low_watermark', r)}")
log(f"after : {c.get_watermark_offsets(TopicPartition(T2, 0), timeout=5)}")
c.close()
print("\ndelete_records only moves the log-start-offset; physical segment files are removed when fully below it.")
