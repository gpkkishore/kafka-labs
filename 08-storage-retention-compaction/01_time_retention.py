#!/usr/bin/env python3
"""KiA 8.1 / AKiA 10.3 — time-based retention deletes WHOLE CLOSED SEGMENTS, not individual records.

Topic: retention.ms=20s, segment.ms=5s. The broker checks every log.retention.check.interval.ms (10s in this lab).
Watch the LOW WATERMARK (first available offset) jump forward in steps = one segment at a time.

    python 08-storage-retention-compaction/01_time_retention.py
"""
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer, Producer, TopicPartition  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner, log, recreate_topic  # noqa: E402

TOPIC = "kinaction_retention_time"
recreate_topic(TOPIC, 1, 3, {"retention.ms": 20000, "segment.ms": 5000})
p = Producer({"bootstrap.servers": BOOTSTRAP})
c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "wm"})
tp = TopicPartition(TOPIC, 0)


def segments():
    out = subprocess.run(["docker", "exec", "kafka1", "sh", "-c", f"ls /var/lib/kafka/data/{TOPIC}-0/ | grep '\\.log$'"],
                         capture_output=True, text=True).stdout.split()
    return [int(s.split(".")[0]) for s in out]


banner("produce 1 record/s for 30s, then stop and keep watching for 45s")
start = time.time()
i = 0
while time.time() - start < 75:
    if time.time() - start < 30:
        p.produce(TOPIC, value=f"r{i}")
        p.flush()
        i += 1
    lo, hi = c.get_watermark_offsets(tp, timeout=5)
    log(f"low={lo:3d} high={hi:3d} retained={hi - lo:3d}  segments(base offsets)={segments()}")
    time.sleep(3 if time.time() - start > 30 else 1)
c.close()
print("""
* A segment can only be deleted once it's CLOSED (rolled) and its NEWEST record is older than retention.ms.
* Therefore data can live up to retention.ms + segment.ms (+ check interval). Default segment.ms is 7 days!
* The active segment is never deleted directly; once segment.ms has passed the broker rolls it to a new empty
  segment and the old one becomes deletable -> in the end low == high (topic empty, offsets NOT reset to 0).
""")
