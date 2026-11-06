#!/usr/bin/env python3
"""Ch.4.2.3 — CreateTime vs LogAppendTime.

message.timestamp.type=CreateTime   (default) the PRODUCER's timestamp is stored (can be set explicitly, even in the past)
message.timestamp.type=LogAppendTime          the BROKER overwrites it with its clock when appending

    python 03-producers/06_timestamps.py
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import TIMESTAMP_CREATE_TIME, Consumer, Producer, TopicPartition  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner, recreate_topic  # noqa: E402

TYPES = {TIMESTAMP_CREATE_TIME: "CreateTime"}
one_day_ago = int((time.time() - 86400) * 1000)
p = Producer({"bootstrap.servers": BOOTSTRAP})

for ts_type in ["CreateTime", "LogAppendTime"]:
    topic = f"kinaction_ts_{ts_type.lower()}"
    recreate_topic(topic, 1, 3, {"message.timestamp.type": ts_type})
    p.produce(topic, value=b"explicit timestamp 1 day ago", timestamp=one_day_ago)
    p.produce(topic, value=b"no timestamp given")
    p.flush()
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "ts-demo", "enable.auto.commit": False})
    c.assign([TopicPartition(topic, 0, 0)])
    banner(f"topic config message.timestamp.type={ts_type}")
    for _ in range(2):
        m = c.poll(5)
        kind, value = m.timestamp()
        print(f"  {m.value().decode():32s} stored ts={time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(value / 1000))} "
              f"type={'CreateTime' if kind == TIMESTAMP_CREATE_TIME else 'LogAppendTime'}")
    c.close()
print("""
Why it matters: retention.ms, time-index lookups (offsets_for_times / replay from a time, see 04-consumers),
and stream-processing windows all use these timestamps. Producing OLD CreateTime timestamps into a topic with
retention.ms=1 day can get data deleted almost immediately!
""")
