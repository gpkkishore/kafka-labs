#!/usr/bin/env python3
"""Show the effect of Flink's exactly-once (transactional) Kafka sink from the consumer side.

    bash 17-flink-sql/run-sql.sh 06_exactly_once_sink.sql
    python 17-flink-sql/01_clickstream_producer.py --seconds 30 &
    python 17-flink-sql/03_read_committed_vs_uncommitted.py

read_uncommitted sees records as soon as Flink writes them (inside an open transaction);
read_committed only sees them after the checkpoint completes and Flink commits the transaction.
"""
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer, TopicPartition  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, admin, banner, log  # noqa: E402

TOPIC = "kinaction_flink_purchases_eos"
parts = admin().list_topics(TOPIC).topics[TOPIC].partitions


def consumer(level):
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"eos-{level}-{uuid.uuid4().hex[:4]}",
                  "isolation.level": level, "enable.auto.commit": False})
    c.assign([TopicPartition(TOPIC, p, 0) for p in parts])
    return c


cons = {lvl: consumer(lvl) for lvl in ("read_uncommitted", "read_committed")}
seen = {lvl: 0 for lvl in cons}
banner("count of visible records, sampled every second for 25s (checkpoint interval = 10s)")
for _ in range(25):
    for lvl, c in cons.items():
        for m in c.consume(1000, timeout=0.3):
            if not m.error():
                seen[lvl] += 1
    log(f"read_uncommitted={seen['read_uncommitted']:5d}   read_committed={seen['read_committed']:5d}")
    time.sleep(0.4)
for c in cons.values():
    c.close()
print("\nread_committed advances in STEPS (one per completed checkpoint); read_uncommitted grows continuously.")
