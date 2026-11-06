#!/usr/bin/env python3
"""Seed data for the consumer examples: topic kinaction_orders, 3 partitions, 10 records each,
timestamps spread over the last 30 minutes (needed by 04_seek_and_replay.py)."""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Producer  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, log, recreate_topic  # noqa: E402

TOPIC = "kinaction_orders"
recreate_topic(TOPIC, 3, 3)
p = Producer({"bootstrap.servers": BOOTSTRAP})
now = int(time.time() * 1000)
for i in range(30):
    ts = now - (30 - i) * 60_000            # one record per minute, oldest first
    p.produce(TOPIC, key=f"order-{i}", value=f"order {i} created {time.strftime('%H:%M', time.localtime(ts / 1000))}",
              partition=i % 3, timestamp=ts)
p.flush()
log(f"seeded 30 records into {TOPIC} (10 per partition, 1/minute over the last 30 min)")
