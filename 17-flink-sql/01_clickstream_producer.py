#!/usr/bin/env python3
"""Produce a click stream for the Flink jobs: JSON events with an EVENT-TIME timestamp (ts_ms).

    python 17-flink-sql/01_clickstream_producer.py                        # 60s at 20 events/s
    python 17-flink-sql/01_clickstream_producer.py --seconds 0 --late-pct 10   # forever, 10% events 3-20s late

user_id 1..8 and product_id 1..6 match the rows of the inventory database from 16-cdc-debezium-mysql,
so the CDC join (sql/04) finds the products.
"""
import argparse
import json
import random
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Producer  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, ensure_topic, log  # noqa: E402

TOPIC = "kinaction_flink_clicks"
ap = argparse.ArgumentParser()
ap.add_argument("--seconds", type=int, default=60, help="0 = forever")
ap.add_argument("--rate", type=int, default=20)
ap.add_argument("--late-pct", type=float, default=5, help="percentage of events with an old timestamp")
args = ap.parse_args()

ensure_topic(TOPIC, 3, 3)
p = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 20})
end = time.time() + args.seconds if args.seconds else float("inf")
n = late = 0
try:
    while time.time() < end:
        now = int(time.time() * 1000)
        ts = now
        if random.random() * 100 < args.late_pct:
            ts = now - random.randint(3000, 20000)          # out-of-order / late event
            late += 1
        action = random.choices(["view", "add_to_cart", "purchase"], [80, 15, 5])[0]
        product = random.choices(range(1, 7), [30, 10, 20, 15, 15, 10])[0]   # product 1 is "hot"
        ev = {"user_id": random.randint(1, 8), "product_id": product, "action": action,
              "quantity": random.randint(1, 3) if action == "purchase" else 0, "ts_ms": ts}
        p.produce(TOPIC, key=str(ev["user_id"]), value=json.dumps(ev))   # key=user -> per-user ordering
        p.poll(0)
        n += 1
        if n % (args.rate * 10) == 0:
            log(f"{n} events produced ({late} late)")
        time.sleep(1 / args.rate)
except KeyboardInterrupt:
    pass
p.flush()
log(f"done: {n} events, {late} with late timestamps")
