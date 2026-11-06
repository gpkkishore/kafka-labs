#!/usr/bin/env python3
"""Steady keyed traffic into a 6-partition topic so you can watch consumers share the work.

    python 05-consumer-groups-rebalancing/producer_steady.py --rate 20
"""
import argparse
import signal
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Producer  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, ensure_topic, log  # noqa: E402

TOPIC = "kinaction_rebalance"
ap = argparse.ArgumentParser()
ap.add_argument("--rate", type=float, default=20, help="messages per second")
ap.add_argument("--partitions", type=int, default=6)
args = ap.parse_args()
ensure_topic(TOPIC, args.partitions, 3)

running = True
signal.signal(signal.SIGTERM, lambda *_: globals().__setitem__("running", False))
p = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 5})
i = 0
log(f"producing ~{args.rate}/s to {TOPIC} (Ctrl+C to stop)")
try:
    while running:
        p.produce(TOPIC, key=f"device-{i % 60}", value=f"reading {i}")
        p.poll(0)
        i += 1
        if i % (int(args.rate) * 10 or 1) == 0:
            log(f"produced {i}")
        time.sleep(1 / args.rate)
except KeyboardInterrupt:
    pass
p.flush()
