#!/usr/bin/env python3
"""Step 1 — create mm2-orders on the PRIMARY cluster and produce records.

    python 19-mirrormaker2-disaster-recovery/01_produce_on_primary.py -n 300
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from confluent_kafka import Producer  # noqa: E402

from common.kafka_helpers import ensure_topic, log  # noqa: E402
from mm2_common import PRIMARY, TOPIC  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("-n", type=int, default=300)
args = ap.parse_args()

ensure_topic(TOPIC, partitions=3, rf=3, config={"retention.ms": 7 * 24 * 3600 * 1000})
p = Producer({"bootstrap.servers": PRIMARY, "enable.idempotence": True})
for i in range(args.n):
    p.produce(TOPIC, key=f"customer-{i % 20}", value=f"order {i}")
    p.poll(0)
p.flush()
log(f"produced {args.n} records to {TOPIC} on PRIMARY")
