#!/usr/bin/env python3
"""Read what the Flink jobs wrote back to Kafka.

    python 17-flink-sql/02_read_results.py                 # all result topics, then exit
    python 17-flink-sql/02_read_results.py --follow        # keep printing

Append topics (window results) are printed as they arrive. Upsert topics (key = PRIMARY KEY) are folded
into a table: newest value per key wins, a null value (tombstone) deletes the key — exactly like a KTable.
"""
import argparse
import json
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, banner  # noqa: E402

APPEND = ["kinaction_flink_action_counts", "kinaction_flink_trending"]
UPSERT = ["kinaction_flink_category_revenue", "kinaction_flink_inventory_value"]
ap = argparse.ArgumentParser()
ap.add_argument("--follow", action="store_true")
ap.add_argument("--last", type=int, default=6, help="window results to show per append topic")
args = ap.parse_args()

c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"flink-results-{uuid.uuid4().hex[:6]}",
              "auto.offset.reset": "earliest"})
c.subscribe(APPEND + UPSERT)
tables = {t: {} for t in UPSERT}
updates = {t: 0 for t in UPSERT}
recent = {t: [] for t in APPEND}
idle = 0
try:
    while args.follow or idle < 6:
        m = c.poll(1.0)
        if m is None:
            idle += 1 if c.assignment() else 0
            continue
        if m.error():
            continue
        idle = 0
        t = m.topic()
        if t in UPSERT:
            k = json.loads(m.key())
            updates[t] += 1
            if m.value() is None:
                tables[t].pop(json.dumps(k), None)
            else:
                tables[t][json.dumps(k)] = json.loads(m.value())
            if args.follow:
                print(f"{t}: {k} -> {m.value() and json.loads(m.value())}")
        else:
            recent[t].append(json.loads(m.value()))
            if args.follow:
                print(f"{t}: {m.value().decode()}")
except KeyboardInterrupt:
    pass
finally:
    c.close()

for t in APPEND:
    banner(f"{t}  (append-only, last {args.last})")
    # records come from 3 partitions -> sort by window to see the newest windows
    for r in sorted(recent[t], key=lambda r: (r["window_end"], str(r)))[-args.last:]:
        print("  ", r)
for t in UPSERT:
    banner(f"{t}  (upsert: {updates[t]} records folded into {len(tables[t])} keys)")
    for k, v in sorted(tables[t].items()):
        print(f"   key={k:28s} {v}")
