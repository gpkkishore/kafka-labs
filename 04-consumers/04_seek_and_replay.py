#!/usr/bin/env python3
"""Ch.5.2 / 5.4 — you control the position: seek to beginning, to an offset, or to a POINT IN TIME.

    python 04-consumers/04_seek_and_replay.py --minutes-ago 10      # replay the last 10 minutes
    python 04-consumers/04_seek_and_replay.py --offset 7            # start every partition at offset 7
"""
import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, admin, banner  # noqa: E402

TOPIC = "kinaction_orders"
ap = argparse.ArgumentParser()
g = ap.add_mutually_exclusive_group()
g.add_argument("--minutes-ago", type=int)
g.add_argument("--offset", type=int)
args = ap.parse_args()

c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "replayer", "enable.auto.commit": False})
parts = sorted(admin().list_topics(TOPIC).topics[TOPIC].partitions)

if args.minutes_ago is not None:
    # offsets_for_times uses the per-segment .timeindex file: "first offset with timestamp >= T"
    t = int((time.time() - args.minutes_ago * 60) * 1000)
    tps = c.offsets_for_times([TopicPartition(TOPIC, p, t) for p in parts], timeout=10)
    banner(f"offsets_for_times({time.strftime('%H:%M:%S', time.localtime(t / 1000))})")
    for tp in tps:
        print(f"  partition {tp.partition}: start at offset {tp.offset}  (-1 = no record that new)")
    start = tps
elif args.offset is not None:
    start = [TopicPartition(TOPIC, p, args.offset) for p in parts]
else:
    start = [TopicPartition(TOPIC, p, OFFSET_BEGINNING) for p in parts]

for p in parts:
    lo, hi = c.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10)
    print(f"  partition {p}: low watermark={lo} (oldest retained) high watermark={hi} (next offset to be written)")

c.assign(start)   # assign() = no group rebalancing; the starting offset comes from us
banner("replaying")
idle = 0
while idle < 3:
    m = c.poll(1.0)
    if m is None:
        idle += 1
        continue
    if not m.error():
        print(f"  p{m.partition()}@{m.offset():<3d} {time.strftime('%H:%M:%S', time.localtime(m.timestamp()[1] / 1000))} {m.value().decode()}")
c.close()
print("\nCLI equivalent: bin/groups --group G --reset-offsets --to-datetime 2026-01-01T10:00:00.000 --topic T --execute")
