#!/usr/bin/env python3
"""Live view of a consumer group from the COORDINATOR's point of view: state, members, assignment, lag.

    python 05-consumer-groups-rebalancing/group_watch.py                 # refresh every 2s
    python 05-consumer-groups-rebalancing/group_watch.py --once
"""
import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import ConsumerGroupTopicPartitions, Consumer, TopicPartition  # noqa: E402

from common.kafka_helpers import BOLD, BOOTSTRAP, RESET, admin, color_for, ts  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--group", default="kinaction_rebalance_group")
ap.add_argument("--topic", default="kinaction_rebalance")
ap.add_argument("--once", action="store_true")
args = ap.parse_args()

a = admin()
wm = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "group-watch-tool"})   # only for watermarks


def snapshot():
    d = a.describe_consumer_groups([args.group], request_timeout=5)[args.group].result()
    owner = {}
    lines = [f"{BOLD}{ts()}  group={d.group_id} state={d.state.name} assignor={d.partition_assignor or '-'} "
             f"coordinator=broker {d.coordinator.id}{RESET}"]
    for mbr in sorted(d.members, key=lambda m: m.client_id):
        parts = sorted(tp.partition for tp in mbr.assignment.topic_partitions if tp.topic == args.topic)
        for p in parts:
            owner[p] = mbr.client_id
        static = f" static={mbr.group_instance_id}" if mbr.group_instance_id else ""
        lines.append(f"  {mbr.client_id:12s} {mbr.member_id[-12:]}{static:22s} partitions="
                     + "[" + ",".join(f"{color_for(p)}{p}{RESET}" for p in parts) + "]")
    if not d.members:
        lines.append("  (no members)")
    committed = a.list_consumer_group_offsets([ConsumerGroupTopicPartitions(args.group)])[args.group].result()
    offs = {tp.partition: tp.offset for tp in committed.topic_partitions if tp.topic == args.topic}
    md = a.list_topics(args.topic, timeout=5).topics.get(args.topic)
    total = 0
    row = []
    for p in sorted(md.partitions if md else []):
        hi = wm.get_watermark_offsets(TopicPartition(args.topic, p), timeout=5)[1]
        lag = hi - offs[p] if offs.get(p, -1) >= 0 else None
        total += lag or 0
        row.append(f"{color_for(p)}p{p}{RESET}:{owner.get(p, '-UNOWNED-')}/lag={lag if lag is not None else '?'}")
    lines.append("  " + "  ".join(row))
    lines.append(f"  total lag = {total}")
    return "\n".join(lines)


while True:
    try:
        out = snapshot()
    except Exception as e:  # noqa: BLE001
        out = f"{ts()} {e}"
    if args.once:
        print(out)
        break
    print("\033[2J\033[H" + out, flush=True)
    time.sleep(2)
