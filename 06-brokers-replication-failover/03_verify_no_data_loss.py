#!/usr/bin/env python3
"""DG ch.6 "Validating system reliability": read the whole topic and check that EVERY record the producer
saw acknowledged (acked.log) is really there. Also reports duplicates.

    python 06-brokers-replication-failover/03_verify_no_data_loss.py
"""
import argparse
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer, TopicPartition  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, admin, banner  # noqa: E402

HERE = Path(__file__).resolve().parent
ap = argparse.ArgumentParser()
ap.add_argument("--topic", default="kinaction_replication")
args = ap.parse_args()

lines = (HERE / "acked.log").read_text().split("\n")
run_id = lines[0].split()[1]                     # first line: "run <id>"
acked = {ln for ln in lines[1:] if ln}
parts = admin().list_topics(args.topic).topics[args.topic].partitions
c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "verifier", "enable.auto.commit": False,
              "isolation.level": "read_committed"})
ends = {p: c.get_watermark_offsets(TopicPartition(args.topic, p))[1] for p in parts}
c.assign([TopicPartition(args.topic, p, 0) for p in parts])
seen: Counter = Counter()
done = {p for p, e in ends.items() if e == 0}
while len(done) < len(parts):
    m = c.poll(5)
    if m is None:
        break
    if m.error():
        continue
    v = m.value().decode()
    if v.startswith(run_id + ":"):                # only records of the latest producer run
        seen[v] += 1
    if m.offset() + 1 >= ends[m.partition()]:  # noqa: SIM102
        done.add(m.partition())
c.close()

missing = acked - set(seen)
dups = {k: v for k, v in seen.items() if v > 1}
banner("Reliability check")
print(f"  acknowledged by brokers : {len(acked)}")
print(f"  found in topic (run {run_id}): {len(seen)} distinct")
print(f"  MISSING acked records   : {len(missing)} {sorted(missing)[:20]}")
print(f"  duplicates              : {len(dups)}  (idempotent producer => should be 0)")
print("\n  ✅ no acknowledged data lost" if not missing else "\n  ❌ DATA LOSS — check acks / min.insync.replicas / unclean election")
