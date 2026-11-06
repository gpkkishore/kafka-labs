#!/usr/bin/env python3
"""Step 4 — DISASTER: the primary is gone. Point the consumer at DR and resume where it left off.

Two ways, both shown:
  a) --mode synced    : just use the same group.id on DR; MM2 already committed translated offsets there
  b) --mode translate : RemoteClusterUtils.translateOffsets() equivalent — read the checkpoints topic,
                        then assign() + seek() explicitly (works even with sync.group.offsets.enabled=false)

    python 19-mirrormaker2-disaster-recovery/04_failover_consumer.py --mode translate
"""
import argparse
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition  # noqa: E402

from common.kafka_helpers import banner, log  # noqa: E402
from mm2_common import CHECKPOINTS, DR, GROUP, REMOTE_TOPIC, TOPIC, decode_checkpoint  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--mode", choices=["synced", "translate"], default="translate")
args = ap.parse_args()


def translate_offsets(group):
    """Latest checkpoint per partition -> {remote_topic_partition: downstream_offset}."""
    c = Consumer({"bootstrap.servers": DR, "group.id": f"cp-{uuid.uuid4()}", "auto.offset.reset": "earliest",
                  "enable.auto.commit": False})
    c.subscribe([CHECKPOINTS])
    out, idle = {}, 0
    while idle < 4:
        m = c.poll(1.0)
        if m is None:
            idle += 1 if c.assignment() else 0
            continue
        if m.error():
            continue
        cp = decode_checkpoint(m.key(), m.value())
        if cp["group"] == group:
            out[(cp["topic"], cp["partition"])] = cp["downstream_offset"]
    c.close()
    return out


from confluent_kafka.admin import AdminClient  # noqa: E402
if CHECKPOINTS not in AdminClient({"bootstrap.servers": DR}).list_topics(timeout=10).topics:
    sys.exit("checkpoints topic missing on DR — run 01..03 first and make sure mm2 is running")
consumer = Consumer({"bootstrap.servers": DR, "group.id": GROUP, "auto.offset.reset": "earliest",
                     "enable.auto.commit": False})
if args.mode == "translate":
    offsets = translate_offsets(GROUP)
    banner(f"translated offsets from {CHECKPOINTS}")
    for (t, p), o in sorted(offsets.items()):
        print(f"  {t}[{p}] -> start at {o}")
    # every partition of the remote topic: checkpointed ones resume, the rest start from the beginning
    parts = consumer.list_topics(timeout=10).topics[REMOTE_TOPIC].partitions
    consumer.assign([TopicPartition(REMOTE_TOPIC, p, offsets.get((REMOTE_TOPIC, p), OFFSET_BEGINNING))
                     for p in sorted(parts)])
else:
    banner(f"subscribe to {REMOTE_TOPIC} with the SAME group id; using offsets MM2 committed on DR")
    consumer.subscribe([REMOTE_TOPIC])

first, n, idle = {}, 0, 0
while idle < 4:
    m = consumer.poll(1.0)
    if m is None:
        idle += 1 if consumer.assignment() else 0
        continue
    if m.error():
        continue
    first.setdefault(m.partition(), m.offset())
    n += 1
consumer.close()
log(f"resumed on DR: read {n} remaining records; first offset per partition: {dict(sorted(first.items()))}")
print(f"""
Compare with what 02 committed on the primary. You resume at (or slightly BEFORE) that point:
checkpoints are only as fresh as the last offset-sync, so failover is AT-LEAST-ONCE (a few duplicates).
The consumer reads '{REMOTE_TOPIC}' — apps must know the remote topic name (or subscribe with a regex
like '.*{TOPIC}' so the same code works on both clusters). Failback = reverse the flow (dr->primary).
""")
