#!/usr/bin/env python3
"""KiA 5.3.1 / DG ch.4 — where committed offsets live: the compacted topic __consumer_offsets.

  coordinator partition = abs(java String.hashCode(group.id)) % offsets.topic.num.partitions (50)
  the LEADER of that partition is the group's coordinator broker.

    python 07-topics-partitions-internals/04_consumer_offsets_internals.py
"""
import subprocess
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer, Producer  # noqa: E402

from common.kafka_helpers import BOOTSTRAP, admin, banner, ensure_topic  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]


def java_hashcode(s: str) -> int:
    h = 0
    for ch in s:
        h = (31 * h + ord(ch)) & 0xFFFFFFFF
    return h - (1 << 32) if h & 0x80000000 else h


TOPIC = "kinaction_internals"
ensure_topic(TOPIC, 3, 3)
group = f"offsets-demo-{uuid.uuid4().hex[:6]}"
p = Producer({"bootstrap.servers": BOOTSTRAP})
for i in range(6):
    p.produce(TOPIC, value=f"v{i}")
p.flush()

part = abs(java_hashcode(group)) % 50
a = admin()
leader = a.list_topics("__consumer_offsets").topics["__consumer_offsets"].partitions[part].leader
desc = None
c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": group, "auto.offset.reset": "earliest", "enable.auto.commit": False})
c.subscribe([TOPIC])
n = 0
while n < 6:
    m = c.poll(2)
    if m and not m.error():
        n += 1
c.commit(asynchronous=False)
desc = a.describe_consumer_groups([group])[group].result()
c.close()

banner(f"group '{group}'")
print(f"  javaHashCode={java_hashcode(group)}  -> __consumer_offsets partition {part}")
print(f"  leader of __consumer_offsets-{part} = broker {leader};  coordinator reported by the broker = {desc.coordinator.id}")

banner(f"raw records in __consumer_offsets-{part} for this group (decoded by the broker's formatters)")
# Kafka 3.x class names (Kafka 4.x moved them to org.apache.kafka.tools.consumer.*MessageFormatter)
for fmt in ["kafka.coordinator.group.GroupMetadataManager$OffsetsMessageFormatter",
            "kafka.coordinator.group.GroupMetadataManager$GroupMetadataMessageFormatter"]:
    out = subprocess.run([str(ROOT / "bin/kafka-cli"), "kafka-console-consumer", "--bootstrap-server", "kafka1:29092",
                          "--topic", "__consumer_offsets", "--partition", str(part), "--offset", "earliest",
                          "--formatter", fmt, "--timeout-ms", "6000"], capture_output=True, text=True)
    lines = [ln for ln in out.stdout.splitlines() if group in ln]
    if not lines and "ClassNotFound" in out.stderr:
        print(f"  ({fmt.split('.')[-1]} not available in this Kafka version)")
    for ln in lines[-6:]:
        print("  " + ln[:220])
print("""
* Offset commit record:   key=[group, topic, partition] -> value={offset, leaderEpoch, metadata, commitTimestamp}
* Group metadata record:  key=[group] -> value={protocolType, generation, leader, members & assignments}
* The topic is COMPACTED: only the latest commit per (group, topic, partition) is kept.
* Deleting a group writes TOMBSTONES for its keys.
""")
