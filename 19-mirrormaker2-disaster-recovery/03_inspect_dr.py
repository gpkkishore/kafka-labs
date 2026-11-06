#!/usr/bin/env python3
"""Step 3 — what does the DR cluster look like?
  * which topics MM2 created (remote topic naming)
  * per-partition record counts primary vs dr (replication lag)
  * the decoded checkpoints (offset translation) for our group

    python 19-mirrormaker2-disaster-recovery/03_inspect_dr.py
"""
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition  # noqa: E402
from confluent_kafka.admin import AdminClient  # noqa: E402

from common.kafka_helpers import banner  # noqa: E402
from mm2_common import CHECKPOINTS, DR, GROUP, PRIMARY, REMOTE_TOPIC, TOPIC, decode_checkpoint  # noqa: E402

dr_admin = AdminClient({"bootstrap.servers": DR})
banner("Topics on the DR cluster")
for t in sorted(dr_admin.list_topics(timeout=10).topics):
    note = {REMOTE_TOPIC: "<- mirrored data (prefix = source cluster alias)",
            CHECKPOINTS: "<- translated consumer offsets",
            "heartbeats": "<- MM2 heartbeats", "mm2-configs.primary.internal": "<- MM2's own Connect state",
            "mm2-offsets.primary.internal": "<- MM2's own Connect state", "mm2-status.primary.internal": "<- MM2's own Connect state",
            "primary.heartbeats": "<- heartbeats replicated from primary"}.get(t, "")
    print(f"  {t:40s} {note}")


def watermarks(bootstrap, topic):
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": "wm", "allow.auto.create.topics": False})
    md = c.list_topics(timeout=10).topics.get(topic)      # list ALL topics: asking for one could auto-create it
    if md is None:
        c.close()
        return {}
    out = {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10) for p in sorted(md.partitions)}
    c.close()
    return out


banner(f"Replication: {TOPIC} (primary) vs {REMOTE_TOPIC} (dr)")
src, dst = watermarks(PRIMARY, TOPIC), watermarks(DR, REMOTE_TOPIC)
for p in src:
    s, d = src[p][1], dst.get(p, (0, 0))[1]
    print(f"  partition {p}: primary end offset={s:<5d} dr end offset={d:<5d} lag={s - d}")
print("  (offsets happen to match here because the DR topic was created empty and nothing was deleted;\n"
      "   in general offsets on the two clusters DIFFER — that's why checkpoints exist)")

banner(f"Checkpoints for group {GROUP}  ({CHECKPOINTS})")
if CHECKPOINTS not in dr_admin.list_topics(timeout=10).topics:
    sys.exit("  checkpoints topic not created yet — is mm2 running? (docker logs mm2) Retry in ~20s.")
c = Consumer({"bootstrap.servers": DR, "group.id": f"cp-{uuid.uuid4()}", "auto.offset.reset": "earliest",
              "enable.auto.commit": False})
c.subscribe([CHECKPOINTS])
latest, idle = {}, 0
while idle < 4:
    m = c.poll(1.0)
    if m is None:
        idle += 1 if c.assignment() else 0
        continue
    if m.error():
        continue
    cp = decode_checkpoint(m.key(), m.value())
    latest[(cp["group"], cp["topic"], cp["partition"])] = cp
c.close()
if not latest:
    print("  none yet — wait ~15s after 02_consume_on_primary.py and retry")
for (g, t, p), cp in sorted(latest.items()):
    print(f"  group={g} {t}[{p}] upstream(primary) offset={cp['upstream_offset']:<5d} -> "
          f"downstream(dr) offset={cp['downstream_offset']}")

banner(f"Offsets MM2 committed for {GROUP} on DR (sync.group.offsets.enabled=true)")
res = dr_admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(GROUP)])
for tp in sorted(res[GROUP].result().topic_partitions, key=lambda x: (x.topic, x.partition)):
    print(f"  {tp.topic}[{tp.partition}] -> {tp.offset}")
