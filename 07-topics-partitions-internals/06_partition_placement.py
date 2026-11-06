#!/usr/bin/env python3
"""DG ch.5 "Partition allocation": how replicas are spread across brokers (and racks).

    python 07-topics-partitions-internals/06_partition_placement.py
"""
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.kafka_helpers import admin, banner, recreate_topic  # noqa: E402

RACK = {1: "rack-a", 2: "rack-b", 3: "rack-c"}   # from infra/docker-compose.yml KAFKA_BROKER_RACK
a = admin()
for name, parts, rf in [("kinaction_place_rf3", 12, 3), ("kinaction_place_rf2", 12, 2)]:
    recreate_topic(name, parts, rf, client=a)
    t = a.list_topics(name).topics[name]
    leaders, replicas = Counter(), Counter()
    banner(f"{name}: {parts} partitions, RF={rf}")
    for pid in sorted(t.partitions):
        p = t.partitions[pid]
        leaders[p.leader] += 1
        replicas.update(p.replicas)
        racks = {RACK[b] for b in p.replicas}
        print(f"  p{pid:<2d} replicas={p.replicas}  racks={sorted(racks)}")
    print(f"  leaders per broker : {dict(sorted(leaders.items()))}")
    print(f"  replicas per broker: {dict(sorted(replicas.items()))}")
print("""
* Round-robin with a random start broker; follower placement is shifted so leaders and followers spread evenly.
* With broker.rack set, each partition's replicas land on DIFFERENT racks (survive a whole rack/AZ outage).
* First replica = preferred leader. Even leader spread = even load (leaders serve all produce/fetch traffic).
""")
