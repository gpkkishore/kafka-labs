#!/usr/bin/env python3
"""Fault-tolerant local state = local store + CHANGELOG topic (how Kafka Streams state stores work).

  * state: running page-view count & last page per user (a dict = our "RocksDB")
  * every state change is also written to a COMPACTED changelog topic (key = user)
  * on startup the store is RESTORED by replaying the changelog before processing resumes
  * input offsets + changelog writes are committed in ONE transaction => restored state and input
    position always match (exactly-once, like processing.guarantee=exactly_once_v2)

    python 14-stream-processing-ksqldb/04_state_store_changelog.py --seconds 30      # build state
    # Ctrl+C or let it finish, then run again: "restored N keys from changelog"
"""
import argparse
import json

from confluent_kafka import Consumer, Producer, TopicPartition

from sp_common import BOOTSTRAP, PAGEVIEWS, Runner, admin, banner, dumps, ensure_topic, log

CHANGELOG, GROUP = "sp-user-activity-store-changelog", "sp-user-activity"
ap = argparse.ArgumentParser()
ap.add_argument("--seconds", type=float, default=30)
args = ap.parse_args()
ensure_topic(CHANGELOG, 3, 3, {"cleanup.policy": "compact", "segment.ms": 60000})

# ---- 1) restore ----
store = {}
rc = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "restore", "enable.auto.commit": False,
               "isolation.level": "read_committed"})
parts = admin().list_topics(CHANGELOG).topics[CHANGELOG].partitions
rc.assign([TopicPartition(CHANGELOG, pid, 0) for pid in parts])
restored = 0
while (m := rc.poll(2)) is not None:
    if not m.error():
        store[m.key().decode()] = json.loads(m.value())
        restored += 1
rc.close()
banner(f"restored {len(store)} keys from {restored} changelog records")
for k in sorted(store):
    print(f"  {k}: {store[k]}")

# ---- 2) process with EOS ----
c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": GROUP, "auto.offset.reset": "earliest",
              "enable.auto.commit": False, "isolation.level": "read_committed"})
p = Producer({"bootstrap.servers": BOOTSTRAP, "transactional.id": "sp-user-activity-0"})
p.init_transactions()
c.subscribe([PAGEVIEWS])
r, total = Runner(args.seconds), 0
while r.running():
    batch = [m for m in c.consume(100, timeout=1.0) if not m.error()]
    if not batch:
        continue
    p.begin_transaction()
    for m in batch:
        e = json.loads(m.value())
        s = store.setdefault(e["user_id"], {"views": 0, "purchases": 0, "last_page": None})
        s["views"] += 1
        s["purchases"] += e["action"] == "purchase"
        s["last_page"] = e["page"]
        p.produce(CHANGELOG, key=e["user_id"], value=dumps(s))     # changelog = durable copy of the store
    p.send_offsets_to_transaction(c.position(c.assignment()), c.consumer_group_metadata())
    p.commit_transaction()
    total += len(batch)
    log(f"processed {total}; store: " + ", ".join(f"{k}={v['views']}" for k, v in sorted(store.items())))
c.close()
log("stopped. Run me again: the store comes back from the changelog, then continues where the offsets say.")
