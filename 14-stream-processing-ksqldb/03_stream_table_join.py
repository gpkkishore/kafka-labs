#!/usr/bin/env python3
"""Stream-table join: enrich every page view with the user's CURRENT country/tier.

  sp_users (compacted)  -> materialized as a local table (dict). We read ALL partitions => like a
                           GlobalKTable (every instance has the full table, no co-partitioning needed).
                           A KTable would only hold the partitions matching this instance's stream partitions.
  sp_pageviews (stream) -> for each event: lookup table[user_id] -> emit enriched record

Table updates keep flowing in, so later events see the NEW value (temporal semantics: the join uses the
table as of the moment the stream record is processed).

    python 14-stream-processing-ksqldb/03_stream_table_join.py --seconds 60
"""
import argparse
import json
import uuid

from confluent_kafka import Consumer, Producer, TopicPartition

from sp_common import BOOTSTRAP, PAGEVIEWS, USERS, Runner, admin, banner, dumps, ensure_topic, log

OUT = "sp_pageviews_enriched"
ap = argparse.ArgumentParser()
ap.add_argument("--seconds", type=float, default=60)
args = ap.parse_args()
ensure_topic(OUT, 3, 3)

# 1) Table side: assign() all partitions from the beginning (no group -> every instance gets everything)
tc = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"sp-table-{uuid.uuid4().hex[:4]}", "enable.auto.commit": False})
parts = admin().list_topics(USERS).topics[USERS].partitions
tc.assign([TopicPartition(USERS, pid, 0) for pid in parts])
table = {}
ends = {pid: tc.get_watermark_offsets(TopicPartition(USERS, pid))[1] for pid in parts}
pos = {pid: 0 for pid in parts}
while any(pos[pid] < ends[pid] for pid in parts):           # bootstrap: read table up to current end
    m = tc.poll(2)
    if m is None:
        break
    if not m.error():
        table[m.key().decode()] = json.loads(m.value()) if m.value() else None
        pos[m.partition()] = m.offset() + 1
log(f"table bootstrapped: {len(table)} users {table}")

# 2) Stream side
sc = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "sp-join", "auto.offset.reset": "latest"})
sc.subscribe([PAGEVIEWS])
p = Producer({"bootstrap.servers": BOOTSTRAP})
banner(f"{PAGEVIEWS} JOIN {USERS} -> {OUT}")
r, n, misses = Runner(args.seconds), 0, 0
while r.running():
    u = tc.poll(0)                                            # keep the table up to date
    if u is not None and not u.error():
        old = table.get(u.key().decode())
        table[u.key().decode()] = json.loads(u.value()) if u.value() else None
        log(f"TABLE UPDATE {u.key().decode()}: {old} -> {table[u.key().decode()]}")
    m = sc.poll(0.2)
    if m is None or m.error():
        continue
    e = json.loads(m.value())
    user = table.get(e["user_id"])
    if user is None:                                          # inner join drops, left join keeps with nulls
        misses += 1
        continue
    enriched = {**e, **user}
    p.produce(OUT, key=m.key(), value=dumps(enriched))
    n += 1
    if n % 25 == 0:
        log(f"{n} enriched, e.g. {enriched}")
    p.poll(0)
p.flush()
log(f"enriched {n}, unmatched {misses}")
