#!/usr/bin/env python3
"""Stateless processing: filter + map (KStream.filter().mapValues().to() in Kafka Streams).

  sp_pageviews --filter(action == purchase)--> --map(add fields)--> sp_purchases (key re-keyed to page)

Stateless = each record is handled on its own -> trivially scalable: run N copies with the same group.id.

    python 14-stream-processing-ksqldb/01_stateless_filter_map.py --seconds 60
"""
import argparse
import json

from confluent_kafka import Consumer, Producer

from sp_common import BOOTSTRAP, PAGEVIEWS, Runner, banner, dumps, ensure_topic, fmt_ts, log

OUT = "sp_purchases"
ap = argparse.ArgumentParser()
ap.add_argument("--seconds", type=float, default=60)
args = ap.parse_args()
ensure_topic(OUT, 3, 3)

c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "sp-filter-map", "auto.offset.reset": "latest"})
p = Producer({"bootstrap.servers": BOOTSTRAP})
c.subscribe([PAGEVIEWS])
banner(f"{PAGEVIEWS} -> filter(purchase) -> map -> {OUT}")
r, seen, kept = Runner(args.seconds), 0, 0
while r.running():
    m = c.poll(0.5)
    if m is None or m.error():
        continue
    seen += 1
    e = json.loads(m.value())
    if e["action"] != "purchase":                      # filter
        continue
    out = {"user_id": e["user_id"], "page": e["page"], "event_time": fmt_ts(e["ts"]),
           "amount": round(10 + hash(e["user_id"]) % 90, 2)}   # map / enrich
    p.produce(OUT, key=e["page"], value=dumps(out))     # re-key: selectKey() => data moves to a new partition
    kept += 1
    log(f"purchase: {out}")
    p.poll(0)
p.flush()
c.close()
log(f"seen {seen}, emitted {kept}")
