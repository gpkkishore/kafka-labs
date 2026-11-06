#!/usr/bin/env python3
"""MySQL --Debezium--> Kafka --this consumer--> Redis : keep a cache in sync without dual writes.

Why not "write to DB, then write to cache" in the app? Two writes can't be atomic: crash in between, races between
concurrent writers, and every new consumer means changing the app. With CDC the DB commit IS the event.

Rules implemented here:
  op c/r/u  -> HSET customer:<id> ...  (upsert = idempotent, safe to replay: at-least-once is fine)
  op d      -> DEL customer:<id>
  tombstone -> ignore (already handled by the op=d event)
  offsets are committed only after Redis was updated  -> no lost updates on crash

    python 16-cdc-debezium-mysql/04_redis_cache_sync.py --verify       # sync, then compare Redis with MySQL
    python 16-cdc-debezium-mysql/04_redis_cache_sync.py --follow       # keep syncing (run 03 meanwhile)
"""
import argparse

import redis
from cdc_common import mysql, unwrap
from confluent_kafka import Consumer

from common.kafka_helpers import BOOTSTRAP, banner, log

ap = argparse.ArgumentParser()
ap.add_argument("--follow", action="store_true")
ap.add_argument("--verify", action="store_true")
args = ap.parse_args()

r = redis.Redis(host="localhost", port=6379, decode_responses=True)
TOPICS = {"cdc.inventory.customers": "customer", "cdc.inventory.products": "product"}
c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "redis-cache-sync", "auto.offset.reset": "earliest",
              "enable.auto.commit": False})
c.subscribe(list(TOPICS))
banner("Syncing MySQL -> Redis (group redis-cache-sync)")
idle, n = 0, 0
try:
    while args.follow or idle < 5:
        msgs = c.consume(num_messages=200, timeout=1.0)
        if not msgs:
            idle += 1 if c.assignment() else 0
            continue
        idle = 0
        pipe = r.pipeline()
        for m in msgs:
            if m.error():
                continue
            ev = unwrap(m.value())
            if ev is None:
                continue                                   # tombstone
            prefix = TOPICS[m.topic()]
            row = ev["after"] or ev["before"]
            key = f"{prefix}:{row['id']}"
            if ev["op"] == "d":
                pipe.delete(key)
                log(f"DEL  {key}")
            else:
                pipe.hset(key, mapping={k: "" if v is None else v for k, v in ev["after"].items()})
                if ev["op"] != "r":
                    log(f"HSET {key} ({'update' if ev['op'] == 'u' else 'insert'})")
            n += 1
        pipe.execute()
        c.commit(asynchronous=False)                       # commit AFTER the cache write
except KeyboardInterrupt:
    pass
finally:
    c.close()
log(f"applied {n} change events")

if args.verify:
    banner("Verify: Redis vs MySQL")
    db = mysql()
    with db.cursor() as cur:
        for table, prefix in (("customers", "customer"), ("products", "product")):
            cur.execute(f"SELECT * FROM {table}")
            rows = {str(x["id"]): x for x in cur.fetchall()}
            cached = {k.split(":")[1] for k in r.scan_iter(f"{prefix}:*")}
            field = "email" if table == "customers" else "price"
            norm = (lambda v: v) if field == "email" else (lambda v: None if v is None else float(v))
            mismatch = [i for i, row in rows.items() if norm(r.hget(f"{prefix}:{i}", field)) != norm(row[field])]
            print(f"  {table:9s} mysql={len(rows):3d} redis={len(cached):3d} missing={sorted(set(rows) - cached)} "
                  f"stale/extra={sorted(cached - set(rows))} value-mismatch={mismatch}")
    print("\n  Try: redis-cli -p 6379 HGETALL customer:1   (or docker exec redis redis-cli HGETALL customer:1)")
