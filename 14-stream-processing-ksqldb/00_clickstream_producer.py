#!/usr/bin/env python3
"""Input data for every example in this folder (and for ksqldb/queries.sql).

  sp_users      compacted "table": user -> {country, tier}; re-sent with changes now and then
  sp_pageviews  stream of page views; key = user_id; value.ts = EVENT time (when it happened)
                ~10% of events arrive LATE (event time 5-40s in the past) — like mobile clients reconnecting

    python 14-stream-processing-ksqldb/00_clickstream_producer.py --seconds 120 --rate 20
    python 14-stream-processing-ksqldb/00_clickstream_producer.py --reset ...   # recreate topics first
"""
import argparse
import random
import time

from confluent_kafka import Producer

from sp_common import BOOTSTRAP, PAGES, PAGEVIEWS, USER_IDS, USERS, Runner, banner, dumps, ensure_topic, log, recreate_topic

ap = argparse.ArgumentParser()
ap.add_argument("--seconds", type=float, default=120)
ap.add_argument("--rate", type=float, default=20, help="events per second")
ap.add_argument("--late-pct", type=float, default=10)
ap.add_argument("--reset", action="store_true")
args = ap.parse_args()

make = recreate_topic if args.reset else ensure_topic
make(PAGEVIEWS, 3, 3)
make(USERS, 3, 3, {"cleanup.policy": "compact"})
# murmur2_random = the Java client's partitioner. Kafka Streams / ksqlDB assume keys are partitioned this way:
# with librdkafka's default (consistent_random, CRC32) ksqlDB pull queries by key look in the WRONG partition
# and return nothing, and stream-table joins would need a repartition. See 03-producers/02_keys_and_partitioning.py
p = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 20, "partitioner": "murmur2_random"})

countries, tiers = ["IN", "US", "DE", "BR"], ["free", "gold"]
for u in USER_IDS:
    p.produce(USERS, key=u, value=dumps({"country": random.choice(countries), "tier": random.choice(tiers)}))
p.flush()
log(f"wrote {len(USER_IDS)} users to {USERS}")

banner(f"producing ~{args.rate}/s page views to {PAGEVIEWS} ({args.late_pct}% late) — Ctrl+C to stop")
r = Runner(args.seconds)
n = late = 0
while r.running():
    u = random.choice(USER_IDS)
    ts = int(time.time() * 1000)
    if random.random() * 100 < args.late_pct:
        ts -= random.randint(5_000, 40_000)
        late += 1
    page = random.choice(PAGES)
    action = "purchase" if page == "/checkout" and random.random() < 0.5 else "view"
    p.produce(PAGEVIEWS, key=u, value=dumps({"user_id": u, "page": page, "action": action, "ts": ts}), timestamp=ts)
    n += 1
    if random.random() < 0.01:   # a user upgrades / moves -> table update
        new = {"country": random.choice(countries), "tier": random.choice(tiers)}
        p.produce(USERS, key=u, value=dumps(new))
        log(f"user update {u} -> {new}")
    if n % 200 == 0:
        log(f"{n} events ({late} late)")
    p.poll(0)
    time.sleep(1 / args.rate)
p.flush()
log(f"done: {n} events, {late} late")
